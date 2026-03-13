"""CDS API client wrapper and base stream class for Copernicus data extraction.

The Copernicus Climate Data Store (CDS) uses an async batch API:
  1. Submit a request to the CDS queue
  2. Poll until the request is completed (or failed/timed out)
  3. Download the result file (NetCDF or GRIB)
  4. Parse the file locally and emit records

This module provides:
  - CDSClient: Wrapper around cdsapi with timeout, logging, and retry.
  - CopernicusStream: Base Singer SDK Stream class for CDS data extraction.
"""

from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import cdsapi
from singer_sdk import Stream
from singer_sdk import typing as th

from tap_copernicus.helpers import (
    ALL_PRESSURE_LEVELS,
    DEFAULT_BOUNDING_BOX,
    compute_default_end_date,
    days_in_month,
    expand_hours,
    generate_monthly_partitions,
    generate_yearly_partitions,
    parse_date_string,
)
from tap_copernicus.netcdf_parser import NetCDFParser

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import date

    from singer_sdk.helpers.types import Context

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared schemas for surface-level and pressure-level streams
# ---------------------------------------------------------------------------

SURFACE_LEVEL_SCHEMA = th.PropertiesList(
    th.Property("time", th.DateTimeType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("variable", th.StringType, required=True),
    th.Property("value", th.NumberType),
    th.Property("units", th.StringType),
    th.Property("dataset", th.StringType),
    th.Property("product_type", th.StringType),
    th.Property("expver", th.StringType),
).to_dict()

PRESSURE_LEVEL_SCHEMA = th.PropertiesList(
    th.Property("time", th.DateTimeType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("variable", th.StringType, required=True),
    th.Property("pressure_level", th.IntegerType, required=True),
    th.Property("value", th.NumberType),
    th.Property("units", th.StringType),
    th.Property("dataset", th.StringType),
    th.Property("product_type", th.StringType),
    th.Property("expver", th.StringType),
).to_dict()


# ---------------------------------------------------------------------------
# CDS Client
# ---------------------------------------------------------------------------


class CDSClient:
    """Wrapper around cdsapi with manual polling, timeout, and structured logging.

    Handles the async CDS request lifecycle: submit -> poll -> download.
    Does NOT use cdsapi's built-in blocking mode so we can enforce our own
    timeout and emit structured log messages during the wait.
    """

    def __init__(
        self,
        url: str,
        key: str,
        timeout: int = 7200,
        poll_interval: int = 30,
    ) -> None:
        """Initialize CDS client.

        Args:
            url: CDS API base URL.
            key: CDS Personal Access Token.
            timeout: Max seconds to wait for a single request.
            poll_interval: Seconds between status polls.
        """
        self._client = cdsapi.Client(
            url=url,
            key=key,
            wait_until_complete=False,
            quiet=True,
        )
        self._timeout = timeout
        self._poll_interval = poll_interval

    def retrieve(self, dataset: str, request: dict, output_path: str) -> str:
        """Submit CDS request, poll until complete, download result.

        Args:
            dataset: CDS dataset identifier.
            request: CDS API request parameters.
            output_path: Local file path for the downloaded result.

        Returns:
            The output_path after successful download.

        Raises:
            RuntimeError: If CDS reports the request as failed.
            TimeoutError: If the request exceeds the configured timeout.
        """
        logger.info(
            "Submitting CDS request: dataset=%s, variables=%s, year=%s, month=%s",
            dataset,
            request.get("variable", []),
            request.get("year", []),
            request.get("month", []),
        )

        result = self._client.retrieve(dataset, request)
        return self._poll_and_download(result, output_path, dataset)

    def _poll_and_download(self, result: object, output_path: str, dataset: str) -> str:
        """Poll CDS for completion and download the result file."""
        start_time = time.monotonic()

        while True:
            result.update()  # type: ignore[attr-defined]
            state = getattr(result, "state", None)
            if state is None:
                state = getattr(result, "reply", {}).get("state", "unknown")

            if state == "completed":
                logger.info("CDS request completed for %s. Downloading to %s", dataset, output_path)
                result.download(output_path)  # type: ignore[attr-defined]
                return output_path

            if state == "failed":
                reply = getattr(result, "reply", {})
                error_detail = reply.get("error", {})
                if isinstance(error_detail, dict):
                    error_msg = error_detail.get("message", str(error_detail))
                else:
                    error_msg = str(error_detail)
                msg = f"CDS request failed for {dataset}: {error_msg}"
                raise RuntimeError(msg)

            elapsed = time.monotonic() - start_time
            if elapsed > self._timeout:
                msg = f"CDS request timed out after {self._timeout}s for {dataset}"
                raise TimeoutError(msg)

            logger.info(
                "CDS request state=%s for %s (elapsed=%.0fs). Polling in %ds...",
                state,
                dataset,
                elapsed,
                self._poll_interval,
            )
            time.sleep(self._poll_interval)


# ---------------------------------------------------------------------------
# Base Stream
# ---------------------------------------------------------------------------


class CopernicusStream(Stream):
    """Base stream class for Copernicus CDS data extraction.

    This is NOT a REST stream. CDS uses an async batch API (submit/poll/download).
    Subclasses configure dataset-specific attributes; the extraction logic lives here.

    Subclasses MUST set:
        dataset_name, earliest_available_date, default_variables,
        available_product_types, default_product_types

    Subclasses MAY override:
        has_pressure_levels, is_monthly
    """

    # --- Subclass configuration (override in each stream) ---
    dataset_name: ClassVar[str]
    earliest_available_date: ClassVar[str]
    default_variables: ClassVar[list[str]]
    available_product_types: ClassVar[list[str]]
    default_product_types: ClassVar[list[str]]
    has_pressure_levels: ClassVar[bool] = False
    is_monthly: ClassVar[bool] = False

    # --- Singer SDK attributes ---
    replication_key = "time"
    is_sorted = False  # Records are grouped by variable, not globally sorted by time

    # Shared parser (stateless, safe to share)
    _parser: ClassVar[NetCDFParser] = NetCDFParser()

    @property
    def partitions(self) -> list[dict]:
        """Generate time-based partitions for CDS requests.

        Hourly streams: one partition per year-month.
        Monthly streams: one partition per year.
        """
        start = self._resolve_start_date()
        end = self._resolve_end_date()

        if self.is_monthly:
            return generate_yearly_partitions(start, end)
        return generate_monthly_partitions(start, end)

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Extract records for a single partition (year-month or year).

        For each partition:
          1. Build CDS request dict
          2. Submit to CDS and wait for completion
          3. Download NetCDF to temp file
          4. Parse with vectorized NetCDFParser
          5. Yield records with schema validation
          6. Clean up temp file

        Failed partitions are logged and skipped (non-strict mode) to
        allow the sync to continue with remaining partitions.
        """
        if context is None:
            return

        year = context["year"]
        month = context.get("month")  # None for monthly (yearly partition) streams
        product_types = self._resolve_product_types()
        variables = self._resolve_variables()
        cds_client = self._get_cds_client()
        temp_dir = self.config.get("temp_dir") or tempfile.gettempdir()

        for product_type in product_types:
            request = self._build_cds_request(
                year=year,
                month=month,
                variables=variables,
                product_type=product_type,
            )

            file_label = f"{self.dataset_name}_{product_type}_{year}_{month or 'yearly'}"
            download_path = str(Path(temp_dir) / f"{file_label}.download")
            extract_dir = str(Path(temp_dir) / f"{file_label}_extracted")
            cleanup_paths: list[str] = [download_path, extract_dir]

            try:
                cds_client.retrieve(self.dataset_name, request, download_path)

                # New CDS may wrap results in a ZIP archive
                data_files = _extract_data_files(download_path, extract_dir)
                if not data_files:
                    logger.warning(
                        "No data files found after download for %s (year=%s, month=%s)",
                        self.dataset_name,
                        year,
                        month,
                    )
                    continue

                record_count = 0
                for data_file in data_files:
                    cleanup_paths.append(data_file)
                    for record in self._parser.parse_to_records(
                        file_path=data_file,
                        requested_variables=variables,
                        dataset_name=self.dataset_name,
                        product_type=product_type,
                    ):
                        self._check_missing_schema_fields(record)
                        record_count += 1
                        yield record

                logger.info(
                    "Emitted %d records from %s (product_type=%s, year=%s, month=%s)",
                    record_count,
                    self.dataset_name,
                    product_type,
                    year,
                    month,
                )

            except (RuntimeError, TimeoutError, OSError):
                logger.exception(
                    "CDS request failed for %s (year=%s, month=%s, product_type=%s). "
                    "Skipping partition.",
                    self.dataset_name,
                    year,
                    month,
                    product_type,
                )
                if self.config.get("strict_mode", False):
                    raise
                continue

            finally:
                _cleanup_paths(cleanup_paths)

    # ----- CDS request building -----

    def _build_cds_request(
        self,
        year: str,
        month: str | None,
        variables: list[str],
        product_type: str,
    ) -> dict:
        """Build the CDS API request dictionary.

        Constructs the request following CDS API conventions:
          - data_format (not "format") for the new CDS API
          - area as [north, west, south, east]
          - grid as [lat_step, lon_step]
        """
        bb = self.config.get("bounding_box", DEFAULT_BOUNDING_BOX)
        grid = self.config.get("grid_resolution", [1.0, 1.0])

        request: dict = {
            "product_type": [product_type],
            "variable": variables,
            "year": [year],
            "data_format": self.config.get("output_format", "netcdf"),
            "area": [bb["north"], bb["west"], bb["south"], bb["east"]],
        }

        # Include grid only if coarser than native (avoids unnecessary regridding)
        if grid != [0.25, 0.25]:
            request["grid"] = grid

        if self.is_monthly:
            # Monthly means: all 12 months for the given year
            request["month"] = [f"{m:02d}" for m in range(1, 13)]
        else:
            # Hourly: specific month with all days and configured hours
            request["month"] = [month]
            num_days = days_in_month(int(year), int(month))  # type: ignore[arg-type]
            request["day"] = [f"{d:02d}" for d in range(1, num_days + 1)]
            request["time"] = expand_hours(
                self.config.get("hours", ["00:00", "06:00", "12:00", "18:00"]),
            )

        if self.has_pressure_levels:
            levels = self.config.get("pressure_levels", ALL_PRESSURE_LEVELS)
            request["pressure_level"] = [str(lvl) for lvl in levels]

        return request

    # ----- Configuration resolution -----

    def _resolve_start_date(self) -> date:
        """Resolve effective start date from config vs dataset minimum."""
        config_start = self.config.get("start_date")
        dataset_start = parse_date_string(self.earliest_available_date)

        if config_start:
            user_start = parse_date_string(config_start)
            return max(user_start, dataset_start)
        return dataset_start

    def _resolve_end_date(self) -> date:
        """Resolve effective end date from config or compute from ERA5 lag."""
        config_end = self.config.get("end_date")
        if config_end:
            return parse_date_string(config_end)
        return compute_default_end_date()

    def _resolve_variables(self) -> list[str]:
        """Resolve variables list from config or use stream-specific defaults."""
        config_vars = self.config.get("variables")
        if config_vars:
            return config_vars
        return self.default_variables

    def _resolve_product_types(self) -> list[str]:
        """Resolve product types from config or use stream-specific defaults.

        Supports "all" to expand to all available product types for the dataset.
        """
        configured = self.config.get("product_types")
        if not configured:
            return self.default_product_types
        if configured == ["all"]:
            return self.available_product_types
        return configured

    def _get_cds_client(self) -> CDSClient:
        """Get or create the shared CDSClient instance on the tap."""
        tap = self._tap
        if not hasattr(tap, "cds_client_instance"):
            tap.cds_client_instance = CDSClient(  # type: ignore[attr-defined]
                url=self.config.get("cds_url", "https://cds.climate.copernicus.eu/api"),
                key=self.config["cds_api_key"],
                timeout=self.config.get("request_timeout_seconds", 7200),
                poll_interval=self.config.get("poll_interval_seconds", 30),
            )
        return tap.cds_client_instance  # type: ignore[attr-defined]

    # ----- Schema validation (tap-massive pattern) -----

    def _check_missing_schema_fields(self, record: dict) -> None:
        """Log warnings for schema/record field mismatches.

        Detects:
        - Fields in schema but missing from record (debug-level, expected for optional fields)
        - Fields in record but missing from schema (warning-level, indicates schema drift)
        """
        schema_fields = set(self.schema.get("properties", {}).keys())
        record_fields = set(record.keys())

        missing_in_record = schema_fields - record_fields
        if missing_in_record:
            logger.debug(
                "Fields in schema but not in record for stream '%s': %s",
                self.name,
                missing_in_record,
            )

        extra_in_record = record_fields - schema_fields
        if extra_in_record:
            logger.warning(
                "SCHEMA DRIFT: Fields in record but not in schema for stream '%s': %s. "
                "Update the stream schema to include these fields.",
                self.name,
                extra_in_record,
            )


# ---------------------------------------------------------------------------
# Module-level helpers for file handling
# ---------------------------------------------------------------------------

_DATA_EXTENSIONS = (".nc", ".nc4", ".netcdf", ".grib", ".grib2", ".grb", ".grb2")


def _extract_data_files(download_path: str, extract_dir: str) -> list[str]:
    """Extract data files from a download, handling ZIP archives.

    The new CDS API often wraps results in a ZIP archive.
    This function detects ZIPs, extracts them, and returns paths to
    the actual data files (NetCDF or GRIB).

    If the download is already a raw data file, returns it directly.
    """
    if not Path(download_path).exists():
        return []

    # Check if it's a ZIP archive
    if zipfile.is_zipfile(download_path):
        logger.debug("Download is a ZIP archive, extracting: %s", download_path)
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(download_path, "r") as zf:
            zf.extractall(extract_dir)

        # Find all data files in the extracted directory
        extract_path = Path(extract_dir)
        data_files: list[str] = []
        for ext in _DATA_EXTENSIONS:
            data_files.extend(str(p) for p in extract_path.rglob(f"*{ext}"))

        if not data_files:
            # If no recognized extensions, try all files (CDS may use odd names)
            data_files = [str(p) for p in extract_path.iterdir() if p.is_file()]

        logger.info("Extracted %d data file(s) from ZIP", len(data_files))
        return sorted(data_files)

    # Not a ZIP - assume it's a raw data file
    return [download_path]


def _cleanup_paths(paths: list[str]) -> None:
    """Remove files and directories, ignoring errors."""
    for p_str in paths:
        p = Path(p_str)
        if not p.exists():
            continue
        try:
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
            logger.debug("Cleaned up: %s", p_str)
        except OSError:
            logger.debug("Failed to clean up: %s", p_str, exc_info=True)
