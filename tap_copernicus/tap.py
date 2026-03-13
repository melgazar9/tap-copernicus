"""TapCopernicus - Singer tap for Copernicus Climate Data Store.

Extracts ERA5 reanalysis weather data from the Copernicus CDS.
Supports ERA5 single levels, pressure levels, and ERA5-Land datasets
in both hourly and monthly-mean temporal resolutions.

Key design decisions:
  - Uses cdsapi (async batch API), NOT a REST API.
  - Monthly chunking: one CDS request per year-month (hourly) or year (monthly).
  - Vectorized NetCDF parsing via xarray + pandas.
  - EAV (Entity-Attribute-Value) output format for flexible variable selection.
  - Includes expver field to distinguish ERA5 (final) from ERA5T (preliminary).
"""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_copernicus.streams import (
    ERA5HourlyLandStream,
    ERA5HourlyPressureLevelsStream,
    ERA5HourlySingleLevelsStream,
    ERA5MonthlyLandStream,
    ERA5MonthlyPressureLevelsStream,
    ERA5MonthlySingleLevelsStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapCopernicus(Tap):
    """Singer tap for Copernicus Climate Data Store (ERA5 reanalysis)."""

    name = "tap-copernicus"

    config_jsonschema = th.PropertiesList(
        # --- Authentication ---
        th.Property(
            "cds_api_key",
            th.StringType,
            required=True,
            secret=True,
            title="CDS API Key",
            description=(
                "Copernicus Climate Data Store Personal Access Token. "
                "Obtain from https://cds.climate.copernicus.eu/ under your profile."
            ),
        ),
        th.Property(
            "cds_url",
            th.StringType,
            default="https://cds.climate.copernicus.eu/api",
            title="CDS API URL",
            description="CDS API base URL. Override only for testing or proxy setups.",
        ),
        # --- Variable and dataset selection ---
        th.Property(
            "variables",
            th.ArrayType(th.StringType),
            title="Variables",
            description=(
                "CDS variable names to extract (e.g., '2m_temperature'). "
                "If not set, each stream uses its own sensible defaults."
            ),
        ),
        th.Property(
            "product_types",
            th.ArrayType(th.StringType),
            title="Product Types",
            description=(
                "CDS product types to extract. Default: ['reanalysis']. "
                "Set to ['all'] to extract all available types for each dataset. "
                "Options vary by dataset: reanalysis, ensemble_mean, ensemble_spread, "
                "monthly_averaged_reanalysis, etc."
            ),
        ),
        th.Property(
            "pressure_levels",
            th.ArrayType(th.IntegerType),
            title="Pressure Levels",
            description=(
                "Pressure levels in hPa for pressure-level datasets. "
                "Default: all 37 levels (1 to 1000 hPa)."
            ),
        ),
        # --- Temporal range ---
        th.Property(
            "start_date",
            th.DateType,
            default="1940-01-01",
            title="Start Date",
            description=(
                "Earliest date to extract (YYYY-MM-DD). "
                "Clamped to dataset minimum (1940 for ERA5, 1950 for ERA5-Land)."
            ),
        ),
        th.Property(
            "end_date",
            th.DateType,
            title="End Date",
            description=(
                "Latest date to extract (YYYY-MM-DD). "
                "Default: 5 days before today (ERA5 data lag)."
            ),
        ),
        th.Property(
            "hours",
            th.ArrayType(th.StringType),
            default=["00:00", "06:00", "12:00", "18:00"],
            title="Hours",
            description=(
                "Hours to extract (e.g., ['00:00', '12:00']). "
                "Set to ['*'] for all 24 hours. Default: 6-hourly."
            ),
        ),
        # --- Spatial extent ---
        th.Property(
            "bounding_box",
            th.ObjectType(
                th.Property("north", th.NumberType, required=True),
                th.Property("west", th.NumberType, required=True),
                th.Property("south", th.NumberType, required=True),
                th.Property("east", th.NumberType, required=True),
            ),
            default={"north": 50.0, "west": -125.0, "south": 24.0, "east": -66.0},
            title="Bounding Box",
            description=(
                "Geographic bounding box as {north, west, south, east} in degrees. "
                "Default: CONUS (Continental United States)."
            ),
        ),
        th.Property(
            "grid_resolution",
            th.ArrayType(th.NumberType),
            default=[1.0, 1.0],
            title="Grid Resolution",
            description=(
                "Output grid resolution as [lat_step, lon_step] in degrees. "
                "Default: 1.0 degree. Native ERA5 resolution is 0.25 degrees."
            ),
        ),
        # --- CDS request tuning ---
        th.Property(
            "poll_interval_seconds",
            th.IntegerType,
            default=30,
            title="Poll Interval (seconds)",
            description="Seconds between CDS status polls while waiting for a request.",
        ),
        th.Property(
            "request_timeout_seconds",
            th.IntegerType,
            default=7200,
            title="Request Timeout (seconds)",
            description="Maximum seconds to wait for a single CDS request (default: 2 hours).",
        ),
        th.Property(
            "output_format",
            th.StringType,
            default="netcdf",
            title="Output Format",
            description="CDS download format: 'netcdf' (default) or 'grib'.",
        ),
        # --- Operational ---
        th.Property(
            "temp_dir",
            th.StringType,
            title="Temp Directory",
            description="Directory for temporary NetCDF/GRIB files. Default: system temp.",
        ),
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            title="Strict Mode",
            description=(
                "If true, raise on CDS request failures instead of skipping. "
                "Default: false (skip failed partitions and continue)."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list:
        """Return all available ERA5 streams.

        All 6 streams are registered. Users control which streams run
        via the `select` configuration in meltano.yml.
        """
        return [
            # Hourly
            ERA5HourlySingleLevelsStream(self),
            ERA5HourlyPressureLevelsStream(self),
            ERA5HourlyLandStream(self),
            # Monthly means
            ERA5MonthlySingleLevelsStream(self),
            ERA5MonthlyPressureLevelsStream(self),
            ERA5MonthlyLandStream(self),
        ]


if __name__ == "__main__":
    TapCopernicus.cli()
