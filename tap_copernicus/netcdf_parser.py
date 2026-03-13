"""Vectorized NetCDF parser for ERA5 data files.

Converts NetCDF files downloaded from CDS into flat EAV (Entity-Attribute-Value)
records suitable for Singer output. Uses xarray + pandas for vectorized operations
instead of nested Python loops.

CDS variable naming:
  - API requests use long names: "2m_temperature"
  - Downloaded NetCDF files use short names: "t2m"
  - The mapping is maintained in helpers.CDS_TO_NETCDF_VARIABLE_MAP
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

from tap_copernicus.helpers import CDS_TO_NETCDF_VARIABLE_MAP, NETCDF_TO_CDS_VARIABLE_MAP

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)

# Coordinate name variants across CDS versions and file formats
_LATITUDE_ALIASES = ("latitude", "lat", "y")
_LONGITUDE_ALIASES = ("longitude", "lon", "x")
_TIME_ALIASES = ("valid_time", "time", "t")
_LEVEL_ALIASES = ("level", "pressure_level", "isobaricInhPa", "plev")


class NetCDFParser:
    """Parse ERA5 NetCDF files into flat tabular records using vectorized operations."""

    def parse_to_records(
        self,
        file_path: str,
        requested_variables: list[str],
        dataset_name: str,
        product_type: str,
    ) -> Iterator[dict]:
        """Parse NetCDF file and yield flat EAV records.

        Uses vectorized xarray-to-pandas conversion. Each data variable in the
        NetCDF file is converted to a DataFrame, NaN values are dropped, and
        records are yielded as dicts.

        Args:
            file_path: Path to the downloaded NetCDF file.
            requested_variables: CDS variable names that were requested.
            dataset_name: CDS dataset identifier for the output record.
            product_type: CDS product type for the output record.

        Yields:
            Flat record dicts with time, lat, lon, variable, value, units, etc.
        """
        expected_netcdf_names = {CDS_TO_NETCDF_VARIABLE_MAP.get(v, v) for v in requested_variables}

        ds = xr.open_dataset(file_path)
        try:
            has_expver = "expver" in ds.dims or "expver" in ds.coords

            for nc_var in ds.data_vars:
                nc_var_name = str(nc_var)
                if nc_var_name not in expected_netcdf_names:
                    continue

                cds_var_name = NETCDF_TO_CDS_VARIABLE_MAP.get(nc_var_name, nc_var_name)
                da = ds[nc_var_name]
                units = da.attrs.get("units", "unknown")

                # Vectorized: DataArray -> DataFrame -> drop NaN -> yield records
                df = da.to_dataframe().reset_index()
                df = df.dropna(subset=[nc_var_name])

                if df.empty:
                    logger.debug(
                        "No valid data for variable '%s' in %s",
                        nc_var_name,
                        file_path,
                    )
                    continue

                df = _standardize_coordinate_columns(df)

                # Build output columns
                df["variable"] = cds_var_name
                df["value"] = df[nc_var_name].round(6).astype(float)
                df["units"] = units
                df["dataset"] = dataset_name
                df["product_type"] = product_type

                # Handle expver (ERA5 final=1, ERA5T preliminary=5)
                if has_expver and "expver" in df.columns:
                    df["expver"] = df["expver"].astype(str).str.strip()
                else:
                    df["expver"] = "1"

                # Format coordinates
                df["time"] = pd.to_datetime(df["time"]).dt.strftime(
                    "%Y-%m-%dT%H:%M:%S+00:00",
                )
                df["latitude"] = df["latitude"].round(4).astype(float)
                df["longitude"] = df["longitude"].round(4).astype(float)

                # Select and order output columns
                output_cols = [
                    "time",
                    "latitude",
                    "longitude",
                    "variable",
                    "value",
                    "units",
                    "dataset",
                    "product_type",
                    "expver",
                ]
                if "pressure_level" in df.columns:
                    df["pressure_level"] = df["pressure_level"].astype(int)
                    output_cols.insert(4, "pressure_level")

                yield from df[output_cols].to_dict("records")

        finally:
            ds.close()


def _standardize_coordinate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename coordinate columns to standard names.

    CDS files use varying coordinate names depending on the dataset,
    format, and CDS version. This normalizes them to:
    latitude, longitude, time, pressure_level.
    """
    rename_map: dict[str, str] = {}

    for alias in _LATITUDE_ALIASES:
        if alias in df.columns and alias != "latitude":
            rename_map[alias] = "latitude"
            break

    for alias in _LONGITUDE_ALIASES:
        if alias in df.columns and alias != "longitude":
            rename_map[alias] = "longitude"
            break

    for alias in _TIME_ALIASES:
        if alias in df.columns and alias != "time":
            rename_map[alias] = "time"
            break

    for alias in _LEVEL_ALIASES:
        if alias in df.columns and alias != "pressure_level":
            rename_map[alias] = "pressure_level"
            break

    if rename_map:
        df = df.rename(columns=rename_map)

    return df
