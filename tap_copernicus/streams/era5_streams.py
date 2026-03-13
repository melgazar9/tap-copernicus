"""ERA5 stream definitions for tap-copernicus.

Each stream class corresponds to one CDS dataset and configures:
  - dataset_name: CDS dataset identifier
  - earliest_available_date: Earliest date data is available
  - default_variables: Default variable set if not overridden in config
  - available_product_types: All product types the dataset supports
  - default_product_types: Default product types if not overridden
  - has_pressure_levels: Whether the dataset has a pressure level dimension
  - is_monthly: Whether this is a monthly means dataset

All extraction logic is inherited from CopernicusStream (client.py).

Stream naming convention:
  era5_{temporal_resolution}_{spatial_domain}
  e.g., era5_hourly_single_levels, era5_monthly_land
"""

from __future__ import annotations

from typing import ClassVar

from tap_copernicus.client import (
    PRESSURE_LEVEL_SCHEMA,
    SURFACE_LEVEL_SCHEMA,
    CopernicusStream,
)
from tap_copernicus.helpers import (
    DEFAULT_LAND_VARIABLES,
    DEFAULT_PRESSURE_LEVEL_VARIABLES,
    DEFAULT_SINGLE_LEVEL_VARIABLES,
)

# ---------------------------------------------------------------------------
# Hourly streams
# ---------------------------------------------------------------------------


class ERA5HourlySingleLevelsStream(CopernicusStream):
    """ERA5 hourly data on single (surface) levels from 1940 to present.

    Dataset: reanalysis-era5-single-levels
    Resolution: 0.25 x 0.25 degrees (native), configurable via grid_resolution.
    Temporal: Hourly, configurable via hours config.
    Coverage: Global.
    """

    name = "era5_hourly_single_levels"
    dataset_name = "reanalysis-era5-single-levels"
    earliest_available_date = "1940-01-01"
    default_variables = DEFAULT_SINGLE_LEVEL_VARIABLES
    available_product_types: ClassVar[list[str]] = [
        "reanalysis",
        "ensemble_mean",
        "ensemble_spread",
    ]
    default_product_types: ClassVar[list[str]] = ["reanalysis"]
    has_pressure_levels = False
    is_monthly = False

    schema = SURFACE_LEVEL_SCHEMA
    primary_keys = ("time", "latitude", "longitude", "variable", "product_type", "expver")


class ERA5HourlyPressureLevelsStream(CopernicusStream):
    """ERA5 hourly data on pressure levels from 1940 to present.

    Dataset: reanalysis-era5-pressure-levels
    Resolution: 0.25 x 0.25 degrees (native), configurable via grid_resolution.
    Temporal: Hourly, configurable via hours config.
    Levels: 37 pressure levels from 1 hPa to 1000 hPa.
    Coverage: Global.
    """

    name = "era5_hourly_pressure_levels"
    dataset_name = "reanalysis-era5-pressure-levels"
    earliest_available_date = "1940-01-01"
    default_variables = DEFAULT_PRESSURE_LEVEL_VARIABLES
    available_product_types: ClassVar[list[str]] = [
        "reanalysis",
        "ensemble_mean",
        "ensemble_spread",
    ]
    default_product_types: ClassVar[list[str]] = ["reanalysis"]
    has_pressure_levels = True
    is_monthly = False

    schema = PRESSURE_LEVEL_SCHEMA
    primary_keys = (
        "time",
        "latitude",
        "longitude",
        "variable",
        "pressure_level",
        "product_type",
        "expver",
    )


class ERA5HourlyLandStream(CopernicusStream):
    """ERA5-Land hourly data from 1950 to present.

    Dataset: reanalysis-era5-land
    Resolution: 0.1 x 0.1 degrees (native), configurable via grid_resolution.
    Temporal: Hourly, configurable via hours config.
    Coverage: Land only. Ocean grid points return NaN (dropped during parsing).

    Note: ERA5-Land has higher spatial resolution than ERA5 but is land-only.
    It does NOT have ensemble product types (reanalysis only).
    """

    name = "era5_hourly_land"
    dataset_name = "reanalysis-era5-land"
    earliest_available_date = "1950-01-01"
    default_variables = DEFAULT_LAND_VARIABLES
    available_product_types: ClassVar[list[str]] = ["reanalysis"]
    default_product_types: ClassVar[list[str]] = ["reanalysis"]
    has_pressure_levels = False
    is_monthly = False

    schema = SURFACE_LEVEL_SCHEMA
    primary_keys = ("time", "latitude", "longitude", "variable", "product_type", "expver")


# ---------------------------------------------------------------------------
# Monthly means streams
# ---------------------------------------------------------------------------


class ERA5MonthlySingleLevelsStream(CopernicusStream):
    """ERA5 monthly averaged data on single levels from 1940 to present.

    Dataset: reanalysis-era5-single-levels-monthly-means
    Resolution: 0.25 x 0.25 degrees (native), configurable.
    Temporal: Monthly averages.
    """

    name = "era5_monthly_single_levels"
    dataset_name = "reanalysis-era5-single-levels-monthly-means"
    earliest_available_date = "1940-01-01"
    default_variables = DEFAULT_SINGLE_LEVEL_VARIABLES
    available_product_types: ClassVar[list[str]] = [
        "monthly_averaged_reanalysis",
        "monthly_averaged_reanalysis_by_hour_of_day",
        "monthly_averaged_ensemble_members_by_hour_of_day",
    ]
    default_product_types: ClassVar[list[str]] = ["monthly_averaged_reanalysis"]
    has_pressure_levels = False
    is_monthly = True

    schema = SURFACE_LEVEL_SCHEMA
    primary_keys = ("time", "latitude", "longitude", "variable", "product_type", "expver")


class ERA5MonthlyPressureLevelsStream(CopernicusStream):
    """ERA5 monthly averaged data on pressure levels from 1940 to present.

    Dataset: reanalysis-era5-pressure-levels-monthly-means
    Resolution: 0.25 x 0.25 degrees (native), configurable.
    Temporal: Monthly averages.
    Levels: 37 pressure levels from 1 hPa to 1000 hPa.
    """

    name = "era5_monthly_pressure_levels"
    dataset_name = "reanalysis-era5-pressure-levels-monthly-means"
    earliest_available_date = "1940-01-01"
    default_variables = DEFAULT_PRESSURE_LEVEL_VARIABLES
    available_product_types: ClassVar[list[str]] = [
        "monthly_averaged_reanalysis",
        "monthly_averaged_reanalysis_by_hour_of_day",
        "monthly_averaged_ensemble_members_by_hour_of_day",
    ]
    default_product_types: ClassVar[list[str]] = ["monthly_averaged_reanalysis"]
    has_pressure_levels = True
    is_monthly = True

    schema = PRESSURE_LEVEL_SCHEMA
    primary_keys = (
        "time",
        "latitude",
        "longitude",
        "variable",
        "pressure_level",
        "product_type",
        "expver",
    )


class ERA5MonthlyLandStream(CopernicusStream):
    """ERA5-Land monthly averaged data from 1950 to present.

    Dataset: reanalysis-era5-land-monthly-means
    Resolution: 0.1 x 0.1 degrees (native), configurable.
    Temporal: Monthly averages.
    Coverage: Land only.
    """

    name = "era5_monthly_land"
    dataset_name = "reanalysis-era5-land-monthly-means"
    earliest_available_date = "1950-01-01"
    default_variables = DEFAULT_LAND_VARIABLES
    available_product_types: ClassVar[list[str]] = ["monthly_averaged_reanalysis"]
    default_product_types: ClassVar[list[str]] = ["monthly_averaged_reanalysis"]
    has_pressure_levels = False
    is_monthly = True

    schema = SURFACE_LEVEL_SCHEMA
    primary_keys = ("time", "latitude", "longitude", "variable", "product_type", "expver")
