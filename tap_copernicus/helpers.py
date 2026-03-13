"""Constants and utility functions for tap-copernicus.

Centralizes CDS variable name mappings, dataset metadata, date utilities,
and configuration defaults. All constants are derived from Copernicus CDS
documentation for ERA5 datasets.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone

_DECEMBER = 12

# ---------------------------------------------------------------------------
# CDS API variable name -> NetCDF internal variable name mapping
# CDS requests use human-readable long names (left).
# Downloaded NetCDF files use short names (right).
# Source: ERA5 documentation on CDS + ECMWF parameter database.
# ---------------------------------------------------------------------------
CDS_TO_NETCDF_VARIABLE_MAP: dict[str, str] = {
    # Temperature
    "2m_temperature": "t2m",
    "2m_dewpoint_temperature": "d2m",
    "skin_temperature": "skt",
    "sea_surface_temperature": "sst",
    "soil_temperature_level_1": "stl1",
    "soil_temperature_level_2": "stl2",
    "soil_temperature_level_3": "stl3",
    "soil_temperature_level_4": "stl4",
    "temperature_of_snow_layer": "tsn",
    # Wind
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "100m_u_component_of_wind": "u100",
    "100m_v_component_of_wind": "v100",
    "10m_wind_gust_since_previous_post_processing": "fg10",
    # Precipitation
    "total_precipitation": "tp",
    "large_scale_precipitation": "lsp",
    "convective_precipitation": "cp",
    "snowfall": "sf",
    "snow_depth": "sd",
    "snow_density": "rsn",
    "snow_albedo": "asn",
    # Radiation
    "surface_solar_radiation_downwards": "ssrd",
    "surface_thermal_radiation_downwards": "strd",
    "surface_net_solar_radiation": "ssr",
    "surface_net_thermal_radiation": "str",
    "top_net_solar_radiation": "tsr",
    "top_net_thermal_radiation": "ttr",
    # Pressure
    "surface_pressure": "sp",
    "mean_sea_level_pressure": "msl",
    # Cloud
    "total_cloud_cover": "tcc",
    "low_cloud_cover": "lcc",
    "medium_cloud_cover": "mcc",
    "high_cloud_cover": "hcc",
    # Boundary layer / convection
    "boundary_layer_height": "blh",
    "convective_available_potential_energy": "cape",
    # Evaporation / runoff
    "evaporation": "e",
    "runoff": "ro",
    "potential_evaporation": "pev",
    # Soil water
    "volumetric_soil_water_layer_1": "swvl1",
    "volumetric_soil_water_layer_2": "swvl2",
    "volumetric_soil_water_layer_3": "swvl3",
    "volumetric_soil_water_layer_4": "swvl4",
    # Sea ice
    "sea_ice_cover": "ci",
    # Pressure level variables (ERA5 pressure levels dataset)
    "temperature": "t",
    "u_component_of_wind": "u",
    "v_component_of_wind": "v",
    "specific_humidity": "q",
    "relative_humidity": "r",
    "geopotential": "z",
    "divergence": "d",
    "vorticity": "vo",
    "vertical_velocity": "w",
    "potential_vorticity": "pv",
    "fraction_of_cloud_cover": "cc",
    "ozone_mass_mixing_ratio": "o3",
    "specific_cloud_ice_water_content": "ciwc",
    "specific_cloud_liquid_water_content": "clwc",
    "specific_rain_water_content": "crwc",
    "specific_snow_water_content": "cswc",
    # Mean rates
    "mean_total_precipitation_rate": "mtpr",
    # ERA5-Land specific
    "lake_bottom_temperature": "lblt",
    "lake_ice_depth": "licd",
    "lake_ice_temperature": "lict",
    "lake_mix_layer_depth": "lmld",
    "lake_mix_layer_temperature": "lmlt",
    "lake_shape_factor": "lshf",
    "lake_total_layer_temperature": "ltlt",
    "leaf_area_index_high_vegetation": "lai_hv",
    "leaf_area_index_low_vegetation": "lai_lv",
    "forecast_albedo": "fal",
    "surface_latent_heat_flux": "slhf",
    "surface_sensible_heat_flux": "sshf",
    "surface_runoff": "sro",
    "sub_surface_runoff": "ssro",
    "snow_evaporation": "es",
    "snowmelt": "smlt",
    "skin_reservoir_content": "src",
}

NETCDF_TO_CDS_VARIABLE_MAP: dict[str, str] = {v: k for k, v in CDS_TO_NETCDF_VARIABLE_MAP.items()}


# ---------------------------------------------------------------------------
# Dataset metadata
# ---------------------------------------------------------------------------

DATASET_START_DATES: dict[str, str] = {
    "reanalysis-era5-single-levels": "1940-01-01",
    "reanalysis-era5-pressure-levels": "1940-01-01",
    "reanalysis-era5-land": "1950-01-01",
    "reanalysis-era5-single-levels-monthly-means": "1940-01-01",
    "reanalysis-era5-pressure-levels-monthly-means": "1940-01-01",
    "reanalysis-era5-land-monthly-means": "1950-01-01",
}

# All 37 ERA5 pressure levels in hPa
ALL_PRESSURE_LEVELS: list[int] = [
    1,
    2,
    3,
    5,
    7,
    10,
    20,
    30,
    50,
    70,
    100,
    125,
    150,
    175,
    200,
    225,
    250,
    300,
    350,
    400,
    450,
    500,
    550,
    600,
    650,
    700,
    750,
    775,
    800,
    825,
    850,
    875,
    900,
    925,
    950,
    975,
    1000,
]

# ERA5 data lag: final data available ~5 days after real-time.
# ERA5T (preliminary) may be available sooner.
ERA5_DATA_LAG_DAYS: int = 5


# ---------------------------------------------------------------------------
# Default variable sets per dataset category
# Focused on weather variables relevant to trading / energy / agriculture.
# ---------------------------------------------------------------------------

DEFAULT_SINGLE_LEVEL_VARIABLES: list[str] = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "total_precipitation",
    "surface_solar_radiation_downwards",
    "snowfall",
    "snow_depth",
    "surface_pressure",
    "mean_sea_level_pressure",
    "total_cloud_cover",
]

DEFAULT_PRESSURE_LEVEL_VARIABLES: list[str] = [
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "specific_humidity",
    "relative_humidity",
    "geopotential",
]

DEFAULT_LAND_VARIABLES: list[str] = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "total_precipitation",
    "snowfall",
    "snow_depth",
    "soil_temperature_level_1",
    "volumetric_soil_water_layer_1",
    "evaporation",
    "runoff",
    "surface_solar_radiation_downwards",
]

# Default bounding box: CONUS (Continental United States)
DEFAULT_BOUNDING_BOX: dict[str, float] = {
    "north": 50.0,
    "west": -125.0,
    "south": 24.0,
    "east": -66.0,
}


# ---------------------------------------------------------------------------
# Date and partition utilities
# ---------------------------------------------------------------------------


def generate_monthly_partitions(
    start_date: date,
    end_date: date,
) -> list[dict[str, str]]:
    """Generate year-month partition dicts from start to end date inclusive."""
    partitions: list[dict[str, str]] = []
    current = start_date.replace(day=1)
    while current <= end_date:
        partitions.append(
            {
                "year": str(current.year),
                "month": f"{current.month:02d}",
            }
        )
        if current.month == _DECEMBER:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return partitions


def generate_yearly_partitions(
    start_date: date,
    end_date: date,
) -> list[dict[str, str]]:
    """Generate year-only partition dicts from start to end year inclusive."""
    return [{"year": str(y)} for y in range(start_date.year, end_date.year + 1)]


def compute_default_end_date() -> date:
    """Compute default end date accounting for ERA5 ~5-day data lag."""
    return datetime.now(tz=timezone.utc).date() - timedelta(days=ERA5_DATA_LAG_DAYS)


def expand_hours(hours: list[str]) -> list[str]:
    """Expand hour config. ["*"] means all 24 hours."""
    if hours == ["*"]:
        return [f"{h:02d}:00" for h in range(24)]
    return hours


def days_in_month(year: int, month: int) -> int:
    """Return number of days in a given year-month."""
    return calendar.monthrange(year, month)[1]


def parse_date_string(date_str: str) -> date:
    """Parse ISO date string (YYYY-MM-DD) to date object."""
    return date.fromisoformat(date_str)
