"""ERA5 stream definitions for tap-copernicus."""

from tap_copernicus.streams.era5_streams import (
    ERA5HourlyLandStream,
    ERA5HourlyPressureLevelsStream,
    ERA5HourlySingleLevelsStream,
    ERA5MonthlyLandStream,
    ERA5MonthlyPressureLevelsStream,
    ERA5MonthlySingleLevelsStream,
)

__all__ = [
    "ERA5HourlyLandStream",
    "ERA5HourlyPressureLevelsStream",
    "ERA5HourlySingleLevelsStream",
    "ERA5MonthlyLandStream",
    "ERA5MonthlyPressureLevelsStream",
    "ERA5MonthlySingleLevelsStream",
]
