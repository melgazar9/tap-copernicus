"""Core tests for tap-copernicus.

These tests validate configuration, schema definitions, and utility functions
WITHOUT requiring a live CDS API connection. For production validation,
use meltano el with real credentials.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from tap_copernicus.helpers import (
    CDS_TO_NETCDF_VARIABLE_MAP,
    DATASET_START_DATES,
    NETCDF_TO_CDS_VARIABLE_MAP,
    compute_default_end_date,
    days_in_month,
    expand_hours,
    generate_monthly_partitions,
    generate_yearly_partitions,
    parse_date_string,
)
from tap_copernicus.tap import TapCopernicus

# ---------------------------------------------------------------------------
# Helper / utility tests
# ---------------------------------------------------------------------------


class TestHelpers:
    """Tests for helper functions and constants."""

    def test_variable_map_bidirectional(self):
        """Every CDS->NetCDF mapping must have a reverse mapping."""
        for cds_name, nc_name in CDS_TO_NETCDF_VARIABLE_MAP.items():
            assert nc_name in NETCDF_TO_CDS_VARIABLE_MAP
            assert NETCDF_TO_CDS_VARIABLE_MAP[nc_name] == cds_name

    def test_dataset_start_dates_all_present(self):
        """All 6 ERA5 datasets must have start dates defined."""
        expected_datasets = {
            "reanalysis-era5-single-levels",
            "reanalysis-era5-pressure-levels",
            "reanalysis-era5-land",
            "reanalysis-era5-single-levels-monthly-means",
            "reanalysis-era5-pressure-levels-monthly-means",
            "reanalysis-era5-land-monthly-means",
        }
        assert set(DATASET_START_DATES.keys()) == expected_datasets

    def test_generate_monthly_partitions(self):
        """Monthly partitions should cover start to end inclusive."""
        partitions = generate_monthly_partitions(
            date(2024, 1, 1), date(2024, 3, 15),
        )
        assert len(partitions) == 3
        assert partitions[0] == {"year": "2024", "month": "01"}
        assert partitions[1] == {"year": "2024", "month": "02"}
        assert partitions[2] == {"year": "2024", "month": "03"}

    def test_generate_monthly_partitions_cross_year(self):
        """Partitions should cross year boundaries correctly."""
        partitions = generate_monthly_partitions(
            date(2023, 11, 1), date(2024, 2, 1),
        )
        assert len(partitions) == 4
        assert partitions[0] == {"year": "2023", "month": "11"}
        assert partitions[3] == {"year": "2024", "month": "02"}

    def test_generate_yearly_partitions(self):
        """Yearly partitions should span start to end year inclusive."""
        partitions = generate_yearly_partitions(date(2020, 6, 1), date(2023, 3, 1))
        assert len(partitions) == 4
        assert partitions[0] == {"year": "2020"}
        assert partitions[3] == {"year": "2023"}

    def test_compute_default_end_date(self):
        """Default end date should be ~5 days before today."""
        end_date = compute_default_end_date()
        today = datetime.now(tz=timezone.utc).date()
        lag = (today - end_date).days
        assert 4 <= lag <= 6

    def test_expand_hours_wildcard(self):
        """Wildcard ['*'] should expand to all 24 hours."""
        hours = expand_hours(["*"])
        assert len(hours) == 24
        assert hours[0] == "00:00"
        assert hours[23] == "23:00"

    def test_expand_hours_explicit(self):
        """Explicit hours should pass through unchanged."""
        hours = ["06:00", "18:00"]
        assert expand_hours(hours) == hours

    def test_days_in_month(self):
        """Days in month should handle Feb leap year."""
        assert days_in_month(2024, 2) == 29  # Leap year
        assert days_in_month(2023, 2) == 28
        assert days_in_month(2024, 1) == 31

    def test_parse_date_string(self):
        """Should parse ISO date strings."""
        assert parse_date_string("2024-03-15") == date(2024, 3, 15)


# ---------------------------------------------------------------------------
# Tap configuration tests
# ---------------------------------------------------------------------------


class TestTapConfig:
    """Tests for TapCopernicus configuration schema."""

    def test_config_schema_has_required_fields(self):
        """Config schema must include cds_api_key as required."""
        schema = TapCopernicus.config_jsonschema
        props = schema.get("properties", {})
        assert "cds_api_key" in props
        assert "cds_api_key" in schema.get("required", [])

    def test_config_schema_has_all_settings(self):
        """All documented settings must be in the config schema."""
        schema = TapCopernicus.config_jsonschema
        props = schema.get("properties", {})
        expected_settings = [
            "cds_api_key", "cds_url", "variables", "product_types",
            "pressure_levels", "start_date", "end_date", "hours",
            "bounding_box", "grid_resolution", "poll_interval_seconds",
            "request_timeout_seconds", "output_format", "temp_dir",
            "strict_mode",
        ]
        for setting in expected_settings:
            assert setting in props, f"Missing config property: {setting}"

    def test_cds_api_key_is_secret(self):
        """CDS API key must be marked as secret."""
        schema = TapCopernicus.config_jsonschema
        props = schema.get("properties", {})
        assert props["cds_api_key"].get("secret") is True


# ---------------------------------------------------------------------------
# Stream definition tests
# ---------------------------------------------------------------------------


class TestStreamDefinitions:
    """Tests for stream class attributes and schemas."""

    def test_all_streams_discovered(self):
        """Tap should discover all 6 ERA5 streams."""
        tap = TapCopernicus(
            config={"cds_api_key": "test_key"},
            parse_env_config=False,
        )
        streams = tap.discover_streams()
        assert len(streams) == 6

        stream_names = {s.name for s in streams}
        expected = {
            "era5_hourly_single_levels",
            "era5_hourly_pressure_levels",
            "era5_hourly_land",
            "era5_monthly_single_levels",
            "era5_monthly_pressure_levels",
            "era5_monthly_land",
        }
        assert stream_names == expected

    def test_pressure_level_streams_have_pressure_level_in_schema(self):
        """Pressure level streams must include pressure_level in schema."""
        tap = TapCopernicus(
            config={"cds_api_key": "test_key"},
            parse_env_config=False,
        )
        for stream in tap.discover_streams():
            if "pressure" in stream.name:
                assert "pressure_level" in stream.schema["properties"]
            else:
                assert "pressure_level" not in stream.schema["properties"]

    def test_all_streams_have_required_schema_fields(self):
        """All streams must have the core EAV schema fields."""
        tap = TapCopernicus(
            config={"cds_api_key": "test_key"},
            parse_env_config=False,
        )
        required_fields = {"time", "latitude", "longitude", "variable", "value", "units"}
        for stream in tap.discover_streams():
            schema_fields = set(stream.schema["properties"].keys())
            missing = required_fields - schema_fields
            assert not missing, f"Stream '{stream.name}' missing fields: {missing}"

    def test_land_streams_start_at_1950(self):
        """ERA5-Land streams should not allow data before 1950."""
        tap = TapCopernicus(
            config={"cds_api_key": "test_key", "start_date": "1940-01-01"},
            parse_env_config=False,
        )
        for stream in tap.discover_streams():
            if "land" in stream.name:
                assert stream.earliest_available_date == "1950-01-01"
                # Verify start date clamping
                resolved = stream._resolve_start_date()
                assert resolved == date(1950, 1, 1)

    def test_monthly_streams_use_yearly_partitions(self):
        """Monthly means streams should partition by year, not year-month."""
        tap = TapCopernicus(
            config={
                "cds_api_key": "test_key",
                "start_date": "2023-01-01",
                "end_date": "2024-12-31",
            },
            parse_env_config=False,
        )
        for stream in tap.discover_streams():
            if "monthly" in stream.name:
                partitions = stream.partitions
                assert all("month" not in p for p in partitions)
                assert all("year" in p for p in partitions)
            else:
                partitions = stream.partitions
                assert all("month" in p for p in partitions)
