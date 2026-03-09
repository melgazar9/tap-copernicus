# Prompt: Build `tap-copernicus` — A Singer Tap for Copernicus Climate Data Store (ERA5 Reanalysis)

## Goal
Build a production-grade Singer tap (Meltano SDK) that extracts historical weather reanalysis data from the Copernicus Climate Data Store (CDS), specifically the ERA5 and ERA5-Land datasets. ERA5 provides hourly global weather data from 1940 to present — this is the "ground truth" weather dataset used for computing climatological normals, temperature anomalies, and training features for weather-driven trading models.

**Key challenge:** The CDS API is an async batch API — you submit a request, wait for it to process, then download the result. This is NOT a synchronous REST API. Results come as NetCDF or GRIB files that must be parsed.

This is an extractor ONLY — no loader logic.

---

## Reference Codebases (MUST study these first)

1. **tap-fred** at `~/code/github/personal/tap-fred/` — Config schema, thread-safe caching, `_throttle()`, `_safe_partition_extraction()`, `post_process()`, error handling
2. **tap-massive** at `~/code/github/personal/tap-massive/` — `_check_missing_fields()`, state management, partition patterns
3. **tap-fmp** at `~/code/github/personal/tap-fmp/` — Time slicing for large date ranges

---

## Copernicus CDS API Documentation

### Registration
- Free account at https://cds.climate.copernicus.eu/
- After registration, get API key from user profile page
- Key format: `{uid}:{api_key}` (e.g., `12345:abcdef-1234-5678-...`)

### Python Client
```bash
pip install cdsapi
```

The official `cdsapi` Python client handles the async request/wait/download cycle:
```python
import cdsapi
client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key="UID:API_KEY")
client.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": ["2m_temperature", "10m_u_component_of_wind"],
        "year": "2024",
        "month": "01",
        "day": ["01", "02", "03"],
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "area": [50, -125, 24, -66],  # North, West, South, East
        "format": "netcdf",
    },
    "output.nc"
)
```

### How It Works
1. `client.retrieve()` submits request to CDS queue
2. CDS processes request (can take seconds to hours depending on queue and data size)
3. Client polls until status = "completed"
4. Client downloads result file (NetCDF or GRIB)
5. You parse the file locally

### Datasets

**ERA5 Single Levels** (most important for us):
- Dataset name: `"reanalysis-era5-single-levels"`
- Hourly data from 1940 to present (~5 days lag)
- Surface/near-surface variables

**ERA5 Pressure Levels:**
- Dataset name: `"reanalysis-era5-pressure-levels"`
- Upper-atmosphere data on pressure levels
- Less relevant for surface weather trading

**ERA5-Land:**
- Dataset name: `"reanalysis-era5-land"`
- Higher resolution (0.1° vs 0.25°) but land-only
- Better for agriculture/crop signals
- From 1950 to present

### Key Variables

| CDS Variable Name | Description | Units | Trading Use |
|---|---|---|---|
| `2m_temperature` | 2m air temperature | K | HDD/CDD computation |
| `2m_dewpoint_temperature` | 2m dewpoint | K | Humidity, heat index |
| `10m_u_component_of_wind` | 10m U-wind | m/s | Wind speed |
| `10m_v_component_of_wind` | 10m V-wind | m/s | Wind speed |
| `total_precipitation` | Total precipitation | m | Rain/snow |
| `surface_solar_radiation_downwards` | Solar radiation | J/m² | Solar generation |
| `snowfall` | Snowfall | m of water equiv | Snow events |
| `snow_depth` | Snow depth | m of water equiv | Snow cover |

### Request Constraints
- **Max request size**: ~100,000 items (fields × times × grid points)
- For large requests, CDS will reject or queue for very long
- **Best practice**: Request 1 month at a time, for a specific set of variables
- Time resolution: hourly, but you can select specific hours (e.g., every 6 hours)
- Area: bounding box `[north, west, south, east]`
- Format: `"netcdf"` (recommended) or `"grib"`

### Rate Limits
- Max 2 concurrent requests per user
- Queue can be long during peak times
- Be patient and implement proper polling

---

## Directory Structure

```
tap-copernicus/
├── tap_copernicus/
│   ├── __init__.py
│   ├── __main__.py             # TapCopernicus.cli()
│   ├── tap.py                  # Main Tap class, config, stream registration
│   ├── client.py               # CopernicusStream base, async request handling
│   ├── netcdf_parser.py        # NetCDF parsing with xarray
│   ├── helpers.py              # Coordinate helpers, date utilities
│   └── streams/
│       ├── __init__.py
│       └── era5_streams.py     # ERA5 data extraction streams
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```

---

## Implementation Requirements

### 1. Tap Class (`tap.py`)

```python
class TapCopernicus(Tap):
    name = "tap-copernicus"
```

**Config Properties:**
- `cds_url` (StringType, default "https://cds.climate.copernicus.eu/api")
- `cds_key` (StringType, required, secret) — CDS API key in format `UID:API_KEY`
- `dataset` (StringType, default "reanalysis-era5-single-levels") — Which ERA5 dataset
- `variables` (ArrayType(StringType), default ["2m_temperature", "10m_u_component_of_wind", "10m_v_component_of_wind", "total_precipitation"]) — Variables to extract
- `start_date` (DateType, default "1940-01-01") — Earliest data
- `end_date` (DateType, optional) — Latest data. Default: 5 days ago (ERA5 lag).
- `hours` (ArrayType(StringType), default ["00:00", "06:00", "12:00", "18:00"]) — Which hours. Every 6 hours by default. `["*"]` = all 24 hours.
- `bounding_box` (ObjectType, default {"north": 50, "west": -125, "south": 24, "east": -66}) — CONUS
- `grid_resolution` (ArrayType(NumberType), default [1.0, 1.0]) — Output grid [lat_step, lon_step]. Default 1° (coarser than native 0.25° to reduce volume).
- `chunk_size_days` (IntegerType, default 31) — Days per CDS request. Max ~31 recommended.
- `output_format` (StringType, default "netcdf") — "netcdf" or "grib"
- `max_concurrent_requests` (IntegerType, default 2) — CDS allows max 2 concurrent
- `poll_interval_seconds` (IntegerType, default 30) — How often to poll CDS for completion
- `request_timeout_seconds` (IntegerType, default 7200) — Max wait per request (2 hours)
- `temp_dir` (StringType, optional) — Temp storage for downloaded files
- `strict_mode` (BooleanType, default False)

### 2. CDS Client Wrapper (`client.py`)

**Do NOT use `cdsapi` directly in streams.** Wrap it to add retry logic, timeout handling, and proper cleanup.

```python
class CDSClient:
    """Wrapper around cdsapi with retry, timeout, and cleanup."""

    def __init__(self, url: str, key: str, timeout: int = 7200, poll_interval: int = 30):
        self._client = cdsapi.Client(url=url, key=key, wait_until_complete=False)
        self._timeout = timeout
        self._poll_interval = poll_interval

    def retrieve(self, dataset: str, request: dict, output_path: str) -> str:
        """Submit request, poll until complete, download result."""
        result = self._client.retrieve(dataset, request)

        # Poll until completed or timeout
        start_time = time.time()
        while True:
            result.update()
            state = result.state

            if state == "completed":
                result.download(output_path)
                return output_path
            elif state == "failed":
                raise RuntimeError(f"CDS request failed: {result.error}")
            elif time.time() - start_time > self._timeout:
                raise TimeoutError(f"CDS request timed out after {self._timeout}s")

            logging.info(f"CDS request state: {state}. Waiting {self._poll_interval}s...")
            time.sleep(self._poll_interval)
```

**Base Stream:**
```python
class CopernicusStream(Stream, ABC):
    """Base class for Copernicus streams. Uses async CDS API, not REST."""
```

### 3. NetCDF Parser (`netcdf_parser.py`)

```python
import xarray as xr

class NetCDFParser:
    """Parse ERA5 NetCDF files into flat tabular records."""

    def parse_file(
        self,
        file_path: str,
        variables: list[str],
        grid_step: float = 1.0,
    ) -> list[dict]:
        """Parse NetCDF and return flat records."""
        ds = xr.open_dataset(file_path)
        records = []

        for var_name in ds.data_vars:
            if var_name not in self._map_cds_names(variables):
                continue

            da = ds[var_name]

            # Subsample grid if needed
            if grid_step > 0.25:  # Native resolution is 0.25°
                step = int(grid_step / 0.25)
                da = da.isel(latitude=slice(None, None, step), longitude=slice(None, None, step))

            for t_idx, time_val in enumerate(da.time.values):
                for lat in da.latitude.values:
                    for lon in da.longitude.values:
                        val = float(da.isel(time=t_idx).sel(latitude=lat, longitude=lon).values)
                        if np.isnan(val):
                            continue  # Skip NaN (ocean points for land-only vars)

                        records.append({
                            "time": pd.Timestamp(time_val).isoformat(),
                            "latitude": round(float(lat), 4),
                            "longitude": round(float(lon), 4),
                            "variable": var_name,
                            "value": round(val, 4),
                            "units": da.attrs.get("units", "unknown"),
                        })

        ds.close()
        return records

    @staticmethod
    def _map_cds_names(cds_variables: list[str]) -> list[str]:
        """Map CDS request variable names to NetCDF internal variable names.
        CDS uses long names in requests, NetCDF uses short names internally."""
        mapping = {
            "2m_temperature": "t2m",
            "2m_dewpoint_temperature": "d2m",
            "10m_u_component_of_wind": "u10",
            "10m_v_component_of_wind": "v10",
            "total_precipitation": "tp",
            "surface_solar_radiation_downwards": "ssrd",
            "snowfall": "sf",
            "snow_depth": "sd",
        }
        return [mapping.get(v, v) for v in cds_variables]
```

**CRITICAL: CDS variable naming**
- In the API request, you use long names: `"2m_temperature"`
- In the downloaded NetCDF, the variable is named `"t2m"`
- You MUST maintain a mapping between these

### 4. Streams

**Stream 1: `ERA5DataStream`** — Main data extraction
- **Partitioned by: year-month** (one CDS request per month)
- For each (year, month):
  1. Build CDS request dict
  2. Submit async request via CDSClient
  3. Wait for completion
  4. Download NetCDF to temp file
  5. Parse with NetCDFParser
  6. Yield records
  7. Delete temp file
- Primary keys: `["time", "latitude", "longitude", "variable"]`
- Replication key: `"time"` — for incremental sync
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("time", th.DateTimeType, required=True),
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("variable", th.StringType, required=True),
      th.Property("value", th.NumberType),
      th.Property("units", th.StringType),
      th.Property("dataset", th.StringType),  # e.g. "reanalysis-era5-single-levels"
  ).to_dict()
  ```

**Partition generation:**
```python
@property
def partitions(self):
    partitions = []
    start = parse_date(self.config["start_date"])
    end = parse_date(self.config.get("end_date", (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")))

    current = start.replace(day=1)
    while current <= end:
        partitions.append({
            "year": str(current.year),
            "month": f"{current.month:02d}",
        })
        # Advance to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return partitions
```

**CDS Request builder:**
```python
def _build_cds_request(self, year: str, month: str) -> dict:
    """Build CDS API request dictionary."""
    import calendar
    days_in_month = calendar.monthrange(int(year), int(month))[1]

    hours = self.config.get("hours", ["00:00", "06:00", "12:00", "18:00"])
    if hours == ["*"]:
        hours = [f"{h:02d}:00" for h in range(24)]

    bb = self.config.get("bounding_box", {"north": 50, "west": -125, "south": 24, "east": -66})
    grid = self.config.get("grid_resolution", [1.0, 1.0])

    return {
        "product_type": "reanalysis",
        "variable": self.config.get("variables", ["2m_temperature"]),
        "year": year,
        "month": month,
        "day": [f"{d:02d}" for d in range(1, days_in_month + 1)],
        "time": hours,
        "area": [bb["north"], bb["west"], bb["south"], bb["east"]],
        "grid": grid,
        "format": self.config.get("output_format", "netcdf"),
    }
```

### 5. Key Considerations

**Data Volume:**
- ERA5 at 1° resolution, CONUS, 4 times/day, 4 variables = ~12,000 records/day
- 1 year = ~4.4M records
- 84 years (1940-2024) = ~370M records
- At 0.25° native resolution: 16x more → not recommended for initial extraction

**CDS Queue Times:**
- Small requests (1 month, few variables): seconds to minutes
- Large requests (full year, many variables): hours
- Monthly chunking is the sweet spot

**ERA5 Data Lag:**
- ERA5 data is available ~5 days after real-time
- ERA5T (preliminary) may be available sooner
- Configure `end_date` to account for this

**NetCDF File Sizes:**
- 1 month, CONUS, 4 variables, 6-hourly, 1°: ~5-20MB
- Manageable. Delete after parsing.

**Error Handling:**
- CDS request "failed" state: Log ERROR, skip that month, continue
- CDS timeout: Log WARNING, skip, continue (request may still be processing server-side)
- Download failure: Retry with backoff
- NetCDF parse error: Log ERROR with file details, skip
- NaN values: Skip silently (ocean points for land variables)

### 6. pyproject.toml

```toml
[project]
name = "tap-copernicus"
version = "0.0.1"
description = "Singer tap for Copernicus Climate Data Store (ERA5 reanalysis weather data)"
requires-python = ">=3.10,<4.0"

dependencies = [
    "singer-sdk~=0.53.5",
    "requests~=2.32.3",
    "backoff>=2.2.1,<3.0.0",
    "cdsapi>=0.7.0",
    "xarray>=2024.1.0",
    "netcdf4>=1.6.0",
    "numpy>=1.26.0",
]

[project.scripts]
tap-copernicus = "tap_copernicus.tap:TapCopernicus.cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### 7. meltano.yml

```yaml
version: 1
send_anonymous_usage_stats: false
project_id: "tap-copernicus"

plugins:
  extractors:
  - name: tap-copernicus
    namespace: tap_copernicus
    pip_url: -e .
    capabilities:
      - state
      - catalog
      - discover
      - about
      - stream-maps
    settings:
      - name: cds_key
        kind: password
        sensitive: true
      - name: dataset
        kind: string
      - name: variables
        kind: array
      - name: start_date
        kind: date_iso8601
      - name: end_date
        kind: date_iso8601
      - name: hours
        kind: array
      - name: bounding_box
        kind: object
      - name: grid_resolution
        kind: array
      - name: chunk_size_days
        kind: integer
      - name: max_concurrent_requests
        kind: integer
      - name: poll_interval_seconds
        kind: integer
      - name: request_timeout_seconds
        kind: integer
      - name: strict_mode
        kind: boolean
    select:
      - era5_data.*
    config:
      cds_key: ${CDS_API_KEY}
      dataset: "reanalysis-era5-single-levels"
      variables:
        - "2m_temperature"
        - "2m_dewpoint_temperature"
        - "10m_u_component_of_wind"
        - "10m_v_component_of_wind"
        - "total_precipitation"
      start_date: "1940-01-01"
      hours: ["00:00", "06:00", "12:00", "18:00"]
      bounding_box:
        north: 50
        west: -125
        south: 24
        east: -66
      grid_resolution: [1.0, 1.0]
      poll_interval_seconds: 30
      request_timeout_seconds: 7200

  loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
```

---

## Critical Rules

1. **ALL imports at top of file.** No inline imports.
2. **CDS variable names ≠ NetCDF variable names.** Maintain the mapping (e.g., `"2m_temperature"` → `"t2m"`).
3. **ERA5 has ~5-day lag.** Don't request data that doesn't exist yet.
4. **Monthly chunking.** One CDS request per month. Larger requests take forever in queue.
5. **Max 2 concurrent CDS requests.** Don't exceed or your account gets throttled.
6. **Delete temp NetCDF files** after parsing.
7. **Temperature in Kelvin.** Output as-is with units field.
8. **Handle NaN** — ocean grid points have NaN for land-only variables. Skip silently.
9. **Use UV** for all Python execution.
10. **Match existing tap patterns** from tap-fred, tap-fmp, tap-massive.
