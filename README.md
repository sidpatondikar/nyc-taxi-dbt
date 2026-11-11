# NYC Taxi Analytics with dbt

An analytics engineering project that models **9.5M NYC yellow taxi trips** (Jan–Mar 2024) into tested, documented staging, intermediate and mart layers using **dbt Core** on **DuckDB**.

It is the SQL/analytics-engineering companion to [NYC-Taxi-Pipeline](https://github.com/sidpatondikar/NYC-Taxi-Pipeline), which processes the same data with PySpark, Delta Lake, Kafka and Airflow. Same data, different job: that project moves and processes data; this one models it, tests it, and documents it.

![dbt](https://img.shields.io/badge/dbt_Core-1.12-FF694B)
![DuckDB](https://img.shields.io/badge/DuckDB-1.x-FFF000)
![dbt_utils](https://img.shields.io/badge/package-dbt__utils-FF694B)
![Python](https://img.shields.io/badge/Python-3.12-blue)

> **Status:** work in progress. Sections are marked ✅ **Built** or 🔜 **Planned**. Every number in this README comes from an actual run unless it is marked planned.

---

## Architecture

```
  data/raw/  (not in Git)                                                  DuckDB file: nyc_taxi.duckdb
  ┌──────────────────────────┐
  │ yellow_tripdata_2024-01  │      SOURCES         STAGING (views)        INTERMEDIATE (ephemeral)        MARTS (tables)
  │ yellow_tripdata_2024-02  │──┐
  │ yellow_tripdata_2024-03  │  │  ┌────────────┐   ┌──────────────────┐                                   ┌───────────────────────────┐
  │   (.parquet, 9.55M rows) │  └─▶│ yellow_    │──▶│ stg_yellow_trips │──┐   ┌──────────────────────┐  ┌──▶│ fct_trips            ✅    │
  └──────────────────────────┘     │ trips      │   └──────────────────┘  ├──▶│ int_trips_enriched   │──┤   │ (incremental)             │
  ┌──────────────────────────┐     ├────────────┤   ┌──────────────────┐  │   │ zone join, derived   │  │   └───────────────────────────┘
  │ taxi_zone_lookup.csv     │────▶│ taxi_zones │──▶│ stg_taxi_zones   │──┤   │ metrics, DQ flags    │  └──▶┌───────────────────────────┐
  │   (265 zones)            │     └────────────┘   └──────────────────┘  │   └──────────────────────┘      │ agg_daily_zone_revenue ✅ │
  └──────────────────────────┘                                            │                                 └───────────────────────────┘
                                   seeds/payment_types.csv ✅             └────────────────────────────────▶┌───────────────────────────┐
                                   snapshots/snap_taxi_zones 🔜 (SCD2)                                       │ dim_zones            ✅    │
                                                                                                             └───────────────────────────┘
```

`dbt build` compiles each model, wraps it in `CREATE VIEW` / `CREATE TABLE`, runs it in DuckDB in dependency order (resolved from `ref()` / `source()`), and runs each model's tests before building anything downstream of it.

## Tech stack

| Piece | Choice | Why |
|---|---|---|
| Transformation | dbt Core 1.12 (`dbt-duckdb` adapter) | Industry-standard SQL modeling, testing and docs |
| Warehouse | DuckDB (single local file) | Zero setup; reads Parquet/CSV directly; fast on 10M rows |
| Packages | `dbt_utils` | Surrogate keys, `expression_is_true`, `accepted_range` tests |
| Data | NYC TLC Yellow Taxi trip records (Parquet) + Taxi Zone Lookup (CSV) | Same domain as the Spark project |
| CI 🔜 | GitHub Actions running `dbt build` on a sample | Catch breaking changes on every push |
| Docs 🔜 | `dbt docs generate` published to GitHub Pages | Browsable lineage graph |

---

## Model layers

| Layer | Materialization | Responsibility | Models |
|---|---|---|---|
| **Sources** | external files | Declare raw data once; models reference it by name | `raw.yellow_trips`, `raw.taxi_zones` |
| **Staging** | view | 1:1 with each source: rename to snake_case, cast types. No joins, no business logic | `stg_yellow_trips`, `stg_taxi_zones` |
| **Intermediate** | ephemeral | Joins, derived metrics, data-quality flags | `int_trips_enriched` |
| **Marts** | table (`fct_trips`: incremental) | Star schema that analysts and dashboards query | `fct_trips`, `dim_zones`, `agg_daily_zone_revenue` |

Materializations are set per folder in `dbt_project.yml` and can be overridden per model.

**Why these materializations:** staging logic is light (renames and casts), so views avoid storing a second copy of the raw data. The intermediate model is only a building block, so it's ephemeral and gets inlined as a CTE into whatever uses it. Marts hold the expensive work (joins, deduplication) and are queried often, so they're stored as tables.

### Sources: `models/staging/_sources.yml`
- `raw.yellow_trips` reads all monthly Parquet files with one glob. `filename = true` keeps the source file name for lineage and incremental logic.
- `raw.taxi_zones` reads the TLC zone lookup CSV.
- Moving to a warehouse such as Snowflake means editing this one file; the models stay the same.

### Staging
- **`stg_yellow_trips`**: 19 raw columns renamed (`PULocationID` → `pickup_location_id`, `tpep_pickup_datetime` → `pickup_at`, …), IDs cast to integer, plus `source_file`. **9,554,778 rows.**
- **`stg_taxi_zones`**: `location_id`, `borough`, `zone_name`, `service_zone`. **265 rows.**

### Intermediate: `int_trips_enriched`
Built as four CTEs, each doing one job:
1. **Join**: trips joined to zones **twice** (pickup and dropoff) for real borough and zone names.
2. **Derive**: `trip_duration_min`, `avg_speed_mph`, `tip_pct`, `pickup_date`, `pickup_hour`, `is_weekend`, and `file_month` (parsed from the file name with a regex). Divisions use the custom **`safe_divide`** macro, so a zero duration or zero fare returns null instead of an error.
3. **Flag**: `is_reversal`, `is_in_file_month`, `has_valid_duration`, `has_valid_distance`, `has_valid_fare_cap`, and a combined `is_valid_for_analysis`.
4. **Filter**: removes only *physically impossible* rows (dropoff before pickup; pickup more than a day outside the file's month). **189 rows removed.**

Design rule: **remove what is impossible, flag what is questionable.** Each mart decides which flags to filter on.

### Marts
- **`dim_zones`**: one row per zone, with `is_airport` and `is_unknown_location` flags.
- **`fct_trips`**: grain is **one row per trip**. Holds IDs, measures and quality flags; zone descriptions live in `dim_zones` (star schema).
  - **Surrogate key** `trip_id` from `dbt_utils.generate_surrogate_key` over 9 columns, because TLC data has no trip ID.
  - **Deduplication** with `qualify row_number() over (partition by trip_id ...) = 1`.
  - **9,554,588 rows.**
  - **Incremental** (`unique_key='trip_id'`, `delete+insert`). Each run loads only TLC file months not already in the table: `where file_month > (select max(file_month) from {{ this }})`. See [Incremental load](#incremental-load) below.
- **`agg_daily_zone_revenue`**: grain is **one row per pickup date × pickup borough** (721 rows). Revenue totals keep reversals so refunds net to zero; averages (fare, distance, duration) use only `is_valid_for_analysis` trips, and average tip % uses card trips only, because TLC doesn't record cash tips.
  - Manhattan ≈ **$194M** across 8.4M trips; Queens ≈ **$57M** across 0.8M trips, with an average fare of about $53, three times Manhattan's (JFK and LaGuardia).

### Incremental load

The watermark is the **source file month**, not the pickup timestamp. 47 real trips (e.g. Jan 31 23:59) are published in the *next* month's file, and a `pickup_at > max(pickup_at)` filter would skip them forever. Filtering on `file_month` loads them with their file.

Simulated monthly releases by adding one Parquet file to `data/raw/` at a time:

| Run | Rows processed | Time |
|---|---|---|
| First load (Jan, `--full-refresh`) | 2,964,563 | 8.76 s |
| Incremental: add Feb | 3,007,516 | 9.67 s |
| Incremental: add Mar | 3,582,509 | 11.39 s |
| Full refresh, all 3 months | 9,554,588 | 46.96 s |

- The incremental and full-refresh tables are identical: 9,554,588 rows either way.
- Adding a month incrementally took **11.4 s vs 47.0 s for a full rebuild (~4× faster)**, processing 37% of the rows. The gap grows with every month of history.
- Known limits: corrected re-published months aren't reloaded (would need a lookback window or a periodic `--full-refresh`), and hard deletes upstream aren't detected.

---

## Data quality

### Tests: 31 total, all passing

| Layer | Tests |
|---|---|
| Staging (11) | `unique` / `not_null` on zone ID; `not_null` on timestamps and location IDs; `accepted_values` on `payment_type`; `relationships` from pickup/dropoff location to zones; conditional `not_null` on `passenger_count` (`where: payment_type != 0`) |
| Intermediate (6) | `not_null` on both boroughs (join worked) and `file_month` (regex worked); `accepted_values` on borough; `dbt_utils.expression_is_true` (`dropoff_at >= pickup_at`); `dbt_utils.accepted_range` (duration ≥ 0) |
| Seed (3) | `unique` / `not_null` on `payment_types.payment_type`; `not_null` on `payment_type_name` |
| Marts (10) | `unique` / `not_null` on `dim_zones.location_id` and `fct_trips.trip_id`; `relationships` from fact to dimension for both location IDs and from `fct_trips.payment_type` to the `payment_types` seed; `dbt_utils.unique_combination_of_columns` on (`pickup_date`, `pickup_borough`) for the aggregate's grain; `not_null` on both |
| Singular (1) | `assert_agg_revenue_matches_fct`: total revenue in `agg_daily_zone_revenue` reconciles to `fct_trips` (within $1 for rounding) |

### What the tests and profiling found

| Finding | Rows | What it was | Decision |
|---|---|---|---|
| `payment_type = 0` failed `accepted_values` | 751,962 (7.9%), **$14.29M** in fares | Official TLC code for **Flex Fare** (confirmed in the TLC data dictionary, Mar 2025) | Added `0` to the allowed codes |
| Null `passenger_count` | 751,962 | **Exactly** the flex-fare trips; those trips never record passengers | Kept the trips; test now requires the value only when `payment_type != 0` |
| Negative fares | 136,567 (1.4%), **−$2.41M** | Mostly disputes/refunds. **101,293 (74%) have an exact positive twin**, so they're reversal records | **Kept and flagged** (`is_reversal`). Dropping only the negative side would overstate revenue by about $2.4M |
| Pickup dates far outside the file month | 9 | Dates in 2002, 2008, 2009 (broken meter clocks) | Removed |
| Trips in the next month's file | 47 | Real trips starting minutes before midnight on the last day of a month | Kept and flagged (`is_in_file_month = false`). These are late-arriving data for the incremental model |
| Dropoff before pickup | 180 | Clock errors | Removed |
| Zero distance / > 200 mi / fare > $500 | 215,764 / 170 / 117 | GPS glitches, cancellations, meter errors | Kept and flagged |
| Duplicate trip | 1 pair | Same trip published twice (every column identical) | Deduplicated in `fct_trips` |

**Result:** 9,179,399 trips (**96.1%**) pass every quality flag and are used for averages. All revenue-bearing rows are kept for totals.

### Design decision: where to deduplicate
The convention is to deduplicate in staging. Here, staging is views, so a window function over 9.5M rows would re-run inside every downstream test; the build stalled for more than 10 minutes. Moving the deduplication into the `fct_trips` **table** runs it once at build time, and the full build takes about 1 min. The alternative is to materialize `stg_yellow_trips` as a table, trading storage for compute.

---

## 🔜 Planned

| Feature | What it will show |
|---|---|
| **Snapshot** `snap_taxi_zones` | SCD Type 2 history (`dbt_valid_from` / `dbt_valid_to`) after editing a zone |
| **Source freshness** | Configured as a warning; documented as non-meaningful for historical files |
| **Docs** | Descriptions on all mart columns, `dbt docs generate`, lineage graph on GitHub Pages |
| **CI** | GitHub Actions: install dbt, run `dbt build` against a committed sample (`ci` target) on every push; optionally Slim CI with `state:modified+` |
| **Stretch: Snowflake** | Same project on a second target: raw loaded via stage + `COPY INTO`, sources repointed, dialect differences handled with dbt cross-database macros |

---

## Relationship to the Spark project

| NYC-Taxi-Pipeline (PySpark) | This project (dbt) |
|---|---|
| Bronze | Sources |
| `bronze_to_silver.py` | Staging + intermediate |
| `silver_to_gold.py` | Marts |
| `data_quality_checks.py` | dbt tests (YAML) |
| Airflow DAG (task order wired by hand) | dbt DAG (inferred from `ref()`) |

Modeling the zone dimension from the official TLC lookup showed that the Spark project's borough mapping, which assigns boroughs by location ID ranges, mislabels zones. For example, Upper East Side (IDs 236/237) is labelled Staten Island and Midtown Center (161) is labelled Queens, because TLC IDs are ordered alphabetically by zone, not grouped by borough. Profiling also showed that filtering out negative fares, as the Spark Silver layer does, keeps the positive side of reversed trips.

---

## Project structure

```
nyc_taxi_dbt/
├── models/
│   ├── staging/
│   │   ├── _sources.yml            # raw Parquet + CSV declared as sources
│   │   ├── _staging.yml            # 11 tests + descriptions
│   │   ├── stg_yellow_trips.sql
│   │   └── stg_taxi_zones.sql
│   ├── intermediate/
│   │   ├── _intermediate.yml       # 6 tests
│   │   └── int_trips_enriched.sql
│   └── marts/
│       ├── _marts.yml              # 9 tests
│       ├── dim_zones.sql
│       ├── fct_trips.sql           # incremental
│       └── agg_daily_zone_revenue.sql
├── tests/
│   └── assert_agg_revenue_matches_fct.sql   # singular reconciliation test
├── seeds/
│   ├── payment_types.csv           # TLC payment codes → names (7 rows)
│   └── _seeds.yml                  # column types + 3 tests
├── snapshots/    🔜 snap_taxi_zones.sql
├── macros/
│   ├── safe_divide.sql             # Jinja macro: division that returns null on a zero denominator
│   └── _macros.yml                 # macro docs
├── data/raw/     (gitignored) TLC Parquet + zone CSV
├── dbt_project.yml
├── packages.yml                    # dbt_utils
└── profiles.yml                    # DuckDB connection (project-local)
```

## How to run

```bash
python -m venv dbt-venv && source dbt-venv/Scripts/activate   # Windows Git Bash
pip install dbt-core dbt-duckdb

mkdir -p data/raw
for m in 01 02 03; do
  curl -o data/raw/yellow_tripdata_2024-$m.parquet \
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet
done
curl -o data/raw/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

dbt deps      # install dbt_utils
dbt debug     # check the connection
dbt build     # build all models and run all tests (~1 min)
```

## Data source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page): Yellow Taxi trips, Jan–Mar 2024, published by the NYC Taxi & Limousine Commission.
