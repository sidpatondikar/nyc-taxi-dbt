{% docs __overview__ %}
# NYC Taxi Analytics with dbt

Models 9.5M NYC yellow taxi trips (Jan–Mar 2024) from raw TLC files into
staging (views), intermediate (ephemeral) and mart (tables) layers on DuckDB.

**Start here:** `fct_trips` (one row per trip), `dim_zones` (one row per zone)
and `agg_daily_zone_revenue` (one row per day × borough).

**Key rules**
- Physically impossible rows are removed; questionable rows are kept and flagged.
- Revenue keeps reversals (negative fares) so refunds net to zero.
- Averages use only trips where `is_valid_for_analysis` is true.

Click the blue graph icon (bottom right) to see the lineage graph.
{% enddocs %}

{% docs trip_id %}
Surrogate key: a hash of vendor, pickup/dropoff times and locations, payment type,
distance, fare and total. TLC data has no trip ID.
{% enddocs %}

{% docs payment_type %}
TLC payment code: 0 = Flex fare, 1 = Credit card, 2 = Cash, 3 = No charge,
4 = Dispute, 5 = Unknown, 6 = Voided trip. Names are in the `payment_types` seed.
{% enddocs %}

{% docs location_id %}
TLC taxi zone ID (1–265). Join to `dim_zones.location_id`. 264 and 265 are unknown / outside NYC.
{% enddocs %}

{% docs file_month %}
Month of the TLC file the trip was published in (YYYY-MM), parsed from the file name.
Used as the incremental watermark. Can differ from the pickup month for trips near midnight
at month end.
{% enddocs %}

{% docs is_reversal %}
True when fare_amount is negative. Most of these cancel an earlier positive row for the
same trip (disputes, refunds). Kept so revenue nets out; excluded from averages.
{% enddocs %}

{% docs is_valid_for_analysis %}
True when the trip passes every quality flag: not a reversal, in its file month,
duration 1–300 min, distance 0–200 mi, fare ≤ $500. Use it to filter averages, not revenue totals.
{% enddocs %}