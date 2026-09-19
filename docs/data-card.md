# NYC TLC Yellow Taxi Data Card

## Intended use

This project uses monthly New York City Taxi and Limousine Commission (TLC) yellow-taxi trip
records to demonstrate reproducible batch ingestion, point-in-time feature computation, model
training, and simulated event-time streaming. It is not intended for decisions about individual
passengers, drivers, or neighborhoods.

## Source and provenance

- Publisher: New York City Taxi and Limousine Commission.
- Source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Unit: one vendor-reported yellow-taxi trip.
- Project partitions: January through April 2024; January through March train models and April is
  held out.
- Storage: source Parquet is downloaded at run time and is never committed. Each bronze object has
  a retrieval timestamp, source URL, byte count, and SHA-256 checksum.

TLC states that technology providers supply these records and that the agency does not guarantee
their accuracy or completeness. The publisher also warns that Parquet schemas may change between
releases. The ingestion boundary therefore checks required columns explicitly and fails loudly on
schema drift.

## Fields used

Pickup and drop-off timestamps, pickup and drop-off taxi-zone identifiers, trip distance,
passenger count, and fare amount are read from bronze. Silver adds a stable source-row trip ID,
duration in seconds, source row number, and contract version. Payment details, vendor identifiers,
and surcharge fields are not needed by the model and are not copied into silver.

Gold adds pickup hour-of-week and rolling 15- and 60-minute zone aggregates: trip counts, mean
duration, mean speed, and mean distance for the pickup zone, plus a 60-minute destination-zone
count. Each aggregate is based only on trips completed strictly before the subject pickup, and its
latest eligible completion timestamp is retained for auditability. Gold manifests identify every
silver input and the output by SHA-256.

## Quality contract

Under the default `passenger_count_policy: required` (silver contract 1.0), a row is valid when:

- every required field is present;
- pickup precedes drop-off and duration is from 60 seconds through 3 hours;
- distance is from 0 through 100 miles;
- passenger count is an integer from 0 through 9;
- pickup and drop-off zones are in the configured known-zone set (1 through 265 by default);
- fare is non-negative; and
- both timestamps are inside the declared source month.

Rule failures are counted independently. A partition is accepted only when it is non-empty and
the share of rows failing one or more rules is at or below the configured threshold (10 percent by
default). The default was calibrated against the official January 2024 partition, whose union
violation rate was 6.99 percent (207,260 of 2,964,624 rows); missing required values were the
largest contributor. This keeps normal publisher data usable while the 20 percent synthetic bad
partition remains a hard failure. Accepted partitions contain only valid rows; invalid rows are
retained with rule flags.
Rejected partitions are quarantined as a whole and cannot replace a prior silver partition.

The September 2026 audit found that this January-calibrated partition threshold rejects March
and April 2024. Their union violation rates are 14.12% and 13.88%; missing passenger counts alone
affect 11.90% and 11.63% of rows. These are observed source-completeness differences, not evidence
of a download or schema failure. The threshold remains unchanged. The separately configured
[pilot](../examples/training/real-data-pilot.yaml) uses January for training and February for
evaluation; it does not replace the planned April release holdout.

### Explicit unknown-count policy

The opt-in `passenger_count_policy: allow_unknown` produces silver contract 1.1. It treats a null
passenger count as an unknown optional predictor and preserves it as null. Every other required
field and row rule remains required. Known counts must still be integer values from zero through
nine; zero retains its observed meaning. Fractional, negative, out-of-range, NaN and infinite
counts remain invalid. The passenger-count column itself must be present.

The partition threshold remains 10%. Quality reports expose both total missing counts and missing
counts among otherwise valid rows; missingness is not hidden inside an imputed value. This changes
the admitted population, so models and rolling features use `gold-features-v2`, new model artifacts
and a separate data root. Training and serving retain native missing-value semantics, and reports
compare known/unknown passenger-count cohorts. The original strict pilot and quarantines remain
intact. See [ADR-0018](adr/0018-unknown-passenger-counts.md) and the
[release preparation runbook](runbooks/nullable-passenger-release.md).

## Known limitations

- Records describe completed taxi trips, not total travel demand, and reflect medallion-taxi
  coverage and vendor reporting behavior.
- Location IDs are coarse taxi zones, not exact coordinates.
- Passenger count is driver-reported and can be missing or inaccurate.
- Duration and speed features can be distorted by timestamp, distance, or zone errors that remain
  inside the declared bounds.
- TLC timestamps are local wall-clock values without an encoded UTC offset. Window ordering follows
  those published values; a future dataset spanning the autumn daylight-saving transition needs an
  explicit disambiguation policy.
- Historical performance may not transfer to other vehicle types, years, policy regimes, or
  unusual demand periods.
- The generated trip ID is stable for a byte-identical source partition but is not a publisher
  identifier and must not be used to join across revised source files.

The showcase reports observed violation rates rather than implying that contract-valid data is
ground truth.
