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

## Quality contract

A row is valid when:

- every required field is present;
- pickup precedes drop-off and duration is from 60 seconds through 3 hours;
- distance is from 0 through 100 miles;
- passenger count is from 0 through 9;
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

## Known limitations

- Records describe completed taxi trips, not total travel demand, and reflect medallion-taxi
  coverage and vendor reporting behavior.
- Location IDs are coarse taxi zones, not exact coordinates.
- Passenger count is driver-reported and can be missing or inaccurate.
- Duration and speed features can be distorted by timestamp, distance, or zone errors that remain
  inside the declared bounds.
- Historical performance may not transfer to other vehicle types, years, policy regimes, or
  unusual demand periods.
- The generated trip ID is stable for a byte-identical source partition but is not a publisher
  identifier and must not be used to join across revised source files.

The showcase reports observed violation rates rather than implying that contract-valid data is
ground truth.
