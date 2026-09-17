select
    cast(trip_id as varchar) as trip_id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pickup_zone_id as smallint) as pickup_zone_id,
    cast(dropoff_zone_id as smallint) as dropoff_zone_id,
    cast(trip_distance_miles as double) as trip_distance_miles,
    cast(passenger_count as smallint) as passenger_count,
    cast(fare_amount as double) as fare_amount,
    cast(actual_duration_seconds as integer) as actual_duration_seconds,
    cast(source_row_number as bigint) as source_row_number,
    cast(contract_version as varchar) as contract_version
from read_parquet(
    {{ sql_string_list(var('silver_paths')) }},
    union_by_name = true
)
