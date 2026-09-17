{{
    config(
        materialized='external',
        location=var('gold_path'),
        format='parquet',
        options={'compression': 'zstd'}
    )
}}

{% set short_window = var('short_window_seconds') | int %}
{% set long_window = var('long_window_seconds') | int %}

with trips as (
    select * from {{ ref('stg_trips') }}
),

pickup_zone_events as (
    select
        null::varchar as trip_id,
        pickup_zone_id as zone_id,
        dropoff_datetime as event_time,
        'completion' as event_kind,
        actual_duration_seconds,
        trip_distance_miles,
        trip_distance_miles / (actual_duration_seconds / 3600.0) as speed_mph
    from trips

    union all

    select
        trip_id,
        pickup_zone_id as zone_id,
        pickup_datetime as event_time,
        'pickup' as event_kind,
        null::integer as actual_duration_seconds,
        null::double as trip_distance_miles,
        null::double as speed_mph
    from trips
),

pickup_zone_snapshots as (
    select
        trip_id,
        event_kind,
        count(actual_duration_seconds) over short_window as pu_zone_trips_short,
        avg(actual_duration_seconds) over short_window as pu_zone_mean_duration_short,
        avg(speed_mph) over short_window as pu_zone_mean_speed_short,
        avg(trip_distance_miles) over short_window as pu_zone_mean_distance_short,
        max(case when event_kind = 'completion' then event_time end)
            over short_window as pu_zone_short_as_of,
        count(actual_duration_seconds) over long_window as pu_zone_trips_long,
        avg(actual_duration_seconds) over long_window as pu_zone_mean_duration_long,
        avg(speed_mph) over long_window as pu_zone_mean_speed_long,
        avg(trip_distance_miles) over long_window as pu_zone_mean_distance_long,
        max(case when event_kind = 'completion' then event_time end)
            over long_window as pu_zone_long_as_of
    from pickup_zone_events
    window
        short_window as (
            partition by zone_id
            order by event_time
            range between interval '{{ short_window }} seconds' preceding and current row
            exclude group
        ),
        long_window as (
            partition by zone_id
            order by event_time
            range between interval '{{ long_window }} seconds' preceding and current row
            exclude group
        )
),

pickup_zone_features as (
    select * from pickup_zone_snapshots where event_kind = 'pickup'
),

dropoff_zone_events as (
    select
        null::varchar as trip_id,
        dropoff_zone_id as zone_id,
        dropoff_datetime as event_time,
        'completion' as event_kind,
        actual_duration_seconds
    from trips

    union all

    select
        trip_id,
        dropoff_zone_id as zone_id,
        pickup_datetime as event_time,
        'pickup' as event_kind,
        null::integer as actual_duration_seconds
    from trips
),

dropoff_zone_snapshots as (
    select
        trip_id,
        event_kind,
        count(actual_duration_seconds) over long_window as do_zone_trips_long,
        max(case when event_kind = 'completion' then event_time end)
            over long_window as do_zone_long_as_of
    from dropoff_zone_events
    window long_window as (
        partition by zone_id
        order by event_time
        range between interval '{{ long_window }} seconds' preceding and current row
        exclude group
    )
),

dropoff_zone_features as (
    select * from dropoff_zone_snapshots where event_kind = 'pickup'
)

select
    trips.trip_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.pickup_zone_id,
    trips.dropoff_zone_id,
    cast(
        extract(dow from trips.pickup_datetime) * 24
        + extract(hour from trips.pickup_datetime)
        as smallint
    ) as pickup_hour_of_week,
    trips.trip_distance_miles,
    trips.passenger_count,
    trips.actual_duration_seconds,
    cast(pu.pu_zone_trips_short as integer) as pu_zone_trips_15m,
    pu.pu_zone_mean_duration_short as pu_zone_mean_duration_15m,
    pu.pu_zone_mean_speed_short as pu_zone_mean_speed_15m,
    pu.pu_zone_mean_distance_short as pu_zone_mean_distance_15m,
    cast(pu.pu_zone_trips_long as integer) as pu_zone_trips_60m,
    pu.pu_zone_mean_duration_long as pu_zone_mean_duration_60m,
    pu.pu_zone_mean_speed_long as pu_zone_mean_speed_60m,
    pu.pu_zone_mean_distance_long as pu_zone_mean_distance_60m,
    cast(do_features.do_zone_trips_long as integer) as do_zone_trips_60m,
    pu.pu_zone_short_as_of as pu_zone_features_15m_as_of,
    pu.pu_zone_long_as_of as pu_zone_features_60m_as_of,
    do_features.do_zone_long_as_of as do_zone_features_60m_as_of,
    trips.contract_version,
    '{{ var("model_version") }}' as feature_model_version
from trips
inner join pickup_zone_features as pu using (trip_id)
inner join dropoff_zone_features as do_features using (trip_id)
where
    trips.pickup_datetime >= timestamp {{ sql_string(var('month_start')) }}
    and trips.pickup_datetime < timestamp {{ sql_string(var('month_end')) }}
