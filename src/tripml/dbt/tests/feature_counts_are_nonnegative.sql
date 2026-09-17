select *
from {{ ref('training_features') }}
where
    pu_zone_trips_15m < 0
    or pu_zone_trips_60m < 0
    or do_zone_trips_60m < 0
