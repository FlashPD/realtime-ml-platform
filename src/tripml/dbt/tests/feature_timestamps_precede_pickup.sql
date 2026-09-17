select *
from {{ ref('training_features') }}
where
    pu_zone_features_15m_as_of >= pickup_datetime
    or pu_zone_features_60m_as_of >= pickup_datetime
    or do_zone_features_60m_as_of >= pickup_datetime
