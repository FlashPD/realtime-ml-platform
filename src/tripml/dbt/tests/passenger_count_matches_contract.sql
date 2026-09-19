select trip_id
from {{ ref('training_features') }}
where
    (contract_version = '1.0' and passenger_count is null)
    or passenger_count < 0
    or passenger_count > 9
    or (contract_version = '1.0' and feature_model_version <> 'gold-features-v1')
    or (contract_version = '1.1' and feature_model_version <> 'gold-features-v2')
    or contract_version not in ('1.0', '1.1')
