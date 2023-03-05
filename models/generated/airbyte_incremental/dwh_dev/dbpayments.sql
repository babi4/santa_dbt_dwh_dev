{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbpayments_scd') }}
select
    _airbyte_unique_key,
    id,
    state,
    status,
    user_id,
    price_id,
    card_type,
    test_mode,
    created_at,
    updated_at,
    description,
    cardfirstsix,
    cardlastfour,
    extra_data_str,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbpayments_hashid
from {{ ref('dbpayments_scd') }}
-- dbpayments from {{ source('dwh_dev', '_airbyte_raw_dbpayments') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

