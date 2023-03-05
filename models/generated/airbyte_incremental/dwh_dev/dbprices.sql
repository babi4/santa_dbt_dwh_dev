{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbprices_scd') }}
select
    _airbyte_unique_key,
    id,
    name,
    amount,
    currency,
    created_at,
    updated_at,
    max_players,
    payment_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbprices_hashid
from {{ ref('dbprices_scd') }}
-- dbprices from {{ source('dwh_dev', '_airbyte_raw_dbprices') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

