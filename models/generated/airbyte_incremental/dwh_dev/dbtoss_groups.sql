{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbtoss_groups_scd') }}
select
    _airbyte_unique_key,
    id,
    name,
    default,
    game_id,
    created_at,
    updated_at,
    involvment_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbtoss_groups_hashid
from {{ ref('dbtoss_groups_scd') }}
-- dbtoss_groups from {{ source('dwh_dev', '_airbyte_raw_dbtoss_groups') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

