{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbgames_users_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from game_id), '{{ dbt_utils.type_string() }}'), 'null') as game_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from user_id), '{{ dbt_utils.type_string() }}'), 'null') as user_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbgames_users_ab1') }}
-- dbgames_users
where 1 = 1

