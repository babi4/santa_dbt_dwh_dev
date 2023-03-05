{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbtoss_groups_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from id), '{{ dbt_utils.type_string() }}'), 'null') as id,
    nullif(accurateCastOrNull(trim(BOTH '"' from name), '{{ dbt_utils.type_string() }}'), 'null') as name,
    {{ cast_to_boolean('default') }} as default,
    nullif(accurateCastOrNull(trim(BOTH '"' from game_id), '{{ dbt_utils.type_string() }}'), 'null') as game_id,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('created_at') }})) as created_at,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('updated_at') }})) as updated_at,
    accurateCastOrNull(involvment_type, '{{ dbt_utils.type_bigint() }}') as involvment_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbtoss_groups_ab1') }}
-- dbtoss_groups
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

