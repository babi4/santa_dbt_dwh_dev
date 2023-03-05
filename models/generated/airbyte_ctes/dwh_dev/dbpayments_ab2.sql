{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbpayments_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from id), '{{ dbt_utils.type_string() }}'), 'null') as id,
    nullif(accurateCastOrNull(trim(BOTH '"' from state), '{{ dbt_utils.type_string() }}'), 'null') as state,
    nullif(accurateCastOrNull(trim(BOTH '"' from status), '{{ dbt_utils.type_string() }}'), 'null') as status,
    nullif(accurateCastOrNull(trim(BOTH '"' from user_id), '{{ dbt_utils.type_string() }}'), 'null') as user_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from price_id), '{{ dbt_utils.type_string() }}'), 'null') as price_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from card_type), '{{ dbt_utils.type_string() }}'), 'null') as card_type,
    {{ cast_to_boolean('test_mode') }} as test_mode,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('created_at') }})) as created_at,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('updated_at') }})) as updated_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from description), '{{ dbt_utils.type_string() }}'), 'null') as description,
    nullif(accurateCastOrNull(trim(BOTH '"' from cardfirstsix), '{{ dbt_utils.type_string() }}'), 'null') as cardfirstsix,
    nullif(accurateCastOrNull(trim(BOTH '"' from cardlastfour), '{{ dbt_utils.type_string() }}'), 'null') as cardlastfour,
    nullif(accurateCastOrNull(trim(BOTH '"' from extra_data_str), '{{ dbt_utils.type_string() }}'), 'null') as extra_data_str,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbpayments_ab1') }}
-- dbpayments
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

