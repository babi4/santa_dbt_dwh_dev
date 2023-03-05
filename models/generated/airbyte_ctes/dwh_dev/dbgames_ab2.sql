{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbgames_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from id), '{{ dbt_utils.type_string() }}'), 'null') as id,
    nullif(accurateCastOrNull(trim(BOTH '"' from name), '{{ dbt_utils.type_string() }}'), 'null') as name,
    nullif(accurateCastOrNull(trim(BOTH '"' from state), '{{ dbt_utils.type_string() }}'), 'null') as state,
    nullif(accurateCastOrNull(trim(BOTH '"' from message), '{{ dbt_utils.type_string() }}'), 'null') as message,
    accurateCastOrNull(currency, '{{ dbt_utils.type_bigint() }}') as currency,
    accurateCastOrNull(game_type, '{{ dbt_utils.type_bigint() }}') as game_type,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('created_at') }})) as created_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from payment_id), '{{ dbt_utils.type_string() }}'), 'null') as payment_id,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('updated_at') }})) as updated_at,
    toDate(parseDateTimeBestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('join_before') }}))) as join_before,
    accurateCastOrNull(max_players, '{{ dbt_utils.type_bigint() }}') as max_players,
    accurateCastOrNull(paid_status, '{{ dbt_utils.type_bigint() }}') as paid_status,
    nullif(accurateCastOrNull(trim(BOTH '"' from address_hint), '{{ dbt_utils.type_string() }}'), 'null') as address_hint,
    accurateCastOrNull(created_from, '{{ dbt_utils.type_bigint() }}') as created_from,
    accurateCastOrNull(present_price, '{{ dbt_utils.type_bigint() }}') as present_price,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('toss_datetime') }})) as toss_datetime,
    nullif(accurateCastOrNull(trim(BOTH '"' from messenger_code), '{{ dbt_utils.type_string() }}'), 'null') as messenger_code,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('real_toss_time') }})) as real_toss_time,
    nullif(accurateCastOrNull(trim(BOTH '"' from text_wish_message), '{{ dbt_utils.type_string() }}'), 'null') as text_wish_message,
    toDate(parseDateTimeBestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('presents_send_date') }}))) as presents_send_date,
    {{ cast_to_boolean('wishlist_available') }} as wishlist_available,
    nullif(accurateCastOrNull(trim(BOTH '"' from text_unwish_message), '{{ dbt_utils.type_string() }}'), 'null') as text_unwish_message,
    accurateCastOrNull(where_to_get_presents, '{{ dbt_utils.type_bigint() }}') as where_to_get_presents,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbgames_ab1') }}
-- dbgames
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

