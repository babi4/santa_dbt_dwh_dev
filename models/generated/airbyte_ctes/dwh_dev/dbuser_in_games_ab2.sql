{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbuser_in_games_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from id), '{{ dbt_utils.type_string() }}'), 'null') as id,
    accurateCastOrNull(lat, '{{ dbt_utils.type_float() }}') as lat,
    nullif(accurateCastOrNull(trim(BOTH '"' from city), '{{ dbt_utils.type_string() }}'), 'null') as city,
    nullif(accurateCastOrNull(trim(BOTH '"' from flat), '{{ dbt_utils.type_string() }}'), 'null') as flat,
    accurateCastOrNull(long, '{{ dbt_utils.type_float() }}') as long,
    nullif(accurateCastOrNull(trim(BOTH '"' from floor), '{{ dbt_utils.type_string() }}'), 'null') as floor,
    nullif(accurateCastOrNull(trim(BOTH '"' from region), '{{ dbt_utils.type_string() }}'), 'null') as region,
    accurateCastOrNull(status, '{{ dbt_utils.type_bigint() }}') as status,
    nullif(accurateCastOrNull(trim(BOTH '"' from street), '{{ dbt_utils.type_string() }}'), 'null') as street,
    nullif(accurateCastOrNull(trim(BOTH '"' from address), '{{ dbt_utils.type_string() }}'), 'null') as address,
    nullif(accurateCastOrNull(trim(BOTH '"' from country), '{{ dbt_utils.type_string() }}'), 'null') as country,
    nullif(accurateCastOrNull(trim(BOTH '"' from game_id), '{{ dbt_utils.type_string() }}'), 'null') as game_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from user_id), '{{ dbt_utils.type_string() }}'), 'null') as user_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from entrance), '{{ dbt_utils.type_string() }}'), 'null') as entrance,
    nullif(accurateCastOrNull(trim(BOTH '"' from zip_code), '{{ dbt_utils.type_string() }}'), 'null') as zip_code,
    nullif(accurateCastOrNull(trim(BOTH '"' from telephone), '{{ dbt_utils.type_string() }}'), 'null') as telephone,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('created_at') }})) as created_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from street_num), '{{ dbt_utils.type_string() }}'), 'null') as street_num,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('updated_at') }})) as updated_at,
    accurateCastOrNull(joined_from, '{{ dbt_utils.type_bigint() }}') as joined_from,
    nullif(accurateCastOrNull(trim(BOTH '"' from wish_message), '{{ dbt_utils.type_string() }}'), 'null') as wish_message,
    nullif(accurateCastOrNull(trim(BOTH '"' from entrance_code), '{{ dbt_utils.type_string() }}'), 'null') as entrance_code,
    nullif(accurateCastOrNull(trim(BOTH '"' from toss_group_id), '{{ dbt_utils.type_string() }}'), 'null') as toss_group_id,
    nullif(accurateCastOrNull(trim(BOTH '"' from tracking_code), '{{ dbt_utils.type_string() }}'), 'null') as tracking_code,
    {{ cast_to_boolean('present_bought') }} as present_bought,
    {{ cast_to_boolean('present_sended') }} as present_sended,
    nullif(accurateCastOrNull(trim(BOTH '"' from unwish_message), '{{ dbt_utils.type_string() }}'), 'null') as unwish_message,
    nullif(accurateCastOrNull(trim(BOTH '"' from send_present_to), '{{ dbt_utils.type_string() }}'), 'null') as send_present_to,
    {{ cast_to_boolean('banned_from_chat') }} as banned_from_chat,
    nullif(accurateCastOrNull(trim(BOTH '"' from delivery_comemnt), '{{ dbt_utils.type_string() }}'), 'null') as delivery_comemnt,
    {{ cast_to_boolean('present_received') }} as present_received,
    nullif(accurateCastOrNull(trim(BOTH '"' from sended_present_url), '{{ dbt_utils.type_string() }}'), 'null') as sended_present_url,
    nullif(accurateCastOrNull(trim(BOTH '"' from tracking_description), '{{ dbt_utils.type_string() }}'), 'null') as tracking_description,
    {{ cast_to_boolean('congratulator_seen_congratulated') }} as congratulator_seen_congratulated,
    {{ cast_to_boolean('sended_present_notification_was_sent') }} as sended_present_notification_was_sent,
    {{ cast_to_boolean('received_present_notification_was_sent') }} as received_present_notification_was_sent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbuser_in_games_ab1') }}
-- dbuser_in_games
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

