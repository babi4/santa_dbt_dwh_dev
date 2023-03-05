{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('dwh_dev', '_airbyte_raw_dbuser_in_games') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['lat'], ['lat']) }} as lat,
    {{ json_extract_scalar('_airbyte_data', ['city'], ['city']) }} as city,
    {{ json_extract_scalar('_airbyte_data', ['flat'], ['flat']) }} as flat,
    {{ json_extract_scalar('_airbyte_data', ['long'], ['long']) }} as long,
    {{ json_extract_scalar('_airbyte_data', ['floor'], ['floor']) }} as floor,
    {{ json_extract_scalar('_airbyte_data', ['region'], ['region']) }} as region,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['street'], ['street']) }} as street,
    {{ json_extract_scalar('_airbyte_data', ['address'], ['address']) }} as address,
    {{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as country,
    {{ json_extract_scalar('_airbyte_data', ['game_id'], ['game_id']) }} as game_id,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    {{ json_extract_scalar('_airbyte_data', ['entrance'], ['entrance']) }} as entrance,
    {{ json_extract_scalar('_airbyte_data', ['zip_code'], ['zip_code']) }} as zip_code,
    {{ json_extract_scalar('_airbyte_data', ['telephone'], ['telephone']) }} as telephone,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['street_num'], ['street_num']) }} as street_num,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['joined_from'], ['joined_from']) }} as joined_from,
    {{ json_extract_scalar('_airbyte_data', ['wish_message'], ['wish_message']) }} as wish_message,
    {{ json_extract_scalar('_airbyte_data', ['entrance_code'], ['entrance_code']) }} as entrance_code,
    {{ json_extract_scalar('_airbyte_data', ['toss_group_id'], ['toss_group_id']) }} as toss_group_id,
    {{ json_extract_scalar('_airbyte_data', ['tracking_code'], ['tracking_code']) }} as tracking_code,
    {{ json_extract_scalar('_airbyte_data', ['present_bought'], ['present_bought']) }} as present_bought,
    {{ json_extract_scalar('_airbyte_data', ['present_sended'], ['present_sended']) }} as present_sended,
    {{ json_extract_scalar('_airbyte_data', ['unwish_message'], ['unwish_message']) }} as unwish_message,
    {{ json_extract_scalar('_airbyte_data', ['send_present_to'], ['send_present_to']) }} as send_present_to,
    {{ json_extract_scalar('_airbyte_data', ['banned_from_chat'], ['banned_from_chat']) }} as banned_from_chat,
    {{ json_extract_scalar('_airbyte_data', ['delivery_comemnt'], ['delivery_comemnt']) }} as delivery_comemnt,
    {{ json_extract_scalar('_airbyte_data', ['present_received'], ['present_received']) }} as present_received,
    {{ json_extract_scalar('_airbyte_data', ['sended_present_url'], ['sended_present_url']) }} as sended_present_url,
    {{ json_extract_scalar('_airbyte_data', ['tracking_description'], ['tracking_description']) }} as tracking_description,
    {{ json_extract_scalar('_airbyte_data', ['congratulator_seen_congratulated'], ['congratulator_seen_congratulated']) }} as congratulator_seen_congratulated,
    {{ json_extract_scalar('_airbyte_data', ['sended_present_notification_was_sent'], ['sended_present_notification_was_sent']) }} as sended_present_notification_was_sent,
    {{ json_extract_scalar('_airbyte_data', ['received_present_notification_was_sent'], ['received_present_notification_was_sent']) }} as received_present_notification_was_sent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('dwh_dev', '_airbyte_raw_dbuser_in_games') }} as table_alias
-- dbuser_in_games
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

