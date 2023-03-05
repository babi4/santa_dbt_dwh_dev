{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbuser_in_games_scd') }}
select
    _airbyte_unique_key,
    id,
    lat,
    city,
    flat,
    long,
    floor,
    region,
    status,
    street,
    address,
    country,
    game_id,
    user_id,
    entrance,
    zip_code,
    telephone,
    created_at,
    street_num,
    updated_at,
    joined_from,
    wish_message,
    entrance_code,
    toss_group_id,
    tracking_code,
    present_bought,
    present_sended,
    unwish_message,
    send_present_to,
    banned_from_chat,
    delivery_comemnt,
    present_received,
    sended_present_url,
    tracking_description,
    congratulator_seen_congratulated,
    sended_present_notification_was_sent,
    received_present_notification_was_sent,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbuser_in_games_hashid
from {{ ref('dbuser_in_games_scd') }}
-- dbuser_in_games from {{ source('dwh_dev', '_airbyte_raw_dbuser_in_games') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

