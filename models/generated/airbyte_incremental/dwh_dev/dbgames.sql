{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbgames_scd') }}
select
    _airbyte_unique_key,
    id,
    name,
    state,
    message,
    currency,
    game_type,
    created_at,
    payment_id,
    updated_at,
    join_before,
    max_players,
    paid_status,
    address_hint,
    created_from,
    present_price,
    toss_datetime,
    messenger_code,
    real_toss_time,
    text_wish_message,
    presents_send_date,
    wishlist_available,
    text_unwish_message,
    where_to_get_presents,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbgames_hashid
from {{ ref('dbgames_scd') }}
-- dbgames from {{ source('dwh_dev', '_airbyte_raw_dbgames') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

