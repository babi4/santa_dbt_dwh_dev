{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('dwh_dev', '_airbyte_raw_dbgames') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as name,
    {{ json_extract_scalar('_airbyte_data', ['state'], ['state']) }} as state,
    {{ json_extract_scalar('_airbyte_data', ['message'], ['message']) }} as message,
    {{ json_extract_scalar('_airbyte_data', ['currency'], ['currency']) }} as currency,
    {{ json_extract_scalar('_airbyte_data', ['game_type'], ['game_type']) }} as game_type,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['payment_id'], ['payment_id']) }} as payment_id,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['join_before'], ['join_before']) }} as join_before,
    {{ json_extract_scalar('_airbyte_data', ['max_players'], ['max_players']) }} as max_players,
    {{ json_extract_scalar('_airbyte_data', ['paid_status'], ['paid_status']) }} as paid_status,
    {{ json_extract_scalar('_airbyte_data', ['address_hint'], ['address_hint']) }} as address_hint,
    {{ json_extract_scalar('_airbyte_data', ['created_from'], ['created_from']) }} as created_from,
    {{ json_extract_scalar('_airbyte_data', ['present_price'], ['present_price']) }} as present_price,
    {{ json_extract_scalar('_airbyte_data', ['toss_datetime'], ['toss_datetime']) }} as toss_datetime,
    {{ json_extract_scalar('_airbyte_data', ['messenger_code'], ['messenger_code']) }} as messenger_code,
    {{ json_extract_scalar('_airbyte_data', ['real_toss_time'], ['real_toss_time']) }} as real_toss_time,
    {{ json_extract_scalar('_airbyte_data', ['text_wish_message'], ['text_wish_message']) }} as text_wish_message,
    {{ json_extract_scalar('_airbyte_data', ['presents_send_date'], ['presents_send_date']) }} as presents_send_date,
    {{ json_extract_scalar('_airbyte_data', ['wishlist_available'], ['wishlist_available']) }} as wishlist_available,
    {{ json_extract_scalar('_airbyte_data', ['text_unwish_message'], ['text_unwish_message']) }} as text_unwish_message,
    {{ json_extract_scalar('_airbyte_data', ['where_to_get_presents'], ['where_to_get_presents']) }} as where_to_get_presents,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('dwh_dev', '_airbyte_raw_dbgames') }} as table_alias
-- dbgames
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

