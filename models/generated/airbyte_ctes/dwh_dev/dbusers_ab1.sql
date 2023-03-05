{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('dwh_dev', '_airbyte_raw_dbusers') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as email,
    {{ json_extract_scalar('_airbyte_data', ['gender'], ['gender']) }} as gender,
    {{ json_extract_scalar('_airbyte_data', ['locale'], ['locale']) }} as locale,
    {{ json_extract_scalar('_airbyte_data', ['ref_id'], ['ref_id']) }} as ref_id,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['address'], ['address']) }} as address,
    {{ json_extract_scalar('_airbyte_data', ['birthday'], ['birthday']) }} as birthday,
    {{ json_extract_scalar('_airbyte_data', ['last_name'], ['last_name']) }} as last_name,
    {{ json_extract_scalar('_airbyte_data', ['telephone'], ['telephone']) }} as telephone,
    {{ json_extract_scalar('_airbyte_data', ['validated'], ['validated']) }} as validated,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['first_name'], ['first_name']) }} as first_name,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['confirmed_at'], ['confirmed_at']) }} as confirmed_at,
    {{ json_extract_scalar('_airbyte_data', ['password_set'], ['password_set']) }} as password_set,
    {{ json_extract_scalar('_airbyte_data', ['info_was_given'], ['info_was_given']) }} as info_was_given,
    {{ json_extract_scalar('_airbyte_data', ['registred_from'], ['registred_from']) }} as registred_from,
    {{ json_extract_scalar('_airbyte_data', ['email_delivered'], ['email_delivered']) }} as email_delivered,
    {{ json_extract_scalar('_airbyte_data', ['unconfirmed_email'], ['unconfirmed_email']) }} as unconfirmed_email,
    {{ json_extract_scalar('_airbyte_data', ['chat_notifications'], ['chat_notifications']) }} as chat_notifications,
    {{ json_extract_scalar('_airbyte_data', ['confirmation_token'], ['confirmation_token']) }} as confirmation_token,
    {{ json_extract_scalar('_airbyte_data', ['encrypted_password'], ['encrypted_password']) }} as encrypted_password,
    {{ json_extract_scalar('_airbyte_data', ['wishlist_available'], ['wishlist_available']) }} as wishlist_available,
    {{ json_extract_scalar('_airbyte_data', ['remember_created_at'], ['remember_created_at']) }} as remember_created_at,
    {{ json_extract_scalar('_airbyte_data', ['confirmation_sent_at'], ['confirmation_sent_at']) }} as confirmation_sent_at,
    {{ json_extract_scalar('_airbyte_data', ['reset_password_token'], ['reset_password_token']) }} as reset_password_token,
    {{ json_extract_scalar('_airbyte_data', ['reset_password_sent_at'], ['reset_password_sent_at']) }} as reset_password_sent_at,
    {{ json_extract_scalar('_airbyte_data', ['mobile_authentication_token'], ['mobile_authentication_token']) }} as mobile_authentication_token,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('dwh_dev', '_airbyte_raw_dbusers') }} as table_alias
-- dbusers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

