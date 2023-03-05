{{ config(
    unique_key = "_airbyte_unique_key",
    schema = "dwh_dev",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbusers_scd') }}
select
    _airbyte_unique_key,
    id,
    email,
    gender,
    locale,
    ref_id,
    status,
    address,
    birthday,
    last_name,
    telephone,
    validated,
    created_at,
    first_name,
    updated_at,
    confirmed_at,
    password_set,
    info_was_given,
    registred_from,
    email_delivered,
    unconfirmed_email,
    chat_notifications,
    confirmation_token,
    encrypted_password,
    wishlist_available,
    remember_created_at,
    confirmation_sent_at,
    reset_password_token,
    reset_password_sent_at,
    mobile_authentication_token,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbusers_hashid
from {{ ref('dbusers_scd') }}
-- dbusers from {{ source('dwh_dev', '_airbyte_raw_dbusers') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

