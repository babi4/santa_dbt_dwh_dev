{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbusers_ab1') }}
select
    nullif(accurateCastOrNull(trim(BOTH '"' from id), '{{ dbt_utils.type_string() }}'), 'null') as id,
    nullif(accurateCastOrNull(trim(BOTH '"' from email), '{{ dbt_utils.type_string() }}'), 'null') as email,
    accurateCastOrNull(gender, '{{ dbt_utils.type_bigint() }}') as gender,
    accurateCastOrNull(locale, '{{ dbt_utils.type_bigint() }}') as locale,
    nullif(accurateCastOrNull(trim(BOTH '"' from ref_id), '{{ dbt_utils.type_string() }}'), 'null') as ref_id,
    accurateCastOrNull(status, '{{ dbt_utils.type_bigint() }}') as status,
    nullif(accurateCastOrNull(trim(BOTH '"' from address), '{{ dbt_utils.type_string() }}'), 'null') as address,
    toDate(parseDateTimeBestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('birthday') }}))) as birthday,
    nullif(accurateCastOrNull(trim(BOTH '"' from last_name), '{{ dbt_utils.type_string() }}'), 'null') as last_name,
    nullif(accurateCastOrNull(trim(BOTH '"' from telephone), '{{ dbt_utils.type_string() }}'), 'null') as telephone,
    {{ cast_to_boolean('validated') }} as validated,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('created_at') }})) as created_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from first_name), '{{ dbt_utils.type_string() }}'), 'null') as first_name,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('updated_at') }})) as updated_at,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('confirmed_at') }})) as confirmed_at,
    {{ cast_to_boolean('password_set') }} as password_set,
    {{ cast_to_boolean('info_was_given') }} as info_was_given,
    accurateCastOrNull(registred_from, '{{ dbt_utils.type_bigint() }}') as registred_from,
    {{ cast_to_boolean('email_delivered') }} as email_delivered,
    nullif(accurateCastOrNull(trim(BOTH '"' from unconfirmed_email), '{{ dbt_utils.type_string() }}'), 'null') as unconfirmed_email,
    {{ cast_to_boolean('chat_notifications') }} as chat_notifications,
    nullif(accurateCastOrNull(trim(BOTH '"' from confirmation_token), '{{ dbt_utils.type_string() }}'), 'null') as confirmation_token,
    nullif(accurateCastOrNull(trim(BOTH '"' from encrypted_password), '{{ dbt_utils.type_string() }}'), 'null') as encrypted_password,
    {{ cast_to_boolean('wishlist_available') }} as wishlist_available,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('remember_created_at') }})) as remember_created_at,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('confirmation_sent_at') }})) as confirmation_sent_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from reset_password_token), '{{ dbt_utils.type_string() }}'), 'null') as reset_password_token,
    parseDateTime64BestEffortOrNull(trim(BOTH '"' from {{ empty_string_to_null('reset_password_sent_at') }})) as reset_password_sent_at,
    nullif(accurateCastOrNull(trim(BOTH '"' from mobile_authentication_token), '{{ dbt_utils.type_string() }}'), 'null') as mobile_authentication_token,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbusers_ab1') }}
-- dbusers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

