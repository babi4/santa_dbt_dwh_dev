{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbusers_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'email',
        'gender',
        'locale',
        'ref_id',
        'status',
        'address',
        'birthday',
        'last_name',
        'telephone',
        boolean_to_string('validated'),
        'created_at',
        'first_name',
        'updated_at',
        'confirmed_at',
        boolean_to_string('password_set'),
        boolean_to_string('info_was_given'),
        'registred_from',
        boolean_to_string('email_delivered'),
        'unconfirmed_email',
        boolean_to_string('chat_notifications'),
        'confirmation_token',
        'encrypted_password',
        boolean_to_string('wishlist_available'),
        'remember_created_at',
        'confirmation_sent_at',
        'reset_password_token',
        'reset_password_sent_at',
        'mobile_authentication_token',
    ]) }} as _airbyte_dbusers_hashid,
    tmp.*
from {{ ref('dbusers_ab2') }} tmp
-- dbusers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

