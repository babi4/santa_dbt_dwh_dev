{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbgames_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'name',
        'state',
        'message',
        'currency',
        'game_type',
        'created_at',
        'payment_id',
        'updated_at',
        'join_before',
        'max_players',
        'paid_status',
        'address_hint',
        'created_from',
        'present_price',
        'toss_datetime',
        'messenger_code',
        'real_toss_time',
        'text_wish_message',
        'presents_send_date',
        boolean_to_string('wishlist_available'),
        'text_unwish_message',
        'where_to_get_presents',
    ]) }} as _airbyte_dbgames_hashid,
    tmp.*
from {{ ref('dbgames_ab2') }} tmp
-- dbgames
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

