{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbuser_in_games_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'lat',
        'city',
        'flat',
        'long',
        'floor',
        'region',
        'status',
        'street',
        'address',
        'country',
        'game_id',
        'user_id',
        'entrance',
        'zip_code',
        'telephone',
        'created_at',
        'street_num',
        'updated_at',
        'joined_from',
        'wish_message',
        'entrance_code',
        'toss_group_id',
        'tracking_code',
        boolean_to_string('present_bought'),
        boolean_to_string('present_sended'),
        'unwish_message',
        'send_present_to',
        boolean_to_string('banned_from_chat'),
        'delivery_comemnt',
        boolean_to_string('present_received'),
        'sended_present_url',
        'tracking_description',
        boolean_to_string('congratulator_seen_congratulated'),
        boolean_to_string('sended_present_notification_was_sent'),
        boolean_to_string('received_present_notification_was_sent'),
    ]) }} as _airbyte_dbuser_in_games_hashid,
    tmp.*
from {{ ref('dbuser_in_games_ab2') }} tmp
-- dbuser_in_games
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

