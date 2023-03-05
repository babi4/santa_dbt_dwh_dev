{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbgames_users_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'game_id',
        'user_id',
    ]) }} as _airbyte_dbgames_users_hashid,
    tmp.*
from {{ ref('dbgames_users_ab2') }} tmp
-- dbgames_users
where 1 = 1

