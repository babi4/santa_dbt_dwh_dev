{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbtoss_groups_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'name',
        boolean_to_string('default'),
        'game_id',
        'created_at',
        'updated_at',
        'involvment_type',
    ]) }} as _airbyte_dbtoss_groups_hashid,
    tmp.*
from {{ ref('dbtoss_groups_ab2') }} tmp
-- dbtoss_groups
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

