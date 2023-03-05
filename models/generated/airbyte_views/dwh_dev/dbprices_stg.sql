{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbprices_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'name',
        'amount',
        'currency',
        'created_at',
        'updated_at',
        'max_players',
        'payment_type',
    ]) }} as _airbyte_dbprices_hashid,
    tmp.*
from {{ ref('dbprices_ab2') }} tmp
-- dbprices
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

