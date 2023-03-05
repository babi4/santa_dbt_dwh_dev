{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbpayments_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'state',
        'status',
        'user_id',
        'price_id',
        'card_type',
        boolean_to_string('test_mode'),
        'created_at',
        'updated_at',
        'description',
        'cardfirstsix',
        'cardlastfour',
        'extra_data_str',
    ]) }} as _airbyte_dbpayments_hashid,
    tmp.*
from {{ ref('dbpayments_ab2') }} tmp
-- dbpayments
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

