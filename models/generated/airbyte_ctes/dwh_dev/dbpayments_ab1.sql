{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('dwh_dev', '_airbyte_raw_dbpayments') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['state'], ['state']) }} as state,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    {{ json_extract_scalar('_airbyte_data', ['price_id'], ['price_id']) }} as price_id,
    {{ json_extract_scalar('_airbyte_data', ['card_type'], ['card_type']) }} as card_type,
    {{ json_extract_scalar('_airbyte_data', ['test_mode'], ['test_mode']) }} as test_mode,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['description'], ['description']) }} as description,
    {{ json_extract_scalar('_airbyte_data', ['cardfirstsix'], ['cardfirstsix']) }} as cardfirstsix,
    {{ json_extract_scalar('_airbyte_data', ['cardlastfour'], ['cardlastfour']) }} as cardlastfour,
    {{ json_extract_scalar('_airbyte_data', ['extra_data_str'], ['extra_data_str']) }} as extra_data_str,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('dwh_dev', '_airbyte_raw_dbpayments') }} as table_alias
-- dbpayments
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

