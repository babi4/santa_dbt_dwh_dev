{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_dwh_dev",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('dwh_dev', '_airbyte_raw_dbtoss_groups') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as name,
    {{ json_extract_scalar('_airbyte_data', ['default'], ['default']) }} as default,
    {{ json_extract_scalar('_airbyte_data', ['game_id'], ['game_id']) }} as game_id,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['involvment_type'], ['involvment_type']) }} as involvment_type,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('dwh_dev', '_airbyte_raw_dbtoss_groups') }} as table_alias
-- dbtoss_groups
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

