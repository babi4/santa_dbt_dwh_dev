{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "dwh_dev",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='dbgames_users_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbgames_users_ab3') }}
select
    game_id,
    user_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbgames_users_hashid
from {{ ref('dbgames_users_ab3') }}
-- dbgames_users from {{ source('dwh_dev', '_airbyte_raw_dbgames_users') }}
where 1 = 1

