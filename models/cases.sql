with __dbt__CTE__demo_dataset_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    json_extract_scalar(_airbyte_data, "$['key']") as key,
    json_extract_scalar(_airbyte_data, "$['date']") as date,
    json_extract_scalar(_airbyte_data, "$['new_tested']") as new_tested,
    json_extract_scalar(_airbyte_data, "$['new_deceased']") as new_deceased,
    json_extract_scalar(_airbyte_data, "$['total_tested']") as total_tested,
    json_extract_scalar(_airbyte_data, "$['new_confirmed']") as new_confirmed,
    json_extract_scalar(_airbyte_data, "$['new_recovered']") as new_recovered,
    json_extract_scalar(_airbyte_data, "$['total_deceased']") as total_deceased,
    json_extract_scalar(_airbyte_data, "$['total_confirmed']") as total_confirmed,
    json_extract_scalar(_airbyte_data, "$['total_recovered']") as total_recovered,
    _airbyte_emitted_at
from `daring-phoenix-365309`.toyset._airbyte_raw_covid_data as table_alias
-- demo_dataset
),  __dbt__CTE__demo_dataset_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast(key as 
    string
) as key,
    cast(date as 
    string
) as date,
    cast(new_tested as 
    float64
) as new_tested,
    cast(new_deceased as 
    float64
) as new_deceased,
    cast(total_tested as 
    float64
) as total_tested,
    cast(new_confirmed as 
    float64
) as new_confirmed,
    cast(new_recovered as 
    float64
) as new_recovered,
    cast(total_deceased as 
    float64
) as total_deceased,
    cast(total_confirmed as 
    float64
) as total_confirmed,
    cast(total_recovered as 
    float64
) as total_recovered,
    _airbyte_emitted_at
from __dbt__CTE__demo_dataset_ab1
-- demo_dataset
),  __dbt__CTE__demo_dataset_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    *,
    to_hex(md5(cast(concat(coalesce(cast(key as 
    string
), ''), '-', coalesce(cast(date as 
    string
), ''), '-', coalesce(cast(new_tested as 
    string
), ''), '-', coalesce(cast(new_deceased as 
    string
), ''), '-', coalesce(cast(total_tested as 
    string
), ''), '-', coalesce(cast(new_confirmed as 
    string
), ''), '-', coalesce(cast(new_recovered as 
    string
), ''), '-', coalesce(cast(total_deceased as 
    string
), ''), '-', coalesce(cast(total_confirmed as 
    string
), ''), '-', coalesce(cast(total_recovered as 
    string
), '')) as 
    string
))) as _airbyte_demo_dataset_hashid
from __dbt__CTE__demo_dataset_ab2
-- demo_dataset
)-- Final base SQL model
select
    key,
    date,
    new_tested,
    new_deceased,
    total_tested,
    new_confirmed,
    new_recovered,
    total_deceased,
    total_confirmed,
    total_recovered,
    _airbyte_emitted_at,
    _airbyte_demo_dataset_hashid
from __dbt__CTE__demo_dataset_ab3
-- demo_dataset from `daring-phoenix-365309`.toyset._airbyte_raw_demo_dataset
