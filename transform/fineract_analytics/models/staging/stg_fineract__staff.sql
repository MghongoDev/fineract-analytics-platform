{{ config(materialized='table', engine='MergeTree()', order_by='(staff_id)') }}

with source as (

    select *
    from {{ source('fineract_raw', 'staff') }} final
    where _is_deleted = 0

)

select
    staff_id,
    {{ surrogate_key(['staff_id']) }}                               as staff_key,
    display_name                                                    as staff_name,
    firstname,
    lastname,
    office_id,
    office_name,
    mobile_no,
    coalesce(is_loan_officer, 0)                                    as is_loan_officer,
    coalesce(is_active, 0)                                          as is_active,
    joining_date,
    if(joining_date is null, null, dateDiff('day', joining_date, today())) as tenure_days,

    source_ingested_at,
    source_updated_at,
    _source_commit_at                                               as cdc_commit_at,
    _version                                                        as cdc_version

from source
