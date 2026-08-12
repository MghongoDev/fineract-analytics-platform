{{ config(materialized='table', engine='MergeTree()', order_by='(office_id)') }}

/*
    Staging: office (branch) hierarchy.

    `hierarchy` is Fineract's materialised path ('.1.4.'), which is what
    makes roll-ups possible without a recursive CTE - the depth is just
    the number of separators.
*/

with source as (

    select *
    from {{ source('fineract_raw', 'offices') }} final
    where _is_deleted = 0

)

select
    office_id,
    {{ surrogate_key(['office_id']) }}                              as office_key,
    name                                                            as office_name,
    name_decorated                                                  as office_name_decorated,
    external_id,
    parent_id                                                       as parent_office_id,
    parent_name                                                     as parent_office_name,
    hierarchy,
    -- '.'=root(0), '.1.'=1, '.1.4.'=2 ...
    greatest(length(ifNull(hierarchy, '.')) - length(replaceAll(ifNull(hierarchy, '.'), '.', '')) - 1, 0)
                                                                    as hierarchy_depth,
    if(parent_id is null, 1, 0)                                     as is_head_office,
    opening_date,
    if(opening_date is null, null, dateDiff('day', opening_date, today())) as days_open,

    source_ingested_at,
    source_updated_at,
    _source_commit_at                                               as cdc_commit_at,
    _version                                                        as cdc_version

from source
