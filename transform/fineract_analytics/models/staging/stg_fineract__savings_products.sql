{{ config(materialized='table', engine='MergeTree()', order_by='(product_id)') }}

with source as (

    select *
    from {{ source('fineract_raw', 'savings_products') }} final
    where _is_deleted = 0

)

select
    product_id,
    {{ surrogate_key(['product_id']) }}                             as product_key,
    name                                                            as product_name,
    short_name                                                      as product_short_name,
    description                                                     as product_description,
    currency_code,
    currency_decimal_places,
    nominal_annual_interest_rate,
    interest_compounding_period_type,
    interest_posting_period_type,
    {{ money('min_required_opening_balance') }}                     as min_required_opening_balance,
    status                                                          as product_status,

    source_ingested_at,
    source_updated_at,
    _source_commit_at                                               as cdc_commit_at,
    _version                                                        as cdc_version

from source
