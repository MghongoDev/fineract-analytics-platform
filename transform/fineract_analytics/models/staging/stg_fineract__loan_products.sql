{{ config(materialized='table', engine='MergeTree()', order_by='(product_id)') }}

with source as (

    select *
    from {{ source('fineract_raw', 'loan_products') }} final
    where _is_deleted = 0

)

select
    product_id,
    {{ surrogate_key(['product_id']) }}                             as product_key,
    name                                                            as product_name,
    short_name                                                      as product_short_name,
    description                                                     as product_description,
    fund_name,
    currency_code,
    currency_decimal_places,

    {{ money('principal') }}                                        as default_principal,
    {{ money('min_principal') }}                                    as min_principal,
    {{ money('max_principal') }}                                    as max_principal,
    number_of_repayments                                            as default_number_of_repayments,
    repayment_every,
    repayment_frequency_type,
    interest_rate_per_period,
    interest_rate_frequency_type,
    annual_interest_rate,
    amortization_type,
    interest_type,
    interest_calculation_period_type,

    status                                                          as product_status,
    if(status = 'loanProduct.active', 1, 0)                         as is_active,
    start_date,
    close_date,

    source_ingested_at,
    source_updated_at,
    _source_commit_at                                               as cdc_commit_at,
    _version                                                        as cdc_version

from source
