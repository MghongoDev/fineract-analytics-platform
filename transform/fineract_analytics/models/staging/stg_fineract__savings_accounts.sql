{{ config(materialized='table', engine='MergeTree()', order_by='(savings_id)') }}

with source as (

    select *
    from {{ source('fineract_raw', 'savings_accounts') }} final
    where _is_deleted = 0

)

select
    savings_id,
    {{ surrogate_key(['savings_id']) }}                             as savings_key,
    account_no,
    client_id,
    client_name,
    product_id,
    product_name,
    office_id,
    field_officer_id,
    status_id,
    status_value                                                    as status,
    coalesce(is_active, 0)                                          as is_active,
    currency_code,
    nominal_annual_interest_rate,

    submitted_on_date,
    activated_on_date,
    closed_on_date,

    {{ money('account_balance') }}                                  as account_balance,
    {{ money('available_balance') }}                                as available_balance,
    {{ money('total_deposits') }}                                   as total_deposits,
    {{ money('total_withdrawals') }}                                as total_withdrawals,
    {{ money('total_interest_posted') }}                            as total_interest_posted,

    {{ safe_divide('total_withdrawals', 'nullIf(total_deposits, 0)') }} as withdrawal_ratio,
    if(activated_on_date is null, null,
       dateDiff('day', activated_on_date, today()))                 as days_since_activation,

    source_ingested_at,
    source_updated_at,
    _source_commit_at                                               as cdc_commit_at,
    _version                                                        as cdc_version

from source
