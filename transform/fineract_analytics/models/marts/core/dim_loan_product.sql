{{ config(materialized='table', engine='MergeTree()', order_by='(product_id)') }}

/*
    Loan product dimension with realised performance attached - what the
    product was designed to do (terms) next to what it actually did
    (uptake, PAR, write-off rate). Product managers ask both questions in
    the same breath.
*/

with products as (

    select * from {{ ref('stg_fineract__loan_products') }}

),

performance as (

    select
        product_id,
        count()                                         as total_loans,
        countIf(is_active = 1)                          as active_loans,
        countIf(is_closed = 1)                          as closed_loans,
        countIf(is_written_off = 1)                     as written_off_loans,
        countIf(days_past_due > 0)                      as delinquent_loans,
        countDistinct(client_id)                        as distinct_borrowers,
        sum(principal_disbursed)                        as lifetime_disbursed,
        sumIf(total_outstanding, is_active = 1)         as current_outstanding,
        sumIf(total_overdue, is_active = 1)             as current_overdue,
        sum(principal_written_off)                      as lifetime_written_off,
        avg(toFloat64(principal))                       as avg_principal,
        median(toFloat64(principal))                    as median_principal,
        avg(toFloat64(days_past_due))                   as avg_days_past_due
    from {{ ref('stg_fineract__loans') }}
    group by product_id

)

select
    p.product_key                                               as product_key,
    p.product_id                                                as product_id,
    p.product_name                                              as product_name,
    p.product_short_name                                        as product_short_name,
    p.product_description                                       as product_description,
    p.fund_name                                                 as fund_name,
    p.currency_code                                             as currency_code,

    -- designed terms
    p.default_principal                                         as default_principal,
    p.min_principal                                             as min_principal,
    p.max_principal                                             as max_principal,
    p.default_number_of_repayments                              as default_number_of_repayments,
    p.repayment_frequency_type                                  as repayment_frequency_type,
    p.interest_rate_per_period                                  as interest_rate_per_period,
    p.annual_interest_rate                                      as annual_interest_rate,
    p.amortization_type                                         as amortization_type,
    p.interest_type                                             as interest_type,
    p.product_status                                            as product_status,
    p.is_active                                         as is_product_active,
    p.start_date                                                as start_date,
    p.close_date                                                as close_date,

    -- realised performance
    coalesce(f.total_loans, 0)                          as total_loans,
    coalesce(f.active_loans, 0)                         as active_loans,
    coalesce(f.closed_loans, 0)                         as closed_loans,
    coalesce(f.written_off_loans, 0)                    as written_off_loans,
    coalesce(f.delinquent_loans, 0)                     as delinquent_loans,
    coalesce(f.distinct_borrowers, 0)                   as distinct_borrowers,
    coalesce(f.lifetime_disbursed, 0)                   as lifetime_disbursed,
    coalesce(f.current_outstanding, 0)                  as current_outstanding,
    coalesce(f.current_overdue, 0)                      as current_overdue,
    coalesce(f.lifetime_written_off, 0)                 as lifetime_written_off,
    f.avg_principal                                             as avg_principal,
    f.median_principal                                          as median_principal,
    f.avg_days_past_due                                         as avg_days_past_due,

    {{ safe_divide('f.current_overdue', 'nullIf(f.current_outstanding, 0)') }} as par_ratio,
    {{ safe_divide('f.written_off_loans', 'nullIf(f.total_loans, 0)') }}       as write_off_rate,
    {{ safe_divide('f.delinquent_loans', 'nullIf(f.active_loans, 0)') }}       as delinquency_rate,

    now64(3)                                            as dbt_updated_at

from products p
left join performance f on p.product_id = f.product_id
