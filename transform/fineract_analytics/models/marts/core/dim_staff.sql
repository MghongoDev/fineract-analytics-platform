{{ config(materialized='table', engine='MergeTree()', order_by='(staff_id)') }}

/*
    Loan officer dimension with book quality attached. Portfolio quality
    per officer is a management metric, so it belongs in the dimension
    rather than being recomputed in each report.
*/

with staff as (

    select * from {{ ref('stg_fineract__staff') }}

),

book as (

    select
        loan_officer_id                                 as staff_id,
        count()                                         as total_loans,
        countIf(is_active = 1)                          as active_loans,
        countDistinct(client_id)                        as distinct_borrowers,
        sumIf(total_outstanding, is_active = 1)         as outstanding_portfolio,
        sumIf(total_overdue, is_active = 1)             as overdue_portfolio,
        countIf(days_past_due > 0 and is_active = 1)    as delinquent_loans,
        countIf(is_written_off = 1)                     as written_off_loans,
        avg(toFloat64(days_past_due))                   as avg_days_past_due
    from {{ ref('stg_fineract__loans') }}
    where loan_officer_id is not null
    group by loan_officer_id

)

select
    s.staff_key                                                 as staff_key,
    s.staff_id                                                  as staff_id,
    s.staff_name                                                as staff_name,
    s.firstname                                                 as firstname,
    s.lastname                                                  as lastname,
    s.office_id                                                 as office_id,
    s.office_name                                               as office_name,
    s.is_loan_officer                                           as is_loan_officer,
    s.is_active                                                 as is_active,
    s.joining_date                                              as joining_date,
    s.tenure_days                                               as tenure_days,

    coalesce(b.total_loans, 0)                          as total_loans,
    coalesce(b.active_loans, 0)                         as active_loans,
    coalesce(b.distinct_borrowers, 0)                   as distinct_borrowers,
    coalesce(b.outstanding_portfolio, 0)                as outstanding_portfolio,
    coalesce(b.overdue_portfolio, 0)                    as overdue_portfolio,
    coalesce(b.delinquent_loans, 0)                     as delinquent_loans,
    coalesce(b.written_off_loans, 0)                    as written_off_loans,
    b.avg_days_past_due                                         as avg_days_past_due,

    {{ safe_divide('b.overdue_portfolio', 'nullIf(b.outstanding_portfolio, 0)') }} as par_ratio,
    {{ safe_divide('b.delinquent_loans', 'nullIf(b.active_loans, 0)') }}           as delinquency_rate,

    now64(3)                                            as dbt_updated_at

from staff s
left join book b on s.staff_id = b.staff_id
