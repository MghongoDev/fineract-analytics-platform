{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(client_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    ============================================================
    Inference dataset: current client features for scoring.
    ============================================================

    Grain: one row per client, features as of `scored_at`.

    This is the *serving* counterpart to ml_loan_default_features. It has
    no label - it exists to be joined to a model's prediction for a
    client who is applying today, and to power the "who should the
    collections team call this week" list.

    Because it is current-state by design, it may legitimately contain
    features that would be leakage in the training table (current
    delinquency, current balances). Keeping the two tables separate, with
    different rules, is what stops that distinction from being lost:
    training reads `ml_loan_default_features`, serving reads this one,
    and the feature names do not overlap by accident - the shared ones
    are named identically on purpose so a model can be applied to both.
*/

with clients as (

    select * from {{ ref('stg_fineract__clients') }}

),

portfolio as (

    select * from {{ ref('int_client_portfolio') }}

),

recent_activity as (

    select
        l.client_id                                                 as client_id,
        countIf(t.transaction_date >= today() - 30)                 as repayments_last_30d,
        countIf(t.transaction_date >= today() - 90)                 as repayments_last_90d,
        sumIf(t.amount, t.transaction_date >= today() - 30)         as repaid_last_30d,
        sumIf(t.amount, t.transaction_date >= today() - 90)         as repaid_last_90d,
        max(t.transaction_date)                                     as last_repayment_date
    from {{ ref('stg_fineract__loan_transactions') }} t
    inner join {{ ref('stg_fineract__loans') }} l on t.loan_id = l.loan_id
    where t.transaction_category = 'repayment' and t.is_countable = 1
    group by l.client_id

)

select
    -- identifiers
    c.client_id                                                     as client_id,
    now64(3)                                                        as scored_at,
    today()                                                         as scoring_date,

    -- demographics (same names as the training table on purpose)
    c.gender                                                        as feat_gender,
    c.legal_form                                                    as feat_legal_form,
    c.client_type                                                   as feat_client_type,
    c.client_classification                                         as feat_client_classification,
    c.age_years                                                     as feat_age_at_origination,
    c.tenure_days                                                   as feat_client_tenure_days,
    c.office_id                                                     as feat_office_id,
    c.staff_id                                                      as feat_loan_officer_id,

    -- relationship history
    coalesce(p.total_loans, 0)                                      as feat_prior_loan_count,
    coalesce(p.written_off_loans, 0)                                as feat_prior_written_off_count,
    coalesce(p.delinquent_loans, 0)                                 as feat_prior_delinquent_count,
    toFloat64(coalesce(p.lifetime_disbursed, 0))                    as feat_prior_total_disbursed,
    toFloat64(coalesce(p.max_loan_principal, 0))                    as feat_prior_max_principal,
    coalesce(p.distinct_products_used, 0)                           as feat_prior_distinct_products,
    p.historical_write_off_rate                                     as feat_prior_write_off_rate,
    p.lifetime_repayment_ratio                                      as feat_lifetime_repayment_ratio,
    if(coalesce(p.total_loans, 0) = 0, 1, 0)                        as feat_is_first_time_borrower,

    -- current exposure (serving-only: leakage if used for training)
    coalesce(p.active_loans, 0)                                     as feat_active_loans,
    toFloat64(coalesce(p.current_total_outstanding, 0))             as feat_current_outstanding,
    toFloat64(coalesce(p.current_total_overdue, 0))                 as feat_current_overdue,
    coalesce(p.current_worst_days_past_due, 0)                      as feat_current_days_past_due,
    {{ par_bucket('p.current_worst_days_past_due') }}               as feat_current_par_bucket,
    toFloat64(coalesce(p.total_savings_balance, 0))                 as feat_savings_balance,
    p.savings_to_debt_ratio                                         as feat_savings_to_debt_ratio,

    -- recent behaviour
    coalesce(r.repayments_last_30d, 0)                              as feat_repayments_last_30d,
    coalesce(r.repayments_last_90d, 0)                              as feat_repayments_last_90d,
    toFloat64(coalesce(r.repaid_last_30d, 0))                       as feat_repaid_last_30d,
    toFloat64(coalesce(r.repaid_last_90d, 0))                       as feat_repaid_last_90d,
    if(r.last_repayment_date is null, null,
       dateDiff('day', r.last_repayment_date, today()))             as feat_days_since_last_repayment,

    -- Eligibility flags the collections and credit teams filter on.
    if(coalesce(p.active_loans, 0) > 0, 1, 0)                       as is_currently_borrowing,
    if(coalesce(p.current_worst_days_past_due, 0) > 0, 1, 0)        as needs_collections_review,

    now64(3)                                                        as dbt_updated_at

from clients c
left join portfolio p on c.client_id = p.client_id
left join recent_activity r on c.client_id = r.client_id
