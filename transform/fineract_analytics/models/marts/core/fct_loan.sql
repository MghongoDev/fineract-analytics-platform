{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMM(disbursed_on_date)',
        order_by='(disbursed_on_date, office_id, product_id, loan_id)',
        settings={'index_granularity': 8192, 'allow_nullable_key': 1}
    )
}}

/*
    Loan fact - one row per loan account, current state.

    ORDER BY is (disbursed_on_date, office_id, product_id, loan_id)
    because that is the actual filter order of the dashboards this table
    exists for: a period, then a branch, then a product. ClickHouse's
    primary index is the sort key, so getting this order wrong is the
    difference between a granule scan and a full scan.

    PARTITION BY month of disbursement gives partition pruning for the
    "originations in Q2" family of questions, and keeps parts at a size
    where merges stay cheap. A loan's disbursement date does not change
    once set, so no row ever needs to move partitions.

    Loans that were never disbursed have a NULL disbursement date, which
    ClickHouse places in its own partition - deliberate: the pipeline
    (submitted / approved / rejected) is a separate reporting question
    from the book.
*/

with loans as (

    select * from {{ ref('stg_fineract__loans') }}

),

behaviour as (

    select * from {{ ref('int_loan_repayment_behaviour') }}

),

clients as (

    select client_id, client_segment, client_classification, gender,
           legal_form, age_band, tenure_band
    from {{ ref('dim_client') }}

)

select
    -- keys
    l.loan_key                                                  as loan_key,
    l.loan_id                                                   as loan_id,
    l.account_no                                                as account_no,
    l.client_id                                                 as client_id,
    l.product_id                                                as product_id,
    l.office_id                                                 as office_id,
    l.loan_officer_id                                           as loan_officer_id,
    toYYYYMMDD(l.disbursed_on_date)                     as disbursement_date_key,

    -- descriptive
    l.status                                                    as status,
    l.status_id                                                 as status_id,
    l.loan_type                                                 as loan_type,
    l.currency_code                                             as currency_code,
    l.is_active                                                 as is_active,
    l.is_closed                                                 as is_closed,
    l.is_overpaid                                               as is_overpaid,
    l.is_written_off                                            as is_written_off,

    -- client context (degenerate attributes kept on the fact for the
    -- common "slice the book by borrower segment" query, which would
    -- otherwise need a join on every single dashboard panel)
    c.client_segment                                            as client_segment,
    c.client_classification                                     as client_classification,
    c.gender                                                    as gender,
    c.legal_form                                                as legal_form,
    c.age_band                                                  as age_band,
    c.tenure_band                                               as tenure_band,

    -- lifecycle
    l.submitted_on_date                                         as submitted_on_date,
    l.approved_on_date                                          as approved_on_date,
    l.disbursed_on_date                                         as disbursed_on_date,
    l.expected_maturity_date                                    as expected_maturity_date,
    l.closed_on_date                                            as closed_on_date,
    l.overdue_since_date                                        as overdue_since_date,
    l.days_since_disbursement                                   as days_since_disbursement,
    l.term_days                                                 as term_days,
    if(l.submitted_on_date is null or l.approved_on_date is null, null,
       dateDiff('day', l.submitted_on_date, l.approved_on_date))    as days_to_approval,
    if(l.approved_on_date is null or l.disbursed_on_date is null, null,
       dateDiff('day', l.approved_on_date, l.disbursed_on_date))    as days_to_disbursement,

    -- terms
    l.number_of_repayments                                      as number_of_repayments,
    l.repayment_frequency_type                                  as repayment_frequency_type,
    l.interest_rate_per_period                                  as interest_rate_per_period,
    l.annual_interest_rate                                      as annual_interest_rate,

    -- measures: principal
    l.principal                                                 as principal,
    l.approved_principal                                        as approved_principal,
    l.principal_disbursed                                       as principal_disbursed,
    l.principal_paid                                            as principal_paid,
    l.principal_outstanding                                     as principal_outstanding,
    l.principal_overdue                                         as principal_overdue,
    l.principal_written_off                                     as principal_written_off,

    -- measures: interest and charges
    l.interest_charged                                          as interest_charged,
    l.interest_paid                                             as interest_paid,
    l.interest_outstanding                                      as interest_outstanding,
    l.fee_charges_charged                                       as fee_charges_charged,
    l.fee_charges_paid                                          as fee_charges_paid,
    l.penalty_charges_charged                                   as penalty_charges_charged,

    -- measures: totals
    l.total_expected_repayment                                  as total_expected_repayment,
    l.total_repayment                                           as total_repayment,
    l.total_outstanding                                         as total_outstanding,
    l.total_overdue                                             as total_overdue,

    -- risk
    l.days_past_due                                             as days_past_due,
    l.par_bucket                                                as par_bucket,
    l.is_in_default                                             as is_in_default,
    l.delinquent_amount                                         as delinquent_amount,
    -- PAR30 exposure: the outstanding balance of loans 30+ days late.
    -- Stored as a measure so a dashboard sums a column instead of
    -- re-deriving a definition.
    if(l.days_past_due > 30, l.total_outstanding, toDecimal64(0, 6)) as par30_exposure,
    if(l.days_past_due > 90, l.total_outstanding, toDecimal64(0, 6)) as par90_exposure,

    -- behaviour
    coalesce(b.repayment_count, 0)                      as repayment_count,
    coalesce(b.total_repaid, 0)                         as ledger_total_repaid,
    b.avg_repayment_amount                                      as avg_repayment_amount,
    b.first_repayment_date                                      as first_repayment_date,
    b.last_repayment_date                                       as last_repayment_date,
    b.days_since_last_activity                                  as days_since_last_activity,
    b.avg_days_between_repayments                               as avg_days_between_repayments,
    b.repayment_regularity_cv                                   as repayment_regularity_cv,
    coalesce(b.reversed_transaction_count, 0)           as reversed_transaction_count,

    -- ratios
    l.principal_repaid_ratio                                    as principal_repaid_ratio,
    l.repayment_progress_ratio                                  as repayment_progress_ratio,
    {{ safe_divide('l.total_overdue', 'nullIf(l.total_outstanding, 0)') }} as overdue_ratio,

    -- Reconciliation: the loan summary Fineract reports vs the sum of
    -- the ledger it is supposedly derived from. A non-zero value here
    -- means CDC lost a transaction or the source is inconsistent - it is
    -- asserted on by a singular test, not just displayed.
    l.total_repayment - coalesce(b.total_repaid, 0)     as repayment_reconciliation_delta,

    -- lineage
    l.cdc_commit_at                                     as source_updated_at,
    now64(3)                                            as dbt_updated_at

from loans l
left join behaviour b on l.loan_id = b.loan_id
left join clients c on l.client_id = c.client_id
