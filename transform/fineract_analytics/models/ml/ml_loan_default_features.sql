{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMM(observation_date)',
        order_by='(observation_date, loan_id)',
        settings={'index_granularity': 8192, 'allow_nullable_key': 1}
    )
}}

/*
    ============================================================
    Training dataset: probability of default at origination.
    ============================================================

    Grain: one row per disbursed loan.
    Observation date: the loan's disbursement date.
    Label: `label_defaulted` - the loan later reached
           {{ var('default_threshold_days') }}+ days past due or was written off.

    POINT-IN-TIME CORRECTNESS
    -------------------------
    This is the part that makes or breaks a credit model, so it is worth
    being explicit about.

    Every feature in this table is computed from information that existed
    **strictly before the observation date**:

      * prior-loan features join the borrower's OTHER loans with
        `prev.disbursed_on_date < this.disbursed_on_date`
      * prior-repayment features only read ledger rows with
        `transaction_date < this.disbursed_on_date`
      * client attributes are demographic (age at origination, tenure at
        origination) and are re-derived AS OF the observation date rather
        than taken from the current dimension row

    What is deliberately NOT here: the loan's own repayment behaviour,
    current balances, current delinquency, `client_segment` from
    dim_client. Every one of those is computed from post-origination
    data, and every one of them would leak the label. A model trained on
    them scores ~0.95 AUC offline and is worthless in production - the
    single most common failure in credit-risk feature engineering.

    The one intentional exception is the label itself, which by
    definition looks forward from the observation date.

    Cost note: the prior-loan join is an inequality self-join. At demo
    scale it is trivial; at millions of loans, the right shape is an ASOF
    JOIN or a windowed pre-aggregation - see docs/SCALING.md.
*/

with loans as (

    select * from {{ ref('stg_fineract__loans') }}
    where disbursed_on_date is not null

),

clients as (

    select * from {{ ref('stg_fineract__clients') }}

),

products as (

    select * from {{ ref('stg_fineract__loan_products') }}

),

transactions as (

    select * from {{ ref('stg_fineract__loan_transactions') }}

),

/* ---------------------------------------------------------------
   Prior loan history, strictly before this loan was disbursed.
   --------------------------------------------------------------- */
prior_loans as (

    select
        cur.loan_id                                                 as loan_id,
        count()                                                     as prior_loan_count,
        countIf(prev.is_written_off = 1)                            as prior_written_off_count,
        countIf(prev.days_past_due > 30)                            as prior_delinquent_count,
        sum(prev.principal_disbursed)                               as prior_total_disbursed,
        max(prev.principal)                                         as prior_max_principal,
        avg(toFloat64(prev.principal))                              as prior_avg_principal,
        countDistinct(prev.product_id)                              as prior_distinct_products,
        max(prev.disbursed_on_date)                                 as prior_last_disbursement_date,
        min(prev.disbursed_on_date)                                 as prior_first_disbursement_date
    from loans cur
    inner join loans prev
        on cur.client_id = prev.client_id
       and prev.loan_id != cur.loan_id
       and prev.disbursed_on_date < cur.disbursed_on_date
    group by cur.loan_id

),

/* ---------------------------------------------------------------
   Repayment behaviour on those prior loans, again strictly before
   the observation date.
   --------------------------------------------------------------- */
prior_repayments as (

    select
        cur.loan_id                                                 as loan_id,
        count()                                                     as prior_repayment_count,
        sum(tx.amount)                                              as prior_total_repaid,
        avg(toFloat64(tx.amount))                                   as prior_avg_repayment,
        countIf(tx.is_reversed = 1)                                 as prior_reversal_count,
        max(tx.transaction_date)                                    as prior_last_repayment_date
    from loans cur
    inner join loans prev
        on cur.client_id = prev.client_id
       and prev.loan_id != cur.loan_id
       and prev.disbursed_on_date < cur.disbursed_on_date
    inner join transactions tx
        on tx.loan_id = prev.loan_id
       and tx.transaction_date < cur.disbursed_on_date
       and tx.transaction_category = 'repayment'
    group by cur.loan_id

)

select
    -- ---------------- identifiers (excluded from training) -------
    l.loan_id                                                       as loan_id,
    l.client_id                                                     as client_id,
    l.disbursed_on_date                                             as observation_date,
    toYYYYMM(l.disbursed_on_date)                                   as observation_month,

    -- ---------------- LABEL --------------------------------------
    if(l.is_in_default = 1 or l.is_written_off = 1, 1, 0)           as label_defaulted,
    l.days_past_due                                                 as label_days_past_due,
    l.is_written_off                                                as label_written_off,

    -- ---------------- loan terms (known at origination) ----------
    toFloat64(l.principal)                                          as feat_principal,
    l.number_of_repayments                                          as feat_number_of_repayments,
    toFloat64(l.annual_interest_rate)                               as feat_annual_interest_rate,
    l.term_days                                                     as feat_term_days,
    toFloat64(l.principal) / nullIf(l.number_of_repayments, 0)      as feat_installment_size,
    if(l.submitted_on_date is null, null,
       dateDiff('day', l.submitted_on_date, l.disbursed_on_date))   as feat_days_application_to_disbursement,

    -- ---------------- product ------------------------------------
    l.product_id                                                    as feat_product_id,
    p.product_name                                                  as feat_product_name,
    toFloat64(p.default_principal)                                  as feat_product_default_principal,
    -- how far this loan deviates from the product's normal size:
    -- outsized loans against a product's own distribution are a
    -- well-known risk signal
    {{ safe_divide('toFloat64(l.principal)', 'nullIf(toFloat64(p.default_principal), 0)') }}
                                                                    as feat_principal_vs_product_default,

    -- ---------------- borrower, AS OF the observation date -------
    c.gender                                                        as feat_gender,
    c.legal_form                                                    as feat_legal_form,
    c.client_type                                                   as feat_client_type,
    c.client_classification                                         as feat_client_classification,
    if(c.date_of_birth is null, null,
       dateDiff('year', c.date_of_birth, l.disbursed_on_date))      as feat_age_at_origination,
    if(c.activation_date is null, null,
       dateDiff('day', c.activation_date, l.disbursed_on_date))     as feat_client_tenure_days,

    -- ---------------- organisation -------------------------------
    l.office_id                                                     as feat_office_id,
    l.loan_officer_id                                               as feat_loan_officer_id,

    -- ---------------- prior relationship -------------------------
    coalesce(pl.prior_loan_count, 0)                                as feat_prior_loan_count,
    coalesce(pl.prior_written_off_count, 0)                         as feat_prior_written_off_count,
    coalesce(pl.prior_delinquent_count, 0)                          as feat_prior_delinquent_count,
    toFloat64(coalesce(pl.prior_total_disbursed, 0))                as feat_prior_total_disbursed,
    toFloat64(coalesce(pl.prior_max_principal, 0))                  as feat_prior_max_principal,
    coalesce(pl.prior_distinct_products, 0)                         as feat_prior_distinct_products,
    if(pl.prior_last_disbursement_date is null, null,
       dateDiff('day', pl.prior_last_disbursement_date, l.disbursed_on_date))
                                                                    as feat_days_since_prior_loan,
    {{ safe_divide('pl.prior_written_off_count', 'nullIf(pl.prior_loan_count, 0)') }}
                                                                    as feat_prior_write_off_rate,
    {{ safe_divide('pl.prior_delinquent_count', 'nullIf(pl.prior_loan_count, 0)') }}
                                                                    as feat_prior_delinquency_rate,
    -- Is this loan a step up from anything they have handled before?
    {{ safe_divide('toFloat64(l.principal)', 'nullIf(toFloat64(pl.prior_max_principal), 0)') }}
                                                                    as feat_principal_vs_prior_max,

    -- ---------------- prior repayment behaviour ------------------
    coalesce(pr.prior_repayment_count, 0)                           as feat_prior_repayment_count,
    toFloat64(coalesce(pr.prior_total_repaid, 0))                   as feat_prior_total_repaid,
    coalesce(pr.prior_avg_repayment, 0)                             as feat_prior_avg_repayment,
    coalesce(pr.prior_reversal_count, 0)                            as feat_prior_reversal_count,
    if(pr.prior_last_repayment_date is null, null,
       dateDiff('day', pr.prior_last_repayment_date, l.disbursed_on_date))
                                                                    as feat_days_since_prior_repayment,
    if(coalesce(pl.prior_loan_count, 0) = 0, 1, 0)                  as feat_is_first_time_borrower,

    -- ---------------- split hint ---------------------------------
    -- Time-based, not random: a random split leaks the future into the
    -- training set for a temporally-ordered process like lending.
    multiIf(
        l.disbursed_on_date < today() - INTERVAL 12 MONTH, 'train',
        l.disbursed_on_date < today() - INTERVAL 6 MONTH,  'validation',
        'test'
    )                                                               as split_hint,

    -- Loans too young to have shown default behaviour yet must be
    -- excluded from training, or the model learns "new loans never
    -- default".
    if(dateDiff('day', l.disbursed_on_date, today()) >= 180, 1, 0)  as is_label_mature,

    now64(3)                                                        as dbt_updated_at

from loans l
left join clients c on l.client_id = c.client_id
left join products p on l.product_id = p.product_id
left join prior_loans pl on l.loan_id = pl.loan_id
left join prior_repayments pr on l.loan_id = pr.loan_id
