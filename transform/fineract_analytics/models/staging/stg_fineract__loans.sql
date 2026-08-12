{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(loan_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    Staging: loan accounts.

    The one piece of judgement applied here is `days_past_due`, and it is
    applied here on purpose: Fineract reports delinquency two different
    ways (`delinquent.pastDueDays` and `summary.overdueSinceDate`) and
    they disagree when the delinquency bucket configuration changes. Every
    downstream consumer must use the same reconciliation, so it happens
    once, at the boundary, with the rule written down:

        prefer the explicit delinquency counter; fall back to
        today - overdue_since_date; otherwise 0.
*/

with source as (

    select *
    from {{ source('fineract_raw', 'loans') }} final
    where _is_deleted = 0

),

renamed as (

    select
        -- keys
        loan_id,
        {{ surrogate_key(['loan_id']) }}                            as loan_key,
        account_no,
        external_id,
        client_id,
        group_id,
        product_id,
        office_id,
        loan_officer_id,

        -- denormalised labels straight from the source (kept for
        -- traceability; marts join the real dimensions instead)
        client_name,
        product_name,
        office_name,
        loan_officer_name,

        -- classification
        loan_type,
        currency_code,
        status_id,
        status_code,
        status_value                                                as status,
        coalesce(is_active, 0)                                      as is_active,
        coalesce(is_closed, 0)                                      as is_closed,
        coalesce(is_overpaid, 0)                                    as is_overpaid,
        if(status_id = 601, 1, 0)                                   as is_written_off,

        -- lifecycle
        submitted_on_date,
        approved_on_date,
        disbursed_on_date,
        expected_maturity_date,
        closed_on_date,
        overdue_since_date,

        -- terms
        term_frequency,
        term_frequency_type,
        number_of_repayments,
        repayment_every,
        repayment_frequency_type,
        interest_rate_per_period,
        annual_interest_rate,

        -- principal
        {{ money('principal') }}                                    as principal,
        {{ money('approved_principal') }}                           as approved_principal,
        {{ money('principal_disbursed') }}                          as principal_disbursed,
        {{ money('principal_paid') }}                               as principal_paid,
        {{ money('principal_written_off') }}                        as principal_written_off,
        {{ money('principal_outstanding') }}                        as principal_outstanding,
        {{ money('principal_overdue') }}                            as principal_overdue,

        -- interest
        {{ money('interest_charged') }}                             as interest_charged,
        {{ money('interest_paid') }}                                as interest_paid,
        {{ money('interest_waived') }}                              as interest_waived,
        {{ money('interest_outstanding') }}                         as interest_outstanding,
        {{ money('interest_overdue') }}                             as interest_overdue,

        -- charges
        {{ money('fee_charges_charged') }}                          as fee_charges_charged,
        {{ money('fee_charges_paid') }}                             as fee_charges_paid,
        {{ money('fee_charges_outstanding') }}                      as fee_charges_outstanding,
        {{ money('penalty_charges_charged') }}                      as penalty_charges_charged,
        {{ money('penalty_charges_paid') }}                         as penalty_charges_paid,
        {{ money('penalty_charges_outstanding') }}                  as penalty_charges_outstanding,

        -- totals
        {{ money('total_expected_repayment') }}                     as total_expected_repayment,
        {{ money('total_repayment') }}                              as total_repayment,
        {{ money('total_outstanding') }}                            as total_outstanding,
        {{ money('total_overdue') }}                                as total_overdue,
        {{ money('delinquent_amount') }}                            as delinquent_amount,

        -- delinquency: reconcile the two source representations, once.
        multiIf(
            delinquent_days is not null and delinquent_days > 0, delinquent_days,
            overdue_since_date is not null, dateDiff('day', overdue_since_date, today()),
            0
        )                                                           as days_past_due,

        -- lineage
        source_ingested_at,
        source_updated_at,
        _source_commit_at                                           as cdc_commit_at,
        _version                                                    as cdc_version

    from source

),

final as (

    select
        *,
        {{ par_bucket('days_past_due') }}                           as par_bucket,
        if(days_past_due > {{ var('default_threshold_days') }}, 1, 0) as is_in_default,

        -- Repayment progress. Guarded against the zero-principal loans
        -- that exist in every core banking system.
        {{ safe_divide('principal_paid', 'nullIf(principal_disbursed, 0)') }}
                                                                    as principal_repaid_ratio,
        {{ safe_divide('total_repayment', 'nullIf(total_expected_repayment, 0)') }}
                                                                    as repayment_progress_ratio,

        if(disbursed_on_date is null, null,
           dateDiff('day', disbursed_on_date, today()))             as days_since_disbursement,
        if(disbursed_on_date is null or expected_maturity_date is null, null,
           dateDiff('day', disbursed_on_date, expected_maturity_date)) as term_days

    from renamed

)

select * from final
