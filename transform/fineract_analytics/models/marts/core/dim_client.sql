{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(client_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    Client dimension - current state (Type 1).

    Deliberately not a Type 2 SCD. History is already available two ways:
    fineract_raw.cdc_audit holds every change event, and the raw
    ReplacingMergeTree keeps versions until merge. Maintaining a third
    representation would be three sources of truth for the same question.
    If point-in-time client attributes become a hard requirement, the
    right move is a dbt snapshot over stg_fineract__clients, not a
    hand-rolled SCD here - noted in docs/DATA_MODEL.md.
*/

with clients as (

    select * from {{ ref('stg_fineract__clients') }}

),

portfolio as (

    select * from {{ ref('int_client_portfolio') }}

),

offices as (

    select office_id, office_name, parent_office_name, hierarchy_depth
    from {{ ref('stg_fineract__offices') }}

)

select
    -- keys
    c.client_key                                                    as client_key,
    c.client_id                                                     as client_id,
    c.account_no                                                    as account_no,
    c.external_id                                                   as external_id,

    -- attributes
    c.display_name                                                  as display_name,
    c.firstname                                                     as firstname,
    c.lastname                                                      as lastname,
    c.status                                                        as status,
    c.is_active                                                     as is_active,
    c.legal_form                                                    as legal_form,
    c.gender                                                        as gender,
    c.client_type                                                   as client_type,
    c.client_classification                                         as client_classification,
    c.age_years                                                     as age_years,
    multiIf(
        c.age_years is null, 'Unknown',
        c.age_years < 25, '18-24',
        c.age_years < 35, '25-34',
        c.age_years < 45, '35-44',
        c.age_years < 55, '45-54',
        '55+'
    )                                                               as age_band,

    -- org
    c.office_id                                                     as office_id,
    coalesce(o.office_name, c.office_name)                          as office_name,
    o.parent_office_name                                            as parent_office_name,
    o.hierarchy_depth                                               as office_depth,
    c.staff_id                                                      as staff_id,
    c.loan_officer_name                                             as loan_officer_name,

    -- lifecycle
    c.activation_date                                               as activation_date,
    c.submitted_on_date                                             as submitted_on_date,
    c.closed_on_date                                                as closed_on_date,
    c.tenure_days                                                   as tenure_days,
    c.tenure_months                                                 as tenure_months,
    multiIf(
        c.tenure_months is null, 'Unknown',
        c.tenure_months < 6,  '0-6 months',
        c.tenure_months < 12, '6-12 months',
        c.tenure_months < 24, '1-2 years',
        c.tenure_months < 60, '2-5 years',
        '5+ years'
    )                                                               as tenure_band,

    -- relationship (from int_client_portfolio - aggregated exactly once)
    coalesce(p.total_loans, 0)                                      as total_loans,
    coalesce(p.active_loans, 0)                                     as active_loans,
    coalesce(p.closed_loans, 0)                                     as closed_loans,
    coalesce(p.written_off_loans, 0)                                as written_off_loans,
    coalesce(p.delinquent_loans, 0)                                 as delinquent_loans,
    coalesce(p.lifetime_disbursed, 0)                               as lifetime_disbursed,
    coalesce(p.lifetime_repaid, 0)                                  as lifetime_repaid,
    coalesce(p.current_total_outstanding, 0)                        as current_total_outstanding,
    coalesce(p.current_total_overdue, 0)                            as current_total_overdue,
    coalesce(p.total_savings_balance, 0)                            as total_savings_balance,
    coalesce(p.active_savings_accounts, 0)                          as active_savings_accounts,
    coalesce(p.is_repeat_borrower, 0)                               as is_repeat_borrower,
    coalesce(p.is_multi_product_client, 0)                          as is_multi_product_client,
    p.historical_write_off_rate                                     as historical_write_off_rate,
    p.lifetime_repayment_ratio                                      as lifetime_repayment_ratio,
    p.savings_to_debt_ratio                                         as savings_to_debt_ratio,
    coalesce(p.current_worst_days_past_due, 0)                      as current_worst_days_past_due,
    {{ par_bucket('p.current_worst_days_past_due') }}               as current_par_bucket,

    -- Segmentation used by the portfolio dashboards. One definition,
    -- here, rather than one CASE expression per BI tool.
    multiIf(
        coalesce(p.active_loans, 0) = 0 and coalesce(p.total_loans, 0) = 0, 'Prospect',
        coalesce(p.active_loans, 0) = 0,                                    'Dormant',
        coalesce(p.current_worst_days_past_due, 0) > {{ var('default_threshold_days') }}, 'At risk',
        coalesce(p.current_worst_days_past_due, 0) > 0,                     'Watch',
        coalesce(p.is_repeat_borrower, 0) = 1,                              'Repeat - performing',
        'New - performing'
    )                                                               as client_segment,

    -- lineage
    c.cdc_commit_at                                                 as source_updated_at,
    now64(3)                                                        as dbt_updated_at

from clients c
left join portfolio p on c.client_id = p.client_id
left join offices o on c.office_id = o.office_id
