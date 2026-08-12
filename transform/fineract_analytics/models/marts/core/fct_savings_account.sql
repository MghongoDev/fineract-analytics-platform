{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(office_id, product_id, savings_id)',
        settings={'index_granularity': 8192, 'allow_nullable_key': 1}
    )
}}

/*
    Savings fact - one row per deposit account, current state.

    Savings matter here for a reason beyond completeness: in microfinance
    the savings relationship is one of the better predictors of credit
    behaviour, and `savings_to_debt_ratio` is a feature the scoring model
    consumes. Modelling deposits as a first-class fact keeps that link
    explicit rather than buried in a client rollup.

    Not partitioned: the account population is small and long-lived, and
    partitioning it would create many tiny parts for no pruning benefit.
*/

with savings as (

    select * from {{ ref('stg_fineract__savings_accounts') }}

),

products as (

    select * from {{ ref('stg_fineract__savings_products') }}

),

clients as (

    select client_id, client_segment, client_classification, gender, office_name
    from {{ ref('dim_client') }}

)

select
    -- keys
    s.savings_key                                       as savings_key,
    s.savings_id                                        as savings_id,
    s.account_no                                        as account_no,
    s.client_id                                         as client_id,
    s.product_id                                        as product_id,
    s.office_id                                         as office_id,
    s.field_officer_id                                  as field_officer_id,

    -- descriptive
    p.product_name                                      as product_name,
    p.interest_posting_period_type                      as interest_posting_period,
    s.status                                            as status,
    s.is_active                                         as is_active,
    s.currency_code                                     as currency_code,
    c.client_segment                                    as client_segment,
    c.client_classification                             as client_classification,
    c.gender                                            as gender,
    coalesce(c.office_name, '')                         as office_name,

    -- lifecycle
    s.submitted_on_date                                 as submitted_on_date,
    s.activated_on_date                                 as activated_on_date,
    s.closed_on_date                                    as closed_on_date,
    s.days_since_activation                             as days_since_activation,

    -- measures
    s.account_balance                                   as account_balance,
    s.available_balance                                 as available_balance,
    s.total_deposits                                    as total_deposits,
    s.total_withdrawals                                 as total_withdrawals,
    s.total_interest_posted                             as total_interest_posted,
    s.nominal_annual_interest_rate                      as nominal_annual_interest_rate,

    -- ratios
    s.withdrawal_ratio                                  as withdrawal_ratio,
    {{ safe_divide('s.total_deposits',
                   'nullIf(s.days_since_activation, 0)') }} as avg_daily_deposit_rate,

    -- Behavioural banding used by the deposit-mobilisation dashboards.
    multiIf(
        s.is_active = 0,                                       'Closed',
        s.account_balance <= 0,                                'Empty',
        s.withdrawal_ratio is null,                            'No activity',
        s.withdrawal_ratio > 0.9,                              'Transactional',
        s.withdrawal_ratio > 0.5,                              'Mixed',
        'Accumulating'
    )                                                   as savings_behaviour,

    s.cdc_commit_at                                     as source_updated_at,
    now64(3)                                            as dbt_updated_at

from savings s
left join products p on s.product_id = p.product_id
left join clients c on s.client_id = c.client_id
