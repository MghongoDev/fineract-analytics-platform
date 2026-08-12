{{ config(materialized='table', engine='MergeTree()', order_by='(office_id)') }}

/*
    Office dimension with portfolio context attached, so a branch-level
    dashboard needs one table rather than a fan-out join over the loan
    book on every page load.
*/

with offices as (

    select * from {{ ref('stg_fineract__offices') }}

),

loan_rollup as (

    select
        office_id,
        count()                                     as total_loans,
        countIf(is_active = 1)                      as active_loans,
        sumIf(principal_outstanding, is_active = 1) as principal_outstanding,
        sumIf(total_outstanding, is_active = 1)     as total_outstanding,
        sumIf(total_overdue, is_active = 1)         as total_overdue,
        sum(principal_disbursed)                    as lifetime_disbursed,
        countDistinct(client_id)                    as distinct_borrowers
    from {{ ref('stg_fineract__loans') }}
    group by office_id

),

client_rollup as (

    select
        office_id,
        count()                                     as total_clients,
        countIf(is_active = 1)                      as active_clients
    from {{ ref('stg_fineract__clients') }}
    group by office_id

),

staff_rollup as (

    select
        office_id,
        count()                                     as total_staff,
        countIf(is_loan_officer = 1 and is_active = 1) as active_loan_officers
    from {{ ref('stg_fineract__staff') }}
    group by office_id

)

select
    o.office_key                                                as office_key,
    o.office_id                                                 as office_id,
    o.office_name                                               as office_name,
    o.office_name_decorated                                     as office_name_decorated,
    o.external_id                                               as external_id,
    o.parent_office_id                                          as parent_office_id,
    o.parent_office_name                                        as parent_office_name,
    o.hierarchy                                                 as hierarchy,
    o.hierarchy_depth                                           as hierarchy_depth,
    o.is_head_office                                            as is_head_office,
    o.opening_date                                              as opening_date,
    o.days_open                                                 as days_open,

    coalesce(c.total_clients, 0)                    as total_clients,
    coalesce(c.active_clients, 0)                   as active_clients,
    coalesce(s.total_staff, 0)                      as total_staff,
    coalesce(s.active_loan_officers, 0)             as active_loan_officers,

    coalesce(l.total_loans, 0)                      as total_loans,
    coalesce(l.active_loans, 0)                     as active_loans,
    coalesce(l.distinct_borrowers, 0)               as distinct_borrowers,
    coalesce(l.principal_outstanding, 0)            as principal_outstanding,
    coalesce(l.total_outstanding, 0)                as total_outstanding,
    coalesce(l.total_overdue, 0)                    as total_overdue,
    coalesce(l.lifetime_disbursed, 0)               as lifetime_disbursed,

    -- Portfolio at Risk as a share of the outstanding book: the headline
    -- health metric for a branch.
    {{ safe_divide('l.total_overdue', 'nullIf(l.total_outstanding, 0)') }} as par_ratio,
    {{ safe_divide('l.active_loans', 'nullIf(s.active_loan_officers, 0)') }} as loans_per_officer,

    now64(3)                                        as dbt_updated_at

from offices o
left join loan_rollup l on o.office_id = l.office_id
left join client_rollup c on o.office_id = c.office_id
left join staff_rollup s on o.office_id = s.office_id
