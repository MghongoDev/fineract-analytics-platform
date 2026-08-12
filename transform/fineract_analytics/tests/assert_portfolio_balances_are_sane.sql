/*
    Balance-sheet sanity rules that must hold for every active loan.

    These are not statistical checks - each one is an accounting identity
    that a correct core banking system cannot violate. A failure means
    either the source is inconsistent or the pipeline has corrupted a
    number, and both need a human before anyone reports on it.

      1. Outstanding principal cannot exceed what was disbursed.
      2. Balances cannot be negative.
      3. Overdue cannot exceed total outstanding.
      4. A loan cannot be both active and closed.
*/

with checks as (

    select
        loan_id,
        principal_disbursed,
        principal_outstanding,
        total_outstanding,
        total_overdue,
        is_active,
        is_closed,
        multiIf(
            principal_outstanding > principal_disbursed + 1,
                'outstanding principal exceeds disbursed',
            principal_outstanding < 0 or total_outstanding < 0 or total_overdue < 0,
                'negative balance',
            total_overdue > total_outstanding + 1,
                'overdue exceeds total outstanding',
            is_active = 1 and is_closed = 1,
                'loan is both active and closed',
            ''
        ) as failure_reason
    from {{ ref('fct_loan') }}
    where disbursed_on_date is not null

)

select * from checks where failure_reason != ''
