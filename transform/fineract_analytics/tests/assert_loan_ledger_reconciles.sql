/*
    The loan summary Fineract reports must equal the sum of the ledger it
    is derived from.

    This is the single most valuable test in the project. Every other test
    checks that the warehouse is internally consistent; this one checks
    that the warehouse agrees with the source of truth. A CDC event lost
    between Postgres and ClickHouse shows up here and essentially nowhere
    else - the loan row still looks perfectly valid on its own.

    Tolerance of 1.0 currency unit absorbs legitimate rounding between
    Fineract's own summary calculation and a naive ledger sum. Anything
    larger is a real gap.
*/

select
    loan_id,
    total_repayment,
    ledger_total_repaid,
    repayment_reconciliation_delta
from {{ ref('fct_loan') }}
where abs(toFloat64(repayment_reconciliation_delta)) > 1.0
  -- Only loans that have actually been disbursed can have a ledger.
  and disbursed_on_date is not null
  and repayment_count > 0
