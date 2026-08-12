/*
    A transaction dated in the future is either a data-entry error in the
    core banking system or a timezone bug in the pipeline. Both matter:
    future-dated repayments inflate collections and deflate PAR, which is
    exactly the direction of error nobody notices until an audit.

    Two days of tolerance covers legitimate timezone skew between the
    Fineract tenant (configurable per deployment) and the UTC warehouse.
*/

select
    transaction_id,
    loan_id,
    transaction_date,
    dateDiff('day', today(), transaction_date) as days_in_future
from {{ ref('fct_loan_transaction') }}
where transaction_date > today() + 2
