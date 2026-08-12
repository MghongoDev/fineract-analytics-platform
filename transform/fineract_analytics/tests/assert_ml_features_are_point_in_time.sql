/*
    Label-leakage guard for the training dataset.

    Every "prior" feature must describe something that happened BEFORE the
    observation date. If a prior-loan or prior-repayment feature is
    derived from data at or after origination, the model is being trained
    on the future - it will score beautifully offline and fail in
    production, and nothing else in the pipeline will complain.

    This test encodes the invariant so a refactor of the feature SQL
    cannot quietly break it:

      * feat_days_since_prior_loan must be strictly positive
      * feat_days_since_prior_repayment must be strictly positive
      * a first-time borrower must have no prior-loan features
*/

select
    loan_id,
    observation_date,
    feat_days_since_prior_loan,
    feat_days_since_prior_repayment,
    feat_is_first_time_borrower,
    feat_prior_loan_count,
    'leakage: prior feature is not strictly before observation_date' as failure_reason
from {{ ref('ml_loan_default_features') }}
where (feat_days_since_prior_loan is not null and feat_days_since_prior_loan <= 0)
   or (feat_days_since_prior_repayment is not null and feat_days_since_prior_repayment <= 0)
   or (feat_is_first_time_borrower = 1 and feat_prior_loan_count > 0)
   or (feat_is_first_time_borrower = 0 and feat_prior_loan_count = 0)
