/*
    The PAR bucket is derived from days_past_due by a macro. If the two
    ever disagree, a definition has been changed in one place and not the
    other - and every risk report silently shifts.

    Cheap to check, and it protects the one number the board looks at.
*/

select
    loan_id,
    days_past_due,
    par_bucket,
    multiIf(
        days_past_due is null or days_past_due <= 0, 'Current',
        days_past_due <= 30,  'PAR 1-30',
        days_past_due <= 60,  'PAR 31-60',
        days_past_due <= 90,  'PAR 61-90',
        days_past_due <= 180, 'PAR 91-180',
        'PAR 180+'
    ) as expected_bucket
from {{ ref('fct_loan') }}
where par_bucket != multiIf(
        days_past_due is null or days_past_due <= 0, 'Current',
        days_past_due <= 30,  'PAR 1-30',
        days_past_due <= 60,  'PAR 31-60',
        days_past_due <= 90,  'PAR 61-90',
        days_past_due <= 180, 'PAR 91-180',
        'PAR 180+'
    )
