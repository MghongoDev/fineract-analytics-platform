{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(client_id)',
        settings={'index_granularity': 8192}
    )
}}

/*
    Staging: clients.

    Responsibilities of this layer, and nothing else:
      1. collapse the CDC stream to one current row per key
      2. drop deleted rows
      3. cast and rename into warehouse conventions
      4. add cheap, non-controversial derived attributes (age, tenure)

    No joins, no business rules, no aggregation - so a mart that looks
    wrong can always be bisected against exactly one source table.

    FINAL is used here rather than argMax: the client dimension is small
    (tens of thousands of rows even at scale) and FINAL keeps the model
    readable and correct regardless of merge state.
*/

with source as (

    select *
    from {{ source('fineract_raw', 'clients') }} final
    where _is_deleted = 0

),

renamed as (

    select
        -- keys
        client_id,
        {{ surrogate_key(['client_id']) }}                          as client_key,
        account_no,
        external_id,

        -- status
        status_id,
        status_code,
        status_value                                                as status,
        sub_status_value                                            as sub_status,
        coalesce(is_active, 0)                                      as is_active,

        -- dates
        activation_date,
        submitted_on_date,
        closed_on_date,
        date_of_birth,

        -- org
        office_id,
        office_name,
        staff_id,
        staff_name                                                  as loan_officer_name,

        -- demographics (used by the ML feature layer and by
        -- disaggregated impact reporting)
        legal_form_value                                            as legal_form,
        gender_value                                                as gender,
        client_type_value                                           as client_type,
        client_classification_value                                 as client_classification,

        -- identity
        firstname,
        lastname,
        display_name,
        mobile_no,
        email_address,

        -- derived: cheap, deterministic, no business judgement
        if(date_of_birth is null, null,
           dateDiff('year', date_of_birth, today()))                as age_years,
        if(activation_date is null, null,
           dateDiff('day', activation_date, today()))               as tenure_days,
        if(activation_date is null, null,
           dateDiff('month', activation_date, today()))             as tenure_months,

        -- lineage
        source_ingested_at,
        source_updated_at,
        _source_commit_at                                           as cdc_commit_at,
        _version                                                    as cdc_version

    from source

)

select * from renamed
