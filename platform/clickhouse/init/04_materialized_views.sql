-- =====================================================================
-- 04_materialized_views.sql
--
-- The MVs are the only place where wire types become analytical types.
-- Keeping every cast here means:
--   * the Kafka consumer stays maximally forgiving (a parse failure there
--     stalls a whole partition; here it is a NULL we can see and alert on)
--   * dbt never has to know that Debezium encodes DATE as an int
--
-- Conversion contract
-- -------------------
--   NUMERIC  String  -> toDecimal64OrNull(x, 6)                (no float, no rounding)
--   DATE     Int32   -> toDate(x)   (Int32 days-since-epoch IS ClickHouse's Date)
--   TIMESTAMPTZ Str  -> parseDateTime64BestEffortOrNull(x, 3, 'UTC')
--   BOOLEAN  Bool    -> toUInt8(x)
--
-- Version / delete semantics
-- --------------------------
--   _version    = __source_ts_ms  (Postgres COMMIT time, not read time)
--   _is_deleted = __op = 'd' OR __deleted = 'true'
--
-- Three MVs per topic:
--   mv_<t>          typed rows        -> fineract_raw.<t>
--   mv_<t>_errors   poison messages   -> fineract_raw.cdc_errors
--   mv_<t>_audit    every event       -> fineract_raw.cdc_audit
--
-- Audit MVs are attached only to the mutable, high-value entities
-- (clients, loans, loan_transactions, savings_accounts). The product and
-- office dimensions change a few times a year; auditing them would add
-- parts and merges for no operational answer we do not already have.
-- =====================================================================

-- =====================================================================
-- offices
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_offices
TO fineract_raw.offices AS
SELECT
    assumeNotNull(office_id)                                    AS office_id,
    ifNull(name, '')                                            AS name,
    name_decorated,
    external_id,
    parent_id,
    parent_name,
    hierarchy,
    toDate(opening_date)                                        AS opening_date,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_offices
WHERE length(_error) = 0 AND office_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_offices_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_offices
WHERE length(_error) > 0;

-- =====================================================================
-- staff
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_staff
TO fineract_raw.staff AS
SELECT
    assumeNotNull(staff_id)                                     AS staff_id,
    display_name, firstname, lastname, office_id, office_name, mobile_no,
    toUInt8(is_loan_officer)                                    AS is_loan_officer,
    toUInt8(is_active)                                          AS is_active,
    toDate(joining_date)                                        AS joining_date,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_staff
WHERE length(_error) = 0 AND staff_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_staff_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_staff WHERE length(_error) > 0;

-- =====================================================================
-- loan_products
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loan_products
TO fineract_raw.loan_products AS
SELECT
    assumeNotNull(product_id)                                   AS product_id,
    ifNull(name, '')                                            AS name,
    short_name, description, fund_name, currency_code, currency_decimal_places,
    toDecimal64OrNull(principal, 6)                             AS principal,
    toDecimal64OrNull(min_principal, 6)                         AS min_principal,
    toDecimal64OrNull(max_principal, 6)                         AS max_principal,
    number_of_repayments, repayment_every, repayment_frequency_type,
    toDecimal64OrNull(interest_rate_per_period, 6)              AS interest_rate_per_period,
    interest_rate_frequency_type,
    toDecimal64OrNull(annual_interest_rate, 6)                  AS annual_interest_rate,
    amortization_type, interest_type, interest_calculation_period_type, status,
    toDate(start_date)                                          AS start_date,
    toDate(close_date)                                          AS close_date,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_loan_products
WHERE length(_error) = 0 AND product_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loan_products_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_loan_products WHERE length(_error) > 0;

-- =====================================================================
-- savings_products
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_savings_products
TO fineract_raw.savings_products AS
SELECT
    assumeNotNull(product_id)                                   AS product_id,
    ifNull(name, '')                                            AS name,
    short_name, description, currency_code, currency_decimal_places,
    toDecimal64OrNull(nominal_annual_interest_rate, 6)          AS nominal_annual_interest_rate,
    interest_compounding_period_type, interest_posting_period_type,
    toDecimal64OrNull(min_required_opening_balance, 6)          AS min_required_opening_balance,
    status,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_savings_products
WHERE length(_error) = 0 AND product_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_savings_products_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_savings_products WHERE length(_error) > 0;

-- =====================================================================
-- clients
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_clients
TO fineract_raw.clients AS
SELECT
    assumeNotNull(client_id)                                    AS client_id,
    account_no, external_id, status_id, status_code, status_value, sub_status_value,
    toUInt8(is_active)                                          AS is_active,
    toDate(activation_date)                                     AS activation_date,
    toDate(submitted_on_date)                                   AS submitted_on_date,
    toDate(closed_on_date)                                      AS closed_on_date,
    office_id, office_name, staff_id, staff_name,
    legal_form_value, gender_value, client_type_value, client_classification_value,
    firstname, lastname, display_name, mobile_no, email_address,
    toDate(date_of_birth)                                       AS date_of_birth,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_clients
WHERE length(_error) = 0 AND client_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_clients_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_clients WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_clients_audit
TO fineract_raw.cdc_audit AS
SELECT
    'clients'                                                   AS source_table,
    assumeNotNull(client_id)                                    AS primary_key,
    ifNull(__op, 'r')                                           AS op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS lsn
FROM fineract_raw.kafka_clients
WHERE length(_error) = 0 AND client_id IS NOT NULL;

-- =====================================================================
-- loans
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loans
TO fineract_raw.loans AS
SELECT
    assumeNotNull(loan_id)                                      AS loan_id,
    account_no, external_id, client_id, client_name, group_id,
    product_id, product_name, office_id, office_name,
    loan_officer_id, loan_officer_name, loan_type,
    currency_code, currency_decimal_places,
    status_id, status_code, status_value,
    toUInt8(is_active)                                          AS is_active,
    toUInt8(is_overpaid)                                        AS is_overpaid,
    toUInt8(is_closed)                                          AS is_closed,
    toDate(submitted_on_date)                                   AS submitted_on_date,
    toDate(approved_on_date)                                    AS approved_on_date,
    toDate(disbursed_on_date)                                   AS disbursed_on_date,
    toDate(expected_maturity_date)                              AS expected_maturity_date,
    toDate(closed_on_date)                                      AS closed_on_date,
    term_frequency, term_frequency_type, number_of_repayments,
    repayment_every, repayment_frequency_type,
    toDecimal64OrNull(interest_rate_per_period, 6)              AS interest_rate_per_period,
    toDecimal64OrNull(annual_interest_rate, 6)                  AS annual_interest_rate,
    toDecimal64OrNull(principal, 6)                             AS principal,
    toDecimal64OrNull(approved_principal, 6)                    AS approved_principal,
    toDecimal64OrNull(principal_disbursed, 6)                   AS principal_disbursed,
    toDecimal64OrNull(principal_paid, 6)                        AS principal_paid,
    toDecimal64OrNull(principal_written_off, 6)                 AS principal_written_off,
    toDecimal64OrNull(principal_outstanding, 6)                 AS principal_outstanding,
    toDecimal64OrNull(principal_overdue, 6)                     AS principal_overdue,
    toDecimal64OrNull(interest_charged, 6)                      AS interest_charged,
    toDecimal64OrNull(interest_paid, 6)                         AS interest_paid,
    toDecimal64OrNull(interest_waived, 6)                       AS interest_waived,
    toDecimal64OrNull(interest_outstanding, 6)                  AS interest_outstanding,
    toDecimal64OrNull(interest_overdue, 6)                      AS interest_overdue,
    toDecimal64OrNull(fee_charges_charged, 6)                   AS fee_charges_charged,
    toDecimal64OrNull(fee_charges_paid, 6)                      AS fee_charges_paid,
    toDecimal64OrNull(fee_charges_outstanding, 6)               AS fee_charges_outstanding,
    toDecimal64OrNull(penalty_charges_charged, 6)               AS penalty_charges_charged,
    toDecimal64OrNull(penalty_charges_paid, 6)                  AS penalty_charges_paid,
    toDecimal64OrNull(penalty_charges_outstanding, 6)           AS penalty_charges_outstanding,
    toDecimal64OrNull(total_expected_repayment, 6)              AS total_expected_repayment,
    toDecimal64OrNull(total_repayment, 6)                       AS total_repayment,
    toDecimal64OrNull(total_outstanding, 6)                     AS total_outstanding,
    toDecimal64OrNull(total_overdue, 6)                         AS total_overdue,
    toDate(overdue_since_date)                                  AS overdue_since_date,
    delinquent_days,
    toDecimal64OrNull(delinquent_amount, 6)                     AS delinquent_amount,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_loans
WHERE length(_error) = 0 AND loan_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loans_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_loans WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loans_audit
TO fineract_raw.cdc_audit AS
SELECT
    'loans'                                                     AS source_table,
    assumeNotNull(loan_id)                                      AS primary_key,
    ifNull(__op, 'r')                                           AS op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS lsn
FROM fineract_raw.kafka_loans
WHERE length(_error) = 0 AND loan_id IS NOT NULL;

-- =====================================================================
-- loan_transactions
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loan_transactions
TO fineract_raw.loan_transactions AS
SELECT
    assumeNotNull(transaction_id)                               AS transaction_id,
    assumeNotNull(loan_id)                                      AS loan_id,
    office_id, office_name, type_id, type_code, type_value,
    toUInt8(is_reversed)                                        AS is_reversed,
    toDate(assumeNotNull(transaction_date))                     AS transaction_date,
    toDate(submitted_on_date)                                   AS submitted_on_date,
    currency_code,
    toDecimal64OrNull(amount, 6)                                AS amount,
    toDecimal64OrNull(net_disbursal_amount, 6)                  AS net_disbursal_amount,
    toDecimal64OrNull(principal_portion, 6)                     AS principal_portion,
    toDecimal64OrNull(interest_portion, 6)                      AS interest_portion,
    toDecimal64OrNull(fee_charges_portion, 6)                   AS fee_charges_portion,
    toDecimal64OrNull(penalty_charges_portion, 6)               AS penalty_charges_portion,
    toDecimal64OrNull(overpayment_portion, 6)                   AS overpayment_portion,
    toDecimal64OrNull(outstanding_loan_balance, 6)              AS outstanding_loan_balance,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_loan_transactions
WHERE length(_error) = 0
  AND transaction_id IS NOT NULL
  AND loan_id IS NOT NULL
  AND transaction_date IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loan_transactions_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_loan_transactions WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_loan_transactions_audit
TO fineract_raw.cdc_audit AS
SELECT
    'loan_transactions'                                         AS source_table,
    assumeNotNull(transaction_id)                               AS primary_key,
    ifNull(__op, 'r')                                           AS op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS lsn
FROM fineract_raw.kafka_loan_transactions
WHERE length(_error) = 0 AND transaction_id IS NOT NULL;

-- =====================================================================
-- savings_accounts
-- =====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_savings_accounts
TO fineract_raw.savings_accounts AS
SELECT
    assumeNotNull(savings_id)                                   AS savings_id,
    account_no, client_id, client_name, product_id, product_name,
    office_id, field_officer_id, status_id, status_value,
    toUInt8(is_active)                                          AS is_active,
    currency_code,
    toDecimal64OrNull(nominal_annual_interest_rate, 6)          AS nominal_annual_interest_rate,
    toDate(submitted_on_date)                                   AS submitted_on_date,
    toDate(activated_on_date)                                   AS activated_on_date,
    toDate(closed_on_date)                                      AS closed_on_date,
    toDecimal64OrNull(account_balance, 6)                       AS account_balance,
    toDecimal64OrNull(available_balance, 6)                     AS available_balance,
    toDecimal64OrNull(total_deposits, 6)                        AS total_deposits,
    toDecimal64OrNull(total_withdrawals, 6)                     AS total_withdrawals,
    toDecimal64OrNull(total_interest_posted, 6)                 AS total_interest_posted,
    parseDateTime64BestEffortOrNull(_ingested_at, 3, 'UTC')     AS source_ingested_at,
    parseDateTime64BestEffortOrNull(_updated_at, 3, 'UTC')      AS source_updated_at,
    _payload_hash                                               AS payload_hash,
    ifNull(__op, 'r')                                           AS _op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS _source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS _cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS _lsn,
    toInt64(ifNull(__source_txId, 0))                           AS _tx_id,
    toUInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0)))        AS _version,
    if(ifNull(__op, '') = 'd' OR ifNull(__deleted, 'false') = 'true', 1, 0) AS _is_deleted
FROM fineract_raw.kafka_savings_accounts
WHERE length(_error) = 0 AND savings_id IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_savings_accounts_errors
TO fineract_raw.cdc_errors AS
SELECT _topic AS topic, _partition AS partition, _offset AS offset,
       _error AS error, _raw_message AS raw_message
FROM fineract_raw.kafka_savings_accounts WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS fineract_raw.mv_savings_accounts_audit
TO fineract_raw.cdc_audit AS
SELECT
    'savings_accounts'                                          AS source_table,
    assumeNotNull(savings_id)                                   AS primary_key,
    ifNull(__op, 'r')                                           AS op,
    fromUnixTimestamp64Milli(toInt64(ifNull(__source_ts_ms, ifNull(__ts_ms, 0))), 'UTC') AS source_commit_at,
    fromUnixTimestamp64Milli(toInt64(ifNull(__ts_ms, 0)), 'UTC')                         AS cdc_read_at,
    toInt64(ifNull(__source_lsn, 0))                            AS lsn
FROM fineract_raw.kafka_savings_accounts
WHERE length(_error) = 0 AND savings_id IS NOT NULL;
