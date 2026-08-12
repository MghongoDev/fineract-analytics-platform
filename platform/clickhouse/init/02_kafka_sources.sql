-- =====================================================================
-- 02_kafka_sources.sql - Kafka engine tables (the CDC consumers).
--
-- WHY A KAFKA ENGINE TABLE RATHER THAN A SINK CONNECTOR
-- -----------------------------------------------------
-- The alternative is clickhouse-kafka-connect running inside Connect.
-- The Kafka engine keeps the consumer inside ClickHouse, which means:
--   * one less JVM process to operate and monitor
--   * inserts are native and batched by ClickHouse's own flush settings
--   * back-pressure is visible in system.kafka_consumers, not buried in
--     Connect task logs
-- The trade-off - the consumer's schema now lives in ClickHouse DDL
-- rather than connector config - is handled below with
-- `input_format_skip_unknown_fields`, so a new column added upstream
-- cannot break the consumer.
--
-- TYPE STRATEGY
-- -------------
-- Everything lands as the *wire* type, and conversion happens in the
-- materialized view. A Kafka table that cannot parse a message stalls
-- the whole partition, so the parse here is made as forgiving as
-- possible and the strictness is moved one hop downstream where a
-- failure is recoverable.
--   NUMERIC  -> String  (decimal.handling.mode=string; parsed to Decimal in the MV)
--   DATE     -> Int32   (days since epoch = ClickHouse's own Date encoding)
--   TIMESTAMPTZ -> String (ISO-8601 from Debezium ZonedTimestamp)
--   BOOLEAN  -> Bool
--
-- ERROR HANDLING
-- --------------
-- kafka_handle_error_mode='stream' exposes `_error` and `_raw_message`
-- as virtual columns instead of killing the consumer on a poison
-- message. 04_materialized_views.sql routes those into
-- fineract_raw.cdc_errors, so a malformed event is quarantined and
-- alertable rather than a 3am outage.
-- =====================================================================

-- ---------------------------------------------------------------------
-- offices
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_offices
(
    office_id        Nullable(Int64),
    name             Nullable(String),
    name_decorated   Nullable(String),
    external_id      Nullable(String),
    parent_id        Nullable(Int64),
    parent_name      Nullable(String),
    hierarchy        Nullable(String),
    opening_date     Nullable(Int32),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.offices',
    kafka_group_name         = 'clickhouse_fineract_offices',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 1,
    kafka_max_block_size     = 8192,
    kafka_poll_max_batch_size = 4096,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1,
    date_time_input_format   = 'best_effort';

-- ---------------------------------------------------------------------
-- staff
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_staff
(
    staff_id         Nullable(Int64),
    display_name     Nullable(String),
    firstname        Nullable(String),
    lastname         Nullable(String),
    office_id        Nullable(Int64),
    office_name      Nullable(String),
    mobile_no        Nullable(String),
    is_loan_officer  Nullable(Bool),
    is_active        Nullable(Bool),
    joining_date     Nullable(Int32),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.staff',
    kafka_group_name         = 'clickhouse_fineract_staff',
    kafka_format             = 'JSONEachRow',
    kafka_max_block_size     = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- loan_products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_loan_products
(
    product_id                       Nullable(Int64),
    name                             Nullable(String),
    short_name                       Nullable(String),
    description                      Nullable(String),
    fund_name                        Nullable(String),
    currency_code                    Nullable(String),
    currency_decimal_places          Nullable(Int32),
    principal                        Nullable(String),
    min_principal                    Nullable(String),
    max_principal                    Nullable(String),
    number_of_repayments             Nullable(Int32),
    repayment_every                  Nullable(Int32),
    repayment_frequency_type         Nullable(String),
    interest_rate_per_period         Nullable(String),
    interest_rate_frequency_type     Nullable(String),
    annual_interest_rate             Nullable(String),
    amortization_type                Nullable(String),
    interest_type                    Nullable(String),
    interest_calculation_period_type Nullable(String),
    status                           Nullable(String),
    start_date                       Nullable(Int32),
    close_date                       Nullable(Int32),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.loan_products',
    kafka_group_name         = 'clickhouse_fineract_loan_products',
    kafka_format             = 'JSONEachRow',
    kafka_max_block_size     = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- savings_products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_savings_products
(
    product_id                       Nullable(Int64),
    name                             Nullable(String),
    short_name                       Nullable(String),
    description                      Nullable(String),
    currency_code                    Nullable(String),
    currency_decimal_places          Nullable(Int32),
    nominal_annual_interest_rate     Nullable(String),
    interest_compounding_period_type Nullable(String),
    interest_posting_period_type     Nullable(String),
    min_required_opening_balance     Nullable(String),
    status                           Nullable(String),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.savings_products',
    kafka_group_name         = 'clickhouse_fineract_savings_products',
    kafka_format             = 'JSONEachRow',
    kafka_max_block_size     = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- clients
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_clients
(
    client_id                   Nullable(Int64),
    account_no                  Nullable(String),
    external_id                 Nullable(String),
    status_id                   Nullable(Int32),
    status_code                 Nullable(String),
    status_value                Nullable(String),
    sub_status_value            Nullable(String),
    is_active                   Nullable(Bool),
    activation_date             Nullable(Int32),
    submitted_on_date           Nullable(Int32),
    closed_on_date              Nullable(Int32),
    office_id                   Nullable(Int64),
    office_name                 Nullable(String),
    staff_id                    Nullable(Int64),
    staff_name                  Nullable(String),
    legal_form_value            Nullable(String),
    gender_value                Nullable(String),
    client_type_value           Nullable(String),
    client_classification_value Nullable(String),
    firstname                   Nullable(String),
    lastname                    Nullable(String),
    display_name                Nullable(String),
    mobile_no                   Nullable(String),
    email_address               Nullable(String),
    date_of_birth               Nullable(Int32),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.clients',
    kafka_group_name         = 'clickhouse_fineract_clients',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 1,
    kafka_max_block_size     = 16384,
    kafka_poll_max_batch_size = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- loans
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_loans
(
    loan_id                     Nullable(Int64),
    account_no                  Nullable(String),
    external_id                 Nullable(String),
    client_id                   Nullable(Int64),
    client_name                 Nullable(String),
    group_id                    Nullable(Int64),
    product_id                  Nullable(Int64),
    product_name                Nullable(String),
    office_id                   Nullable(Int64),
    office_name                 Nullable(String),
    loan_officer_id             Nullable(Int64),
    loan_officer_name           Nullable(String),
    loan_type                   Nullable(String),
    currency_code               Nullable(String),
    currency_decimal_places     Nullable(Int32),
    status_id                   Nullable(Int32),
    status_code                 Nullable(String),
    status_value                Nullable(String),
    is_active                   Nullable(Bool),
    is_overpaid                 Nullable(Bool),
    is_closed                   Nullable(Bool),
    submitted_on_date           Nullable(Int32),
    approved_on_date            Nullable(Int32),
    disbursed_on_date           Nullable(Int32),
    expected_maturity_date      Nullable(Int32),
    closed_on_date              Nullable(Int32),
    term_frequency              Nullable(Int32),
    term_frequency_type         Nullable(String),
    number_of_repayments        Nullable(Int32),
    repayment_every             Nullable(Int32),
    repayment_frequency_type    Nullable(String),
    interest_rate_per_period    Nullable(String),
    annual_interest_rate        Nullable(String),
    principal                   Nullable(String),
    approved_principal          Nullable(String),
    principal_disbursed         Nullable(String),
    principal_paid              Nullable(String),
    principal_written_off       Nullable(String),
    principal_outstanding       Nullable(String),
    principal_overdue           Nullable(String),
    interest_charged            Nullable(String),
    interest_paid               Nullable(String),
    interest_waived             Nullable(String),
    interest_outstanding        Nullable(String),
    interest_overdue            Nullable(String),
    fee_charges_charged         Nullable(String),
    fee_charges_paid            Nullable(String),
    fee_charges_outstanding     Nullable(String),
    penalty_charges_charged     Nullable(String),
    penalty_charges_paid        Nullable(String),
    penalty_charges_outstanding Nullable(String),
    total_expected_repayment    Nullable(String),
    total_repayment             Nullable(String),
    total_outstanding           Nullable(String),
    total_overdue               Nullable(String),
    overdue_since_date          Nullable(Int32),
    delinquent_days             Nullable(Int32),
    delinquent_amount           Nullable(String),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.loans',
    kafka_group_name         = 'clickhouse_fineract_loans',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 1,
    kafka_max_block_size     = 16384,
    kafka_poll_max_batch_size = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- loan_transactions  (highest volume topic - 3 consumers)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_loan_transactions
(
    transaction_id              Nullable(Int64),
    loan_id                     Nullable(Int64),
    office_id                   Nullable(Int64),
    office_name                 Nullable(String),
    type_id                     Nullable(Int32),
    type_code                   Nullable(String),
    type_value                  Nullable(String),
    is_reversed                 Nullable(Bool),
    transaction_date            Nullable(Int32),
    submitted_on_date           Nullable(Int32),
    currency_code               Nullable(String),
    amount                      Nullable(String),
    net_disbursal_amount        Nullable(String),
    principal_portion           Nullable(String),
    interest_portion            Nullable(String),
    fee_charges_portion         Nullable(String),
    penalty_charges_portion     Nullable(String),
    overpayment_portion         Nullable(String),
    outstanding_loan_balance    Nullable(String),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.loan_transactions',
    kafka_group_name         = 'clickhouse_fineract_loan_transactions',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 3,
    kafka_max_block_size     = 65536,
    kafka_poll_max_batch_size = 16384,
    kafka_flush_interval_ms  = 3000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;

-- ---------------------------------------------------------------------
-- savings_accounts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.kafka_savings_accounts
(
    savings_id                   Nullable(Int64),
    account_no                   Nullable(String),
    client_id                    Nullable(Int64),
    client_name                  Nullable(String),
    product_id                   Nullable(Int64),
    product_name                 Nullable(String),
    office_id                    Nullable(Int64),
    field_officer_id             Nullable(Int64),
    status_id                    Nullable(Int32),
    status_value                 Nullable(String),
    is_active                    Nullable(Bool),
    currency_code                Nullable(String),
    nominal_annual_interest_rate Nullable(String),
    submitted_on_date            Nullable(Int32),
    activated_on_date            Nullable(Int32),
    closed_on_date               Nullable(Int32),
    account_balance              Nullable(String),
    available_balance            Nullable(String),
    total_deposits               Nullable(String),
    total_withdrawals            Nullable(String),
    total_interest_posted        Nullable(String),
    _ingested_at     Nullable(String),
    _updated_at      Nullable(String),
    _source_system   Nullable(String),
    _payload_hash    Nullable(String),
    __op             Nullable(String),
    __ts_ms          Nullable(Int64),
    __source_ts_ms   Nullable(Int64),
    __source_lsn     Nullable(Int64),
    __source_txId    Nullable(Int64),
    __source_table   Nullable(String),
    __deleted        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'fineract.oltp.savings_accounts',
    kafka_group_name         = 'clickhouse_fineract_savings_accounts',
    kafka_format             = 'JSONEachRow',
    kafka_max_block_size     = 8192,
    kafka_flush_interval_ms  = 2000,
    kafka_handle_error_mode  = 'stream',
    input_format_skip_unknown_fields = 1,
    input_format_null_as_default      = 1;
