-- =====================================================================
-- 03_raw_tables.sql - the CDC landing tables.
--
-- ENGINE CHOICE: ReplacingMergeTree(_version, _is_deleted)
-- --------------------------------------------------------
-- CDC delivery is at-least-once and out-of-order across partitions, so
-- the raw layer must be able to collapse N versions of a key into the
-- newest one.
--   _version    = source COMMIT time in ms (__source_ts_ms). Using the
--                 Debezium read time here would let a catching-up
--                 connector overwrite a newer row with an older one.
--   _is_deleted = 1 when __op = 'd'. Combined with the version column
--                 this turns a delete into a reconcilable fact rather
--                 than a ghost row.
--
-- Merges are asynchronous, so readers must not assume the table is
-- already collapsed. Staging models do the collapse explicitly with
-- argMax (not FINAL) - see transform/.../staging for why.
--
-- ORDER BY = the natural key, no surrogate. It is the primary index, the
-- merge key and the dedup key all at once; a synthetic key would need a
-- secondary lookup for every one of those.
--
-- PARTITIONING
-- ------------
-- Dimension-sized tables: no partitioning. Partitioning a 5k-row table
-- multiplies parts and slows merges for zero pruning benefit.
-- loan_transactions: PARTITION BY toYYYYMM(transaction_date). Safe here
-- because a Fineract transaction's date is immutable - a correction is a
-- new reversal transaction, never an edit of the date - so a row can
-- never need to move partitions, which would break dedup.
--
-- CODECS
-- ------
-- Monotonic-ish columns (ids, versions, LSNs, dates) get Delta+ZSTD;
-- low-cardinality strings become LowCardinality. On the demo dataset
-- this is roughly a 4-6x reduction over defaults on the fact table.
-- =====================================================================

-- ---------------------------------------------------------------------
-- offices
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.offices
(
    office_id        Int64,
    name             String,
    name_decorated   Nullable(String),
    external_id      Nullable(String),
    parent_id        Nullable(Int64),
    parent_name      Nullable(String),
    hierarchy        Nullable(String),
    opening_date     Nullable(Date),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY office_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- staff
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.staff
(
    staff_id         Int64,
    display_name     Nullable(String),
    firstname        Nullable(String),
    lastname         Nullable(String),
    office_id        Nullable(Int64),
    office_name      Nullable(String),
    mobile_no        Nullable(String),
    is_loan_officer  Nullable(UInt8),
    is_active        Nullable(UInt8),
    joining_date     Nullable(Date),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY staff_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- loan_products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.loan_products
(
    product_id                       Int64,
    name                             String,
    short_name                       Nullable(String),
    description                      Nullable(String),
    fund_name                        Nullable(String),
    currency_code                    LowCardinality(Nullable(String)),
    currency_decimal_places          Nullable(Int32),
    principal                        Nullable(Decimal(19, 6)),
    min_principal                    Nullable(Decimal(19, 6)),
    max_principal                    Nullable(Decimal(19, 6)),
    number_of_repayments             Nullable(Int32),
    repayment_every                  Nullable(Int32),
    repayment_frequency_type         LowCardinality(Nullable(String)),
    interest_rate_per_period         Nullable(Decimal(19, 6)),
    interest_rate_frequency_type     LowCardinality(Nullable(String)),
    annual_interest_rate             Nullable(Decimal(19, 6)),
    amortization_type                LowCardinality(Nullable(String)),
    interest_type                    LowCardinality(Nullable(String)),
    interest_calculation_period_type LowCardinality(Nullable(String)),
    status                           LowCardinality(Nullable(String)),
    start_date                       Nullable(Date),
    close_date                       Nullable(Date),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY product_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- savings_products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.savings_products
(
    product_id                       Int64,
    name                             String,
    short_name                       Nullable(String),
    description                      Nullable(String),
    currency_code                    LowCardinality(Nullable(String)),
    currency_decimal_places          Nullable(Int32),
    nominal_annual_interest_rate     Nullable(Decimal(19, 6)),
    interest_compounding_period_type LowCardinality(Nullable(String)),
    interest_posting_period_type     LowCardinality(Nullable(String)),
    min_required_opening_balance     Nullable(Decimal(19, 6)),
    status                           LowCardinality(Nullable(String)),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY product_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- clients
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.clients
(
    client_id                   Int64,
    account_no                  Nullable(String),
    external_id                 Nullable(String),
    status_id                   Nullable(Int32),
    status_code                 LowCardinality(Nullable(String)),
    status_value                LowCardinality(Nullable(String)),
    sub_status_value            LowCardinality(Nullable(String)),
    is_active                   Nullable(UInt8),
    activation_date             Nullable(Date),
    submitted_on_date           Nullable(Date),
    closed_on_date              Nullable(Date),
    office_id                   Nullable(Int64),
    office_name                 LowCardinality(Nullable(String)),
    staff_id                    Nullable(Int64),
    staff_name                  Nullable(String),
    legal_form_value            LowCardinality(Nullable(String)),
    gender_value                LowCardinality(Nullable(String)),
    client_type_value           LowCardinality(Nullable(String)),
    client_classification_value LowCardinality(Nullable(String)),
    firstname                   Nullable(String),
    lastname                    Nullable(String),
    display_name                Nullable(String),
    mobile_no                   Nullable(String),
    email_address               Nullable(String),
    date_of_birth               Nullable(Date),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY client_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- loans
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.loans
(
    loan_id                     Int64,
    account_no                  Nullable(String),
    external_id                 Nullable(String),
    client_id                   Nullable(Int64),
    client_name                 Nullable(String),
    group_id                    Nullable(Int64),
    product_id                  Nullable(Int64),
    product_name                LowCardinality(Nullable(String)),
    office_id                   Nullable(Int64),
    office_name                 LowCardinality(Nullable(String)),
    loan_officer_id             Nullable(Int64),
    loan_officer_name           Nullable(String),
    loan_type                   LowCardinality(Nullable(String)),
    currency_code               LowCardinality(Nullable(String)),
    currency_decimal_places     Nullable(Int32),
    status_id                   Nullable(Int32),
    status_code                 LowCardinality(Nullable(String)),
    status_value                LowCardinality(Nullable(String)),
    is_active                   Nullable(UInt8),
    is_overpaid                 Nullable(UInt8),
    is_closed                   Nullable(UInt8),
    submitted_on_date           Nullable(Date),
    approved_on_date            Nullable(Date),
    disbursed_on_date           Nullable(Date),
    expected_maturity_date      Nullable(Date),
    closed_on_date              Nullable(Date),
    term_frequency              Nullable(Int32),
    term_frequency_type         LowCardinality(Nullable(String)),
    number_of_repayments        Nullable(Int32),
    repayment_every             Nullable(Int32),
    repayment_frequency_type    LowCardinality(Nullable(String)),
    interest_rate_per_period    Nullable(Decimal(19, 6)),
    annual_interest_rate        Nullable(Decimal(19, 6)),
    principal                   Nullable(Decimal(19, 6)),
    approved_principal          Nullable(Decimal(19, 6)),
    principal_disbursed         Nullable(Decimal(19, 6)),
    principal_paid              Nullable(Decimal(19, 6)),
    principal_written_off       Nullable(Decimal(19, 6)),
    principal_outstanding       Nullable(Decimal(19, 6)),
    principal_overdue           Nullable(Decimal(19, 6)),
    interest_charged            Nullable(Decimal(19, 6)),
    interest_paid               Nullable(Decimal(19, 6)),
    interest_waived             Nullable(Decimal(19, 6)),
    interest_outstanding        Nullable(Decimal(19, 6)),
    interest_overdue            Nullable(Decimal(19, 6)),
    fee_charges_charged         Nullable(Decimal(19, 6)),
    fee_charges_paid            Nullable(Decimal(19, 6)),
    fee_charges_outstanding     Nullable(Decimal(19, 6)),
    penalty_charges_charged     Nullable(Decimal(19, 6)),
    penalty_charges_paid        Nullable(Decimal(19, 6)),
    penalty_charges_outstanding Nullable(Decimal(19, 6)),
    total_expected_repayment    Nullable(Decimal(19, 6)),
    total_repayment             Nullable(Decimal(19, 6)),
    total_outstanding           Nullable(Decimal(19, 6)),
    total_overdue               Nullable(Decimal(19, 6)),
    overdue_since_date          Nullable(Date),
    delinquent_days             Nullable(Int32),
    delinquent_amount           Nullable(Decimal(19, 6)),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY loan_id
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- loan_transactions  (the fact table - partitioned, highest volume)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.loan_transactions
(
    transaction_id              Int64,
    loan_id                     Int64,
    office_id                   Nullable(Int64),
    office_name                 LowCardinality(Nullable(String)),
    type_id                     Nullable(Int32),
    type_code                   LowCardinality(Nullable(String)),
    type_value                  LowCardinality(Nullable(String)),
    is_reversed                 Nullable(UInt8),
    transaction_date            Date,
    submitted_on_date           Nullable(Date),
    currency_code               LowCardinality(Nullable(String)),
    amount                      Nullable(Decimal(19, 6)),
    net_disbursal_amount        Nullable(Decimal(19, 6)),
    principal_portion           Nullable(Decimal(19, 6)),
    interest_portion            Nullable(Decimal(19, 6)),
    fee_charges_portion         Nullable(Decimal(19, 6)),
    penalty_charges_portion     Nullable(Decimal(19, 6)),
    overpayment_portion         Nullable(Decimal(19, 6)),
    outstanding_loan_balance    Nullable(Decimal(19, 6)),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, loan_id, transaction_id)
SETTINGS index_granularity = 8192;

-- Skip index: most operational queries filter on loan_id directly
-- ("show me this borrower's ledger"), which the leading transaction_date
-- in ORDER BY cannot prune. A bloom filter on loan_id restores that.
ALTER TABLE fineract_raw.loan_transactions
    ADD INDEX IF NOT EXISTS idx_loan_id loan_id TYPE bloom_filter(0.01) GRANULARITY 4;

-- ---------------------------------------------------------------------
-- savings_accounts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_raw.savings_accounts
(
    savings_id                   Int64,
    account_no                   Nullable(String),
    client_id                    Nullable(Int64),
    client_name                  Nullable(String),
    product_id                   Nullable(Int64),
    product_name                 LowCardinality(Nullable(String)),
    office_id                    Nullable(Int64),
    field_officer_id             Nullable(Int64),
    status_id                    Nullable(Int32),
    status_value                 LowCardinality(Nullable(String)),
    is_active                    Nullable(UInt8),
    currency_code                LowCardinality(Nullable(String)),
    nominal_annual_interest_rate Nullable(Decimal(19, 6)),
    submitted_on_date            Nullable(Date),
    activated_on_date            Nullable(Date),
    closed_on_date               Nullable(Date),
    account_balance              Nullable(Decimal(19, 6)),
    available_balance            Nullable(Decimal(19, 6)),
    total_deposits               Nullable(Decimal(19, 6)),
    total_withdrawals            Nullable(Decimal(19, 6)),
    total_interest_posted        Nullable(Decimal(19, 6)),
    source_ingested_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at  Nullable(DateTime64(3, 'UTC')),
    payload_hash     Nullable(String),

    _op               LowCardinality(String),
    _source_commit_at DateTime64(3, 'UTC'),
    _cdc_read_at      DateTime64(3, 'UTC'),
    _lsn              Int64 CODEC(Delta, ZSTD(1)),
    _tx_id            Int64 CODEC(Delta, ZSTD(1)),
    _version          UInt64 CODEC(Delta, ZSTD(1)),
    _is_deleted       UInt8 DEFAULT 0,
    _ch_inserted_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY savings_id
SETTINGS index_granularity = 8192;

-- =====================================================================
-- Poison-message quarantine.
--
-- Fed by the *_errors materialized views. A message that cannot be
-- parsed lands here with the raw bytes and the parser error instead of
-- stalling the consumer. `CDCParseErrors` in the Prometheus rules alerts
-- on any row appearing.
-- =====================================================================
CREATE TABLE IF NOT EXISTS fineract_raw.cdc_errors
(
    topic          LowCardinality(String),
    partition      Int64,
    offset         Int64,
    error          String,
    raw_message    String,
    observed_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (topic, observed_at)
TTL toDateTime(observed_at) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- =====================================================================
-- CDC audit stream.
--
-- Every change event, never collapsed - the raw tables keep only the
-- current version of a row, so without this there is no way to answer
-- "how many updates did we receive last hour" or to reconstruct history
-- after the fact. 90-day TTL keeps it bounded.
-- =====================================================================
CREATE TABLE IF NOT EXISTS fineract_raw.cdc_audit
(
    source_table      LowCardinality(String),
    primary_key       Int64,
    op                LowCardinality(String),
    source_commit_at  DateTime64(3, 'UTC'),
    cdc_read_at       DateTime64(3, 'UTC'),
    ch_inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3),
    lsn               Int64 CODEC(Delta, ZSTD(1)),
    lag_ms            Int64 MATERIALIZED
        dateDiff('millisecond', source_commit_at, ch_inserted_at)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(source_commit_at)
ORDER BY (source_table, source_commit_at, primary_key)
TTL toDateTime(source_commit_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
