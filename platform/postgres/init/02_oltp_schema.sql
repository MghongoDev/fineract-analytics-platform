-- =====================================================================
-- 02_oltp_schema.sql
-- OLTP landing model for data pulled from the Apache Fineract REST API.
--
-- DESIGN NOTES
-- ------------
-- * This is a *system of record mirror*, not a warehouse model. It stays
--   deliberately close to the Fineract API resource shape so that the
--   ingestion layer performs no business logic - all reshaping happens
--   later in dbt on ClickHouse. Ingestion is therefore replayable and
--   cheap to reason about.
-- * Every table carries the same audit trailer (`_ingested_at`,
--   `_updated_at`, `_source_system`, `_payload_hash`). `_payload_hash`
--   lets the loader skip no-op updates, which keeps the WAL - and hence
--   the CDC stream - free of churn that carries no information.
-- * Natural keys from Fineract are used as primary keys. Fineract ids are
--   stable per tenant, so this gives idempotent upserts without a
--   surrogate-key lookup.
-- * REPLICA IDENTITY:
--     - dimension-like, low-volume tables -> FULL, so Debezium emits a
--       complete `before` image on UPDATE/DELETE. That makes downstream
--       change analysis and late-arriving corrections trivial.
--     - loan_transactions (highest volume, append-mostly) -> DEFAULT
--       (primary key only), because FULL would roughly double WAL volume
--       for no analytical benefit on an append-only stream.
-- =====================================================================

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS oltp;
CREATE SCHEMA IF NOT EXISTS meta;

COMMENT ON SCHEMA oltp IS 'Landed mirror of the Apache Fineract REST API (system of record).';
COMMENT ON SCHEMA meta IS 'Pipeline control plane: watermarks, run history, rejects.';

-- ---------------------------------------------------------------------
-- Shared audit trailer applied to every landed table.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION oltp.set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW._updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =====================================================================
-- REFERENCE / DIMENSION ENTITIES
-- =====================================================================

CREATE TABLE IF NOT EXISTS oltp.offices (
    office_id           BIGINT       PRIMARY KEY,
    name                TEXT         NOT NULL,
    name_decorated      TEXT,
    external_id         TEXT,
    parent_id           BIGINT,
    parent_name         TEXT,
    hierarchy           TEXT,
    opening_date        DATE,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.offices IS 'GET /offices - branch hierarchy.';

CREATE TABLE IF NOT EXISTS oltp.staff (
    staff_id            BIGINT       PRIMARY KEY,
    display_name        TEXT,
    firstname           TEXT,
    lastname            TEXT,
    office_id           BIGINT,
    office_name         TEXT,
    mobile_no           TEXT,
    is_loan_officer     BOOLEAN,
    is_active           BOOLEAN,
    joining_date        DATE,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.staff IS 'GET /staff - loan officers and branch staff.';

CREATE TABLE IF NOT EXISTS oltp.loan_products (
    product_id                  BIGINT      PRIMARY KEY,
    name                        TEXT        NOT NULL,
    short_name                  TEXT,
    description                 TEXT,
    fund_name                   TEXT,
    currency_code               TEXT,
    currency_decimal_places     INTEGER,
    principal                   NUMERIC(19,6),
    min_principal               NUMERIC(19,6),
    max_principal               NUMERIC(19,6),
    number_of_repayments        INTEGER,
    repayment_every             INTEGER,
    repayment_frequency_type    TEXT,
    interest_rate_per_period    NUMERIC(19,6),
    interest_rate_frequency_type TEXT,
    annual_interest_rate        NUMERIC(19,6),
    amortization_type           TEXT,
    interest_type               TEXT,
    interest_calculation_period_type TEXT,
    status                      TEXT,
    start_date                  DATE,
    close_date                  DATE,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.loan_products IS 'GET /loanproducts - product catalogue.';

CREATE TABLE IF NOT EXISTS oltp.savings_products (
    product_id                  BIGINT      PRIMARY KEY,
    name                        TEXT        NOT NULL,
    short_name                  TEXT,
    description                 TEXT,
    currency_code               TEXT,
    currency_decimal_places     INTEGER,
    nominal_annual_interest_rate NUMERIC(19,6),
    interest_compounding_period_type TEXT,
    interest_posting_period_type TEXT,
    min_required_opening_balance NUMERIC(19,6),
    status                      TEXT,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.savings_products IS 'GET /savingsproducts - savings catalogue.';

-- =====================================================================
-- CORE BUSINESS ENTITIES
-- =====================================================================

CREATE TABLE IF NOT EXISTS oltp.clients (
    client_id           BIGINT       PRIMARY KEY,
    account_no          TEXT,
    external_id         TEXT,
    status_id           INTEGER,
    status_code         TEXT,
    status_value        TEXT,
    sub_status_value    TEXT,
    is_active           BOOLEAN,
    activation_date     DATE,
    submitted_on_date   DATE,
    closed_on_date      DATE,
    office_id           BIGINT,
    office_name         TEXT,
    staff_id            BIGINT,
    staff_name          TEXT,
    legal_form_value    TEXT,
    gender_value        TEXT,
    client_type_value   TEXT,
    client_classification_value TEXT,
    firstname           TEXT,
    lastname            TEXT,
    display_name        TEXT,
    mobile_no           TEXT,
    email_address       TEXT,
    date_of_birth       DATE,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.clients IS 'GET /clients?paged=true - borrower/member master.';

CREATE INDEX IF NOT EXISTS ix_clients_office     ON oltp.clients (office_id);
CREATE INDEX IF NOT EXISTS ix_clients_updated_at ON oltp.clients (_updated_at);

CREATE TABLE IF NOT EXISTS oltp.loans (
    loan_id                     BIGINT      PRIMARY KEY,
    account_no                  TEXT,
    external_id                 TEXT,
    client_id                   BIGINT,
    client_name                 TEXT,
    group_id                    BIGINT,
    product_id                  BIGINT,
    product_name                TEXT,
    office_id                   BIGINT,
    office_name                 TEXT,
    loan_officer_id             BIGINT,
    loan_officer_name           TEXT,
    loan_type                   TEXT,
    currency_code               TEXT,
    currency_decimal_places     INTEGER,
    status_id                   INTEGER,
    status_code                 TEXT,
    status_value                TEXT,
    is_active                   BOOLEAN,
    is_overpaid                 BOOLEAN,
    is_closed                   BOOLEAN,
    submitted_on_date           DATE,
    approved_on_date            DATE,
    disbursed_on_date           DATE,
    expected_maturity_date      DATE,
    closed_on_date              DATE,
    term_frequency              INTEGER,
    term_frequency_type         TEXT,
    number_of_repayments        INTEGER,
    repayment_every             INTEGER,
    repayment_frequency_type    TEXT,
    interest_rate_per_period    NUMERIC(19,6),
    annual_interest_rate        NUMERIC(19,6),
    principal                   NUMERIC(19,6),
    approved_principal          NUMERIC(19,6),
    principal_disbursed         NUMERIC(19,6),
    principal_paid              NUMERIC(19,6),
    principal_written_off       NUMERIC(19,6),
    principal_outstanding       NUMERIC(19,6),
    principal_overdue           NUMERIC(19,6),
    interest_charged            NUMERIC(19,6),
    interest_paid               NUMERIC(19,6),
    interest_waived             NUMERIC(19,6),
    interest_outstanding        NUMERIC(19,6),
    interest_overdue            NUMERIC(19,6),
    fee_charges_charged         NUMERIC(19,6),
    fee_charges_paid            NUMERIC(19,6),
    fee_charges_outstanding     NUMERIC(19,6),
    penalty_charges_charged     NUMERIC(19,6),
    penalty_charges_paid        NUMERIC(19,6),
    penalty_charges_outstanding NUMERIC(19,6),
    total_expected_repayment    NUMERIC(19,6),
    total_repayment             NUMERIC(19,6),
    total_outstanding           NUMERIC(19,6),
    total_overdue               NUMERIC(19,6),
    overdue_since_date          DATE,
    delinquent_days             INTEGER,
    delinquent_amount           NUMERIC(19,6),
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.loans IS 'GET /loans?paged=true - loan accounts with summary balances.';

CREATE INDEX IF NOT EXISTS ix_loans_client      ON oltp.loans (client_id);
CREATE INDEX IF NOT EXISTS ix_loans_product     ON oltp.loans (product_id);
CREATE INDEX IF NOT EXISTS ix_loans_office      ON oltp.loans (office_id);
CREATE INDEX IF NOT EXISTS ix_loans_disbursed   ON oltp.loans (disbursed_on_date);
CREATE INDEX IF NOT EXISTS ix_loans_updated_at  ON oltp.loans (_updated_at);

CREATE TABLE IF NOT EXISTS oltp.loan_transactions (
    transaction_id              BIGINT      PRIMARY KEY,
    loan_id                     BIGINT      NOT NULL,
    office_id                   BIGINT,
    office_name                 TEXT,
    type_id                     INTEGER,
    type_code                   TEXT,
    type_value                  TEXT,
    is_reversed                 BOOLEAN     DEFAULT false,
    transaction_date            DATE,
    submitted_on_date           DATE,
    currency_code               TEXT,
    amount                      NUMERIC(19,6),
    net_disbursal_amount        NUMERIC(19,6),
    principal_portion           NUMERIC(19,6),
    interest_portion            NUMERIC(19,6),
    fee_charges_portion         NUMERIC(19,6),
    penalty_charges_portion     NUMERIC(19,6),
    overpayment_portion         NUMERIC(19,6),
    outstanding_loan_balance    NUMERIC(19,6),
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.loan_transactions IS 'GET /loans/{id}/transactions - repayment/disbursal ledger.';

CREATE INDEX IF NOT EXISTS ix_loan_tx_loan  ON oltp.loan_transactions (loan_id);
CREATE INDEX IF NOT EXISTS ix_loan_tx_date  ON oltp.loan_transactions (transaction_date);

CREATE TABLE IF NOT EXISTS oltp.savings_accounts (
    savings_id                  BIGINT      PRIMARY KEY,
    account_no                  TEXT,
    client_id                   BIGINT,
    client_name                 TEXT,
    product_id                  BIGINT,
    product_name                TEXT,
    office_id                   BIGINT,
    field_officer_id            BIGINT,
    status_id                   INTEGER,
    status_value                TEXT,
    is_active                   BOOLEAN,
    currency_code               TEXT,
    nominal_annual_interest_rate NUMERIC(19,6),
    submitted_on_date           DATE,
    activated_on_date           DATE,
    closed_on_date              DATE,
    account_balance             NUMERIC(19,6),
    available_balance           NUMERIC(19,6),
    total_deposits              NUMERIC(19,6),
    total_withdrawals           NUMERIC(19,6),
    total_interest_posted       NUMERIC(19,6),
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source_system      TEXT         NOT NULL DEFAULT 'fineract',
    _payload_hash       TEXT         NOT NULL
);
COMMENT ON TABLE oltp.savings_accounts IS 'GET /savingsaccounts?paged=true - deposit accounts.';

CREATE INDEX IF NOT EXISTS ix_savings_client ON oltp.savings_accounts (client_id);

-- ---------------------------------------------------------------------
-- Keep `_updated_at` honest without asking the loader to maintain it.
-- ---------------------------------------------------------------------
DO $$
DECLARE
  t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'offices','staff','loan_products','savings_products',
    'clients','loans','loan_transactions','savings_accounts'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_updated_at ON oltp.%1$s', t);
    EXECUTE format(
      'CREATE TRIGGER trg_%1$s_updated_at BEFORE UPDATE ON oltp.%1$s
         FOR EACH ROW EXECUTE FUNCTION oltp.set_updated_at()', t);
  END LOOP;
END
$$;
