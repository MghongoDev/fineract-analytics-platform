"""Declarative catalogue of the Fineract entities we ingest.

Adding a new source entity means adding one :class:`EntitySpec` here -
the client, loader, validator, CLI, metrics and Airflow DAG are all
driven off this registry. No new code paths, no new DAG task written by
hand, no divergence between "what we ingest" and "what we monitor".

Each spec declares
    * where the data comes from (``path`` / ``paged`` / ``params``)
    * how a raw JSON record maps to the target table (``mapper``)
    * how the entity is loaded incrementally (``mode``)
    * the quality expectations that gate the load (``expectations``)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional

from .parsers import (
    dig,
    enum_id,
    enum_value,
    parse_bool,
    parse_date,
    parse_decimal,
    parse_int,
    parse_text,
)

Record = dict[str, Any]
Mapper = Callable[[Mapping[str, Any]], Record]


@dataclass(frozen=True)
class Expectation:
    """A declarative data-quality assertion evaluated on the mapped batch."""

    name: str
    kind: str                       # not_null | unique | non_negative | range | freshness | row_count_min
    columns: tuple[str, ...] = ()
    severity: str = "error"         # error -> fails the run; warn -> logged + metric only
    min_value: Optional[float] = None
    max_value: Optional[float] = None


@dataclass(frozen=True)
class EntitySpec:
    name: str
    path: str
    table: str
    primary_key: str
    mapper: Mapper
    paged: bool = True
    params: Mapping[str, Any] = field(default_factory=dict)
    #: full     - re-read the whole collection each run (small dimensions)
    #: parent   - iterate parent ids and pull a child collection per parent
    mode: str = "full"
    parent_entity: Optional[str] = None
    parent_id_query: Optional[str] = None
    expectations: tuple[Expectation, ...] = ()
    description: str = ""


# =====================================================================
# Mappers - raw Fineract JSON -> target table columns
# =====================================================================
def map_office(r: Mapping[str, Any]) -> Record:
    return {
        "office_id": parse_int(r.get("id")),
        "name": parse_text(r.get("name")),
        "name_decorated": parse_text(r.get("nameDecorated")),
        "external_id": parse_text(r.get("externalId")),
        "parent_id": parse_int(r.get("parentId")),
        "parent_name": parse_text(r.get("parentName")),
        "hierarchy": parse_text(r.get("hierarchy")),
        "opening_date": parse_date(r.get("openingDate")),
    }


def map_staff(r: Mapping[str, Any]) -> Record:
    return {
        "staff_id": parse_int(r.get("id")),
        "display_name": parse_text(r.get("displayName")),
        "firstname": parse_text(r.get("firstname")),
        "lastname": parse_text(r.get("lastname")),
        "office_id": parse_int(r.get("officeId")),
        "office_name": parse_text(r.get("officeName")),
        "mobile_no": parse_text(r.get("mobileNo")),
        "is_loan_officer": parse_bool(r.get("isLoanOfficer")),
        "is_active": parse_bool(r.get("isActive")),
        "joining_date": parse_date(r.get("joiningDate")),
    }


def map_loan_product(r: Mapping[str, Any]) -> Record:
    return {
        "product_id": parse_int(r.get("id")),
        "name": parse_text(r.get("name")),
        "short_name": parse_text(r.get("shortName")),
        "description": parse_text(r.get("description")),
        "fund_name": parse_text(r.get("fundName")),
        "currency_code": parse_text(dig(r, "currency", "code")),
        "currency_decimal_places": parse_int(dig(r, "currency", "decimalPlaces")),
        "principal": parse_decimal(r.get("principal")),
        "min_principal": parse_decimal(r.get("minPrincipal")),
        "max_principal": parse_decimal(r.get("maxPrincipal")),
        "number_of_repayments": parse_int(r.get("numberOfRepayments")),
        "repayment_every": parse_int(r.get("repaymentEvery")),
        "repayment_frequency_type": enum_value(r, "repaymentFrequencyType"),
        "interest_rate_per_period": parse_decimal(r.get("interestRatePerPeriod")),
        "interest_rate_frequency_type": enum_value(r, "interestRateFrequencyType"),
        "annual_interest_rate": parse_decimal(r.get("annualInterestRate")),
        "amortization_type": enum_value(r, "amortizationType"),
        "interest_type": enum_value(r, "interestType"),
        "interest_calculation_period_type": enum_value(r, "interestCalculationPeriodType"),
        "status": parse_text(r.get("status")),
        "start_date": parse_date(r.get("startDate")),
        "close_date": parse_date(r.get("closeDate")),
    }


def map_savings_product(r: Mapping[str, Any]) -> Record:
    return {
        "product_id": parse_int(r.get("id")),
        "name": parse_text(r.get("name")),
        "short_name": parse_text(r.get("shortName")),
        "description": parse_text(r.get("description")),
        "currency_code": parse_text(dig(r, "currency", "code")),
        "currency_decimal_places": parse_int(dig(r, "currency", "decimalPlaces")),
        "nominal_annual_interest_rate": parse_decimal(r.get("nominalAnnualInterestRate")),
        "interest_compounding_period_type": enum_value(r, "interestCompoundingPeriodType"),
        "interest_posting_period_type": enum_value(r, "interestPostingPeriodType"),
        "min_required_opening_balance": parse_decimal(r.get("minRequiredOpeningBalance")),
        "status": parse_text(r.get("status")),
    }


def map_client(r: Mapping[str, Any]) -> Record:
    return {
        "client_id": parse_int(r.get("id")),
        "account_no": parse_text(r.get("accountNo")),
        "external_id": parse_text(r.get("externalId")),
        "status_id": enum_id(r, "status"),
        "status_code": parse_text(dig(r, "status", "code")),
        "status_value": enum_value(r, "status"),
        "sub_status_value": enum_value(r, "subStatus"),
        "is_active": parse_bool(r.get("active")),
        "activation_date": parse_date(r.get("activationDate")),
        "submitted_on_date": parse_date(dig(r, "timeline", "submittedOnDate")),
        "closed_on_date": parse_date(dig(r, "timeline", "closedOnDate")),
        "office_id": parse_int(r.get("officeId")),
        "office_name": parse_text(r.get("officeName")),
        "staff_id": parse_int(r.get("staffId")),
        "staff_name": parse_text(r.get("staffName")),
        "legal_form_value": enum_value(r, "legalForm"),
        "gender_value": enum_value(r, "gender"),
        "client_type_value": enum_value(r, "clientType"),
        "client_classification_value": enum_value(r, "clientClassification"),
        "firstname": parse_text(r.get("firstname")),
        "lastname": parse_text(r.get("lastname")),
        "display_name": parse_text(r.get("displayName")),
        "mobile_no": parse_text(r.get("mobileNo")),
        "email_address": parse_text(r.get("emailAddress")),
        "date_of_birth": parse_date(r.get("dateOfBirth")),
    }


def map_loan(r: Mapping[str, Any]) -> Record:
    summary = r.get("summary") or {}
    timeline = r.get("timeline") or {}
    delinquent = r.get("delinquent") or {}
    status = r.get("status") or {}
    return {
        "loan_id": parse_int(r.get("id")),
        "account_no": parse_text(r.get("accountNo")),
        "external_id": parse_text(r.get("externalId")),
        "client_id": parse_int(r.get("clientId")),
        "client_name": parse_text(r.get("clientName")),
        "group_id": parse_int(r.get("groupId")),
        "product_id": parse_int(r.get("loanProductId")),
        "product_name": parse_text(r.get("loanProductName")),
        "office_id": parse_int(r.get("officeId")),
        "office_name": parse_text(r.get("officeName")),
        "loan_officer_id": parse_int(r.get("loanOfficerId")),
        "loan_officer_name": parse_text(r.get("loanOfficerName")),
        "loan_type": enum_value(r, "loanType"),
        "currency_code": parse_text(dig(r, "currency", "code")),
        "currency_decimal_places": parse_int(dig(r, "currency", "decimalPlaces")),
        "status_id": parse_int(status.get("id")),
        "status_code": parse_text(status.get("code")),
        "status_value": parse_text(status.get("value")),
        "is_active": parse_bool(status.get("active")),
        "is_overpaid": parse_bool(status.get("overpaid")),
        "is_closed": parse_bool(status.get("closed")),
        "submitted_on_date": parse_date(timeline.get("submittedOnDate")),
        "approved_on_date": parse_date(timeline.get("approvedOnDate")),
        "disbursed_on_date": parse_date(timeline.get("actualDisbursementDate")
                                        or timeline.get("expectedDisbursementDate")),
        "expected_maturity_date": parse_date(timeline.get("expectedMaturityDate")),
        "closed_on_date": parse_date(timeline.get("closedOnDate")
                                     or timeline.get("actualMaturityDate")),
        "term_frequency": parse_int(r.get("termFrequency")),
        "term_frequency_type": enum_value(r, "termPeriodFrequencyType"),
        "number_of_repayments": parse_int(r.get("numberOfRepayments")),
        "repayment_every": parse_int(r.get("repaymentEvery")),
        "repayment_frequency_type": enum_value(r, "repaymentFrequencyType"),
        "interest_rate_per_period": parse_decimal(r.get("interestRatePerPeriod")),
        "annual_interest_rate": parse_decimal(r.get("annualInterestRate")),
        "principal": parse_decimal(r.get("principal")),
        "approved_principal": parse_decimal(r.get("approvedPrincipal")),
        "principal_disbursed": parse_decimal(summary.get("principalDisbursed")),
        "principal_paid": parse_decimal(summary.get("principalPaid")),
        "principal_written_off": parse_decimal(summary.get("principalWrittenOff")),
        "principal_outstanding": parse_decimal(summary.get("principalOutstanding")),
        "principal_overdue": parse_decimal(summary.get("principalOverdue")),
        "interest_charged": parse_decimal(summary.get("interestCharged")),
        "interest_paid": parse_decimal(summary.get("interestPaid")),
        "interest_waived": parse_decimal(summary.get("interestWaived")),
        "interest_outstanding": parse_decimal(summary.get("interestOutstanding")),
        "interest_overdue": parse_decimal(summary.get("interestOverdue")),
        "fee_charges_charged": parse_decimal(summary.get("feeChargesCharged")),
        "fee_charges_paid": parse_decimal(summary.get("feeChargesPaid")),
        "fee_charges_outstanding": parse_decimal(summary.get("feeChargesOutstanding")),
        "penalty_charges_charged": parse_decimal(summary.get("penaltyChargesCharged")),
        "penalty_charges_paid": parse_decimal(summary.get("penaltyChargesPaid")),
        "penalty_charges_outstanding": parse_decimal(summary.get("penaltyChargesOutstanding")),
        "total_expected_repayment": parse_decimal(summary.get("totalExpectedRepayment")),
        "total_repayment": parse_decimal(summary.get("totalRepayment")),
        "total_outstanding": parse_decimal(summary.get("totalOutstanding")),
        "total_overdue": parse_decimal(summary.get("totalOverdue")),
        "overdue_since_date": parse_date(summary.get("overdueSinceDate")),
        "delinquent_days": parse_int(delinquent.get("pastDueDays")
                                     or delinquent.get("delinquentDays")),
        "delinquent_amount": parse_decimal(delinquent.get("delinquentAmount")),
    }


def map_loan_transaction(r: Mapping[str, Any]) -> Record:
    tx_type = r.get("type") or {}
    return {
        "transaction_id": parse_int(r.get("id")),
        "loan_id": parse_int(r.get("loanId") or r.get("_loan_id")),
        "office_id": parse_int(r.get("officeId")),
        "office_name": parse_text(r.get("officeName")),
        "type_id": parse_int(tx_type.get("id")),
        "type_code": parse_text(tx_type.get("code")),
        "type_value": parse_text(tx_type.get("value")),
        "is_reversed": parse_bool(r.get("manuallyReversed")) or False,
        "transaction_date": parse_date(r.get("date")),
        "submitted_on_date": parse_date(r.get("submittedOnDate")),
        "currency_code": parse_text(dig(r, "currency", "code")),
        "amount": parse_decimal(r.get("amount")),
        "net_disbursal_amount": parse_decimal(r.get("netDisbursalAmount")),
        "principal_portion": parse_decimal(r.get("principalPortion")),
        "interest_portion": parse_decimal(r.get("interestPortion")),
        "fee_charges_portion": parse_decimal(r.get("feeChargesPortion")),
        "penalty_charges_portion": parse_decimal(r.get("penaltyChargesPortion")),
        "overpayment_portion": parse_decimal(r.get("overpaymentPortion")),
        "outstanding_loan_balance": parse_decimal(r.get("outstandingLoanBalance")),
    }


def map_savings_account(r: Mapping[str, Any]) -> Record:
    summary = r.get("summary") or {}
    status = r.get("status") or {}
    timeline = r.get("timeline") or {}
    return {
        "savings_id": parse_int(r.get("id")),
        "account_no": parse_text(r.get("accountNo")),
        "client_id": parse_int(r.get("clientId")),
        "client_name": parse_text(r.get("clientName")),
        "product_id": parse_int(r.get("savingsProductId")),
        "product_name": parse_text(r.get("savingsProductName")),
        "office_id": parse_int(r.get("officeId")),
        "field_officer_id": parse_int(r.get("fieldOfficerId")),
        "status_id": parse_int(status.get("id")),
        "status_value": parse_text(status.get("value")),
        "is_active": parse_bool(status.get("active")),
        "currency_code": parse_text(dig(r, "currency", "code")),
        "nominal_annual_interest_rate": parse_decimal(r.get("nominalAnnualInterestRate")),
        "submitted_on_date": parse_date(timeline.get("submittedOnDate")),
        "activated_on_date": parse_date(timeline.get("activatedOnDate")),
        "closed_on_date": parse_date(timeline.get("closedOnDate")),
        "account_balance": parse_decimal(summary.get("accountBalance")),
        "available_balance": parse_decimal(summary.get("availableBalance")),
        "total_deposits": parse_decimal(summary.get("totalDeposits")),
        "total_withdrawals": parse_decimal(summary.get("totalWithdrawals")),
        "total_interest_posted": parse_decimal(summary.get("totalInterestPosted")),
    }


# =====================================================================
# Registry
# =====================================================================
ENTITIES: dict[str, EntitySpec] = {
    "offices": EntitySpec(
        name="offices", path="offices", table="oltp.offices",
        primary_key="office_id", mapper=map_office, paged=False,
        description="Branch hierarchy.",
        expectations=(
            Expectation("office_id_not_null", "not_null", ("office_id",)),
            Expectation("office_id_unique", "unique", ("office_id",)),
            Expectation("has_head_office", "row_count_min", min_value=1),
        ),
    ),
    "staff": EntitySpec(
        name="staff", path="staff", table="oltp.staff",
        primary_key="staff_id", mapper=map_staff, paged=False,
        description="Loan officers and branch staff.",
        expectations=(
            Expectation("staff_id_not_null", "not_null", ("staff_id",)),
            Expectation("staff_id_unique", "unique", ("staff_id",)),
        ),
    ),
    "loan_products": EntitySpec(
        name="loan_products", path="loanproducts", table="oltp.loan_products",
        primary_key="product_id", mapper=map_loan_product, paged=False,
        description="Loan product catalogue.",
        expectations=(
            Expectation("product_id_not_null", "not_null", ("product_id",)),
            Expectation("product_id_unique", "unique", ("product_id",)),
            Expectation("principal_non_negative", "non_negative", ("principal",), "warn"),
        ),
    ),
    "savings_products": EntitySpec(
        name="savings_products", path="savingsproducts", table="oltp.savings_products",
        primary_key="product_id", mapper=map_savings_product, paged=False,
        description="Savings product catalogue.",
        expectations=(
            Expectation("product_id_not_null", "not_null", ("product_id",)),
            Expectation("product_id_unique", "unique", ("product_id",)),
        ),
    ),
    "clients": EntitySpec(
        name="clients", path="clients", table="oltp.clients",
        primary_key="client_id", mapper=map_client, paged=True,
        description="Borrower / member master data.",
        expectations=(
            Expectation("client_id_not_null", "not_null", ("client_id",)),
            Expectation("client_id_unique", "unique", ("client_id",)),
            Expectation("office_id_not_null", "not_null", ("office_id",), "warn"),
        ),
    ),
    "loans": EntitySpec(
        name="loans", path="loans", table="oltp.loans",
        primary_key="loan_id", mapper=map_loan, paged=True,
        description="Loan accounts with summary balances.",
        expectations=(
            Expectation("loan_id_not_null", "not_null", ("loan_id",)),
            Expectation("loan_id_unique", "unique", ("loan_id",)),
            Expectation("client_id_not_null", "not_null", ("client_id",), "warn"),
            Expectation("principal_non_negative", "non_negative", ("principal",)),
            Expectation("outstanding_non_negative", "non_negative",
                        ("principal_outstanding", "total_outstanding"), "warn"),
            Expectation("interest_rate_sane", "range", ("annual_interest_rate",),
                        "warn", min_value=0, max_value=200),
        ),
    ),
    "savings_accounts": EntitySpec(
        name="savings_accounts", path="savingsaccounts", table="oltp.savings_accounts",
        primary_key="savings_id", mapper=map_savings_account, paged=True,
        description="Deposit accounts.",
        expectations=(
            Expectation("savings_id_not_null", "not_null", ("savings_id",)),
            Expectation("savings_id_unique", "unique", ("savings_id",)),
        ),
    ),
    # Child collection: Fineract exposes transactions only under a loan.
    # `parent_id_query` is the source of parent ids, read from what we
    # already landed - so the crawl never needs the whole book in memory.
    "loan_transactions": EntitySpec(
        name="loan_transactions", path="loans/{parent_id}/transactions",
        table="oltp.loan_transactions", primary_key="transaction_id",
        mapper=map_loan_transaction, paged=False, mode="parent",
        parent_entity="loans",
        parent_id_query=(
            "SELECT loan_id FROM oltp.loans "
            "WHERE disbursed_on_date IS NOT NULL "
            "ORDER BY coalesce(_updated_at, _ingested_at) DESC"
        ),
        description="Repayment / disbursal ledger per loan.",
        expectations=(
            Expectation("transaction_id_not_null", "not_null", ("transaction_id",)),
            Expectation("transaction_id_unique", "unique", ("transaction_id",)),
            Expectation("loan_id_not_null", "not_null", ("loan_id",)),
            Expectation("amount_non_negative", "non_negative", ("amount",), "warn"),
        ),
    ),
}

#: Load order. Parents before children, dimensions before facts, so that
#: referential expectations can be evaluated meaningfully at each step.
DEFAULT_ORDER: tuple[str, ...] = (
    "offices",
    "staff",
    "loan_products",
    "savings_products",
    "clients",
    "loans",
    "savings_accounts",
    "loan_transactions",
)


def get_entity(name: str) -> EntitySpec:
    try:
        return ENTITIES[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown entity '{name}'. Known: {', '.join(sorted(ENTITIES))}") from exc
