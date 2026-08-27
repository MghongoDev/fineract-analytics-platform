"""A deterministic, dependency-free stand-in for the Fineract v1 API.

Why this exists
---------------
Two problems with pointing CI (or a reviewer behind a firewall) at
``demo.mifos.io``: it is a shared mutable environment, and it may simply
be unreachable. A test that depends on someone else's demo database is
not a test - it is a coin flip.

This module serves the *same resource shapes* as Fineract (including the
``[yyyy, m, d]`` date arrays and the ``pageItems`` envelope) from a
seeded generator, so:

* CI runs the real ingestion code end to end, offline and deterministically
* ``docker compose --profile mock up`` gives a reviewer a working stack
  with no external dependency at all

Run it standalone:

    python -m fineract_ingest.mock_server --port 8090 --clients 500 --loans 900
"""

from __future__ import annotations

import argparse
import json
import random
import re
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

OFFICE_NAMES = ["Head Office", "Nairobi CBD", "Kisumu", "Mombasa", "Nakuru", "Eldoret"]
PRODUCT_NAMES = ["Biashara Boost", "Kilimo Loan", "Boda Asset Finance",
                 "Working Capital 90", "Stawi Micro"]
FIRST_NAMES = ["Amina", "Brian", "Cynthia", "David", "Esther", "Felix", "Grace",
               "Hassan", "Irene", "James", "Kevin", "Lucy", "Moses", "Nancy"]
LAST_NAMES = ["Achieng", "Barasa", "Chebet", "Dube", "Etyang", "Gitau", "Hussein",
              "Juma", "Kamau", "Lagat", "Mwangi", "Njoroge", "Otieno", "Wanjiru"]
LOAN_STATUSES = [
    (100, "loanStatusType.submitted.and.pending.approval", "Submitted and pending approval"),
    (200, "loanStatusType.approved", "Approved"),
    (300, "loanStatusType.active", "Active"),
    (600, "loanStatusType.closed.obligations.met", "Closed (obligations met)"),
    (601, "loanStatusType.closed.written.off", "Closed (written off)"),
    (700, "loanStatusType.overpaid", "Overpaid"),
]
TX_TYPES = [
    (1, "loanTransactionType.disbursement", "Disbursement"),
    (2, "loanTransactionType.repayment", "Repayment"),
    (4, "loanTransactionType.accrual", "Accrual"),
    (6, "loanTransactionType.waiveInterest", "Waive interest"),
]


def _d(value: date) -> list[int]:
    """Fineract encodes dates as [yyyy, m, d]."""
    return [value.year, value.month, value.day]


class FineractDataset:
    """Seeded, in-memory dataset shaped exactly like the Fineract API."""

    def __init__(self, clients: int = 400, loans: int = 700, seed: int = 42,
                 as_of: date | None = None):
        self.random = random.Random(seed)
        self.as_of = as_of or date(2026, 8, 11)
        self.offices = self._build_offices()
        self.staff = self._build_staff()
        self.loan_products = self._build_loan_products()
        self.savings_products = self._build_savings_products()
        self.clients = self._build_clients(clients)
        self.loans = self._build_loans(loans)
        self.savings = self._build_savings(max(1, clients // 3))
        self.transactions = self._build_transactions()

    # -- reference ----------------------------------------------------
    def _build_offices(self) -> list[dict]:
        rows = []
        for index, name in enumerate(OFFICE_NAMES, start=1):
            rows.append({
                "id": index, "name": name,
                "nameDecorated": name if index == 1 else f"....{name}",
                "externalId": f"OFF-{index:03d}",
                "parentId": None if index == 1 else 1,
                "parentName": None if index == 1 else OFFICE_NAMES[0],
                "hierarchy": "." if index == 1 else f".{index}.",
                "openingDate": _d(date(2015, 1, 1) + timedelta(days=180 * index)),
            })
        return rows

    def _build_staff(self) -> list[dict]:
        rows = []
        for index in range(1, 19):
            first = self.random.choice(FIRST_NAMES)
            last = self.random.choice(LAST_NAMES)
            office = self.random.choice(self.offices)
            rows.append({
                "id": index, "firstname": first, "lastname": last,
                "displayName": f"{last}, {first}",
                "officeId": office["id"], "officeName": office["name"],
                "mobileNo": f"+2547{self.random.randint(10_000_000, 99_999_999)}",
                "isLoanOfficer": index % 4 != 0, "isActive": index % 11 != 0,
                "joiningDate": _d(date(2018, 1, 1) + timedelta(days=97 * index)),
            })
        return rows

    def _build_loan_products(self) -> list[dict]:
        rows = []
        for index, name in enumerate(PRODUCT_NAMES, start=1):
            rate = round(self.random.uniform(1.2, 3.8), 2)
            rows.append({
                "id": index, "name": name,
                "shortName": "".join(w[0] for w in name.split())[:4].upper(),
                "description": f"{name} micro-enterprise product",
                "fundName": "Inkomoko Fund" if index % 2 else None,
                "currency": {"code": "KES", "decimalPlaces": 2, "name": "Kenyan Shilling"},
                "principal": 50_000 * index,
                "minPrincipal": 10_000, "maxPrincipal": 500_000 * index,
                "numberOfRepayments": self.random.choice([6, 9, 12, 18, 24]),
                "repaymentEvery": 1,
                "repaymentFrequencyType": {
                    "id": 2,
                    "code": "repaymentFrequency.periodFrequencyType.months",
                    "value": "Months"},
                "interestRatePerPeriod": rate,
                "interestRateFrequencyType": {"id": 2, "value": "Per month"},
                "annualInterestRate": round(rate * 12, 2),
                "amortizationType": {"id": 1, "value": "Equal installments"},
                "interestType": {"id": 0, "value": "Declining Balance"},
                "interestCalculationPeriodType": {"id": 1, "value": "Same as repayment period"},
                "status": "loanProduct.active",
                "startDate": _d(date(2019, 6, 1)),
            })
        return rows

    def _build_savings_products(self) -> list[dict]:
        return [{
            "id": index, "name": name,
            "shortName": name[:4].upper(),
            "description": f"{name} deposit product",
            "currency": {"code": "KES", "decimalPlaces": 2},
            "nominalAnnualInterestRate": round(self.random.uniform(2.0, 6.5), 2),
            "interestCompoundingPeriodType": {"id": 1, "value": "Daily"},
            "interestPostingPeriodType": {"id": 4, "value": "Monthly"},
            "minRequiredOpeningBalance": 500,
            "status": "savingsProduct.active",
        } for index, name in enumerate(
            ["Akiba Savings", "Business Current", "Group Fund"], start=1)]

    # -- core ---------------------------------------------------------
    def _build_clients(self, count: int) -> list[dict]:
        rows = []
        for index in range(1, count + 1):
            office = self.random.choice(self.offices)
            officer = self.random.choice(self.staff)
            first = self.random.choice(FIRST_NAMES)
            last = self.random.choice(LAST_NAMES)
            activation = self.as_of - timedelta(days=self.random.randint(30, 2200))
            active = self.random.random() > 0.12
            rows.append({
                "id": index, "accountNo": f"{index:09d}",
                "externalId": f"EXT-C-{index:06d}",
                "status": {
                    "id": 300 if active else 600,
                    "code": ("clientStatusType.active" if active
                             else "clientStatusType.closed"),
                    "value": "Active" if active else "Closed"},
                "subStatus": {"id": 0, "value": None},
                "active": active,
                "activationDate": _d(activation),
                "timeline": {"submittedOnDate": _d(activation - timedelta(days=3)),
                             "activatedOnDate": _d(activation)},
                "officeId": office["id"], "officeName": office["name"],
                "staffId": officer["id"], "staffName": officer["displayName"],
                "legalForm": {"id": 1 if index % 3 else 2,
                              "value": "PERSON" if index % 3 else "ENTITY"},
                "gender": {"id": 1 if index % 2 else 2,
                           "value": "Female" if index % 2 else "Male"},
                "clientType": {
                    "id": 1,
                    "value": self.random.choice(["Individual", "Group member"])},
                "clientClassification": {"id": 1, "value": self.random.choice(
                    ["Refugee entrepreneur", "Host community", "Youth", "Women-led"])},
                "firstname": first, "lastname": last,
                "displayName": f"{first} {last}",
                "mobileNo": f"+2547{self.random.randint(10_000_000, 99_999_999)}",
                "emailAddress": f"{first.lower()}.{last.lower()}{index}@example.org",
                "dateOfBirth": _d(
                    date(1970, 1, 1) + timedelta(days=self.random.randint(0, 11_000))),
            })
        return rows

    def _build_loans(self, count: int) -> list[dict]:
        rows = []
        for index in range(1, count + 1):
            client = self.random.choice(self.clients)
            product = self.random.choice(self.loan_products)
            office = next(o for o in self.offices if o["id"] == client["officeId"])
            officer = self.random.choice(self.staff)
            status_id, status_code, status_value = self.random.choices(
                LOAN_STATUSES, weights=[4, 6, 45, 30, 5, 10])[0]

            principal = float(self.random.randrange(20_000, 800_000, 5_000))
            term = int(product["numberOfRepayments"])
            rate = float(product["annualInterestRate"])
            submitted = self.as_of - timedelta(days=self.random.randint(20, 1500))
            approved = submitted + timedelta(days=self.random.randint(1, 14))
            disbursed = approved + timedelta(days=self.random.randint(0, 10))
            maturity = disbursed + timedelta(days=30 * term)

            interest_charged = round(principal * (rate / 100) * (term / 12), 2)
            progress = min(1.0, max(0.0, self.random.betavariate(2, 2)))
            if status_id in (600, 700):
                progress = 1.0
            principal_paid = round(principal * progress, 2)
            interest_paid = round(interest_charged * progress, 2)
            principal_outstanding = round(principal - principal_paid, 2)
            interest_outstanding = round(interest_charged - interest_paid, 2)

            overdue_days = 0
            overdue_since = None
            if status_id == 300 and self.random.random() < 0.22:
                overdue_days = self.random.choice([3, 12, 25, 40, 65, 95, 150, 210])
                overdue_since = self.as_of - timedelta(days=overdue_days)
            written_off = round(principal_outstanding, 2) if status_id == 601 else 0.0

            rows.append({
                "id": index, "accountNo": f"L{index:09d}",
                "externalId": f"EXT-L-{index:06d}",
                "clientId": client["id"], "clientName": client["displayName"],
                "groupId": None,
                "loanProductId": product["id"], "loanProductName": product["name"],
                "officeId": office["id"], "officeName": office["name"],
                "loanOfficerId": officer["id"], "loanOfficerName": officer["displayName"],
                "loanType": {"id": 1, "value": "Individual"},
                "currency": {"code": "KES", "decimalPlaces": 2},
                "status": {"id": status_id, "code": status_code, "value": status_value,
                           "active": status_id == 300,
                           "closed": status_id in (600, 601),
                           "overpaid": status_id == 700},
                "timeline": {
                    "submittedOnDate": _d(submitted),
                    "approvedOnDate": _d(approved) if status_id >= 200 else None,
                    "expectedDisbursementDate": _d(disbursed),
                    "actualDisbursementDate": _d(disbursed) if status_id >= 300 else None,
                    "expectedMaturityDate": _d(maturity),
                    "closedOnDate": _d(maturity) if status_id in (600, 601) else None,
                },
                "principal": principal, "approvedPrincipal": principal,
                "termFrequency": term,
                "termPeriodFrequencyType": {"id": 2, "value": "Months"},
                "numberOfRepayments": term, "repaymentEvery": 1,
                "repaymentFrequencyType": {"id": 2, "value": "Months"},
                "interestRatePerPeriod": product["interestRatePerPeriod"],
                "annualInterestRate": rate,
                "summary": {
                    "principalDisbursed": principal if status_id >= 300 else 0,
                    "principalPaid": principal_paid,
                    "principalWrittenOff": written_off,
                    "principalOutstanding": principal_outstanding,
                    "principalOverdue": (
                        round(principal_outstanding * 0.35, 2) if overdue_days else 0),
                    "interestCharged": interest_charged,
                    "interestPaid": interest_paid,
                    "interestWaived": 0,
                    "interestOutstanding": interest_outstanding,
                    "interestOverdue": round(interest_outstanding * 0.35, 2) if overdue_days else 0,
                    "feeChargesCharged": round(principal * 0.02, 2),
                    "feeChargesPaid": round(principal * 0.02 * progress, 2),
                    "feeChargesOutstanding": round(principal * 0.02 * (1 - progress), 2),
                    "penaltyChargesCharged": 500.0 if overdue_days else 0.0,
                    "penaltyChargesPaid": 0.0,
                    "penaltyChargesOutstanding": 500.0 if overdue_days else 0.0,
                    "totalExpectedRepayment": round(principal + interest_charged, 2),
                    "totalRepayment": round(principal_paid + interest_paid, 2),
                    "totalOutstanding": round(principal_outstanding + interest_outstanding, 2),
                    "totalOverdue": round((principal_outstanding + interest_outstanding) * 0.35, 2)
                                    if overdue_days else 0,
                    "overdueSinceDate": _d(overdue_since) if overdue_since else None,
                },
                "delinquent": {"pastDueDays": overdue_days,
                               "delinquentAmount": round(principal_outstanding * 0.35, 2)
                               if overdue_days else 0},
                "_disbursed": disbursed, "_progress": progress,
            })
        return rows

    def _build_savings(self, count: int) -> list[dict]:
        rows = []
        for index in range(1, count + 1):
            client = self.random.choice(self.clients)
            product = self.random.choice(self.savings_products)
            opened = self.as_of - timedelta(days=self.random.randint(30, 1500))
            deposits = round(self.random.uniform(5_000, 400_000), 2)
            withdrawals = round(deposits * self.random.uniform(0.1, 0.9), 2)
            rows.append({
                "id": index, "accountNo": f"S{index:09d}",
                "clientId": client["id"], "clientName": client["displayName"],
                "savingsProductId": product["id"], "savingsProductName": product["name"],
                "officeId": client["officeId"],
                "fieldOfficerId": client["staffId"],
                "status": {"id": 300, "value": "Active", "active": True},
                "currency": {"code": "KES", "decimalPlaces": 2},
                "nominalAnnualInterestRate": product["nominalAnnualInterestRate"],
                "timeline": {"submittedOnDate": _d(opened - timedelta(days=2)),
                             "activatedOnDate": _d(opened)},
                "summary": {
                    "accountBalance": round(deposits - withdrawals, 2),
                    "availableBalance": round(deposits - withdrawals, 2),
                    "totalDeposits": deposits,
                    "totalWithdrawals": withdrawals,
                    "totalInterestPosted": round(deposits * 0.01, 2),
                },
            })
        return rows

    def _build_transactions(self) -> dict[int, list[dict]]:
        by_loan: dict[int, list[dict]] = {}
        tx_id = 1
        for loan in self.loans:
            if loan["status"]["id"] < 300:
                by_loan[loan["id"]] = []
                continue
            rows = [{
                "id": tx_id, "loanId": loan["id"], "officeId": loan["officeId"],
                "officeName": loan["officeName"],
                "type": {"id": TX_TYPES[0][0], "code": TX_TYPES[0][1],
                         "value": TX_TYPES[0][2], "disbursement": True},
                "date": _d(loan["_disbursed"]),
                "submittedOnDate": _d(loan["_disbursed"]),
                "currency": {"code": "KES", "decimalPlaces": 2},
                "amount": loan["principal"],
                "netDisbursalAmount": loan["principal"],
                "principalPortion": loan["principal"],
                "interestPortion": 0, "feeChargesPortion": 0,
                "penaltyChargesPortion": 0, "overpaymentPortion": 0,
                "outstandingLoanBalance": loan["principal"],
                "manuallyReversed": False,
            }]
            tx_id += 1

            installments = max(1, int(loan["numberOfRepayments"] * loan["_progress"]))
            balance = float(loan["principal"])
            per_principal = float(loan["principal"]) / max(1, loan["numberOfRepayments"])
            per_interest = (
                float(loan["summary"]["interestCharged"])
                / max(1, loan["numberOfRepayments"]))
            for n in range(1, installments + 1):
                balance = max(0.0, balance - per_principal)
                rows.append({
                    "id": tx_id, "loanId": loan["id"], "officeId": loan["officeId"],
                    "officeName": loan["officeName"],
                    "type": {"id": TX_TYPES[1][0], "code": TX_TYPES[1][1],
                             "value": TX_TYPES[1][2], "repayment": True},
                    "date": _d(loan["_disbursed"] + timedelta(days=30 * n)),
                    "submittedOnDate": _d(loan["_disbursed"] + timedelta(days=30 * n)),
                    "currency": {"code": "KES", "decimalPlaces": 2},
                    "amount": round(per_principal + per_interest, 2),
                    "principalPortion": round(per_principal, 2),
                    "interestPortion": round(per_interest, 2),
                    "feeChargesPortion": 0, "penaltyChargesPortion": 0,
                    "overpaymentPortion": 0,
                    "outstandingLoanBalance": round(balance, 2),
                    "manuallyReversed": self.random.random() < 0.01,
                })
                tx_id += 1
            by_loan[loan["id"]] = rows
        return by_loan

    # -- serving ------------------------------------------------------
    @staticmethod
    def _strip_private(rows: list[dict]) -> list[dict]:
        return [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]

    def page(self, rows: list[dict], query: dict[str, list[str]]) -> Any:
        rows = self._strip_private(rows)
        if query.get("paged", ["false"])[0].lower() != "true":
            limit = int(query.get("limit", [0])[0] or 0)
            return rows[:limit] if limit else rows
        offset = int(query.get("offset", [0])[0] or 0)
        limit = int(query.get("limit", [200])[0] or 200)
        return {"totalFilteredRecords": len(rows),
                "pageItems": rows[offset:offset + limit]}


class Handler(BaseHTTPRequestHandler):
    dataset: FineractDataset = None            # type: ignore[assignment]
    require_auth: bool = True
    #: Probabilistic fault injection - useful for soak runs.
    failure_rate: float = 0.0
    #: Deterministic fault injection: fail exactly the next N requests
    #: with 503. Probabilistic injection makes a retry test flaky (a
    #: single request has a 1-in-2 chance of never retrying), so tests
    #: that assert on the retry path use this instead.
    fail_next_n: int = 0

    protocol_version = "HTTP/1.1"

    def log_message(self, *args: Any) -> None:  # keep CI output clean
        return

    def _send(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _unauthorised(self) -> bool:
        if not self.require_auth:
            return False
        if not self.headers.get("Authorization"):
            self._send({"developerMessage": "missing Authorization"}, 401)
            return True
        if not self.headers.get("Fineract-Platform-TenantId"):
            self._send({"developerMessage": "missing Fineract-Platform-TenantId"}, 400)
            return True
        return False

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.endswith("/authentication"):
            length = int(self.headers.get("Content-Length", 0) or 0)
            self.rfile.read(length)
            self._send({"username": "mifos", "authenticated": True,
                        "base64EncodedAuthenticationKey": "bWlmb3M6cGFzc3dvcmQ="})
            return
        self._send({"developerMessage": "not found"}, 404)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = re.sub(r"^.*?/api/v1", "", parsed.path).strip("/")

        if path in {"actuator/health", "health"}:
            self._send({"status": "UP"})
            return
        if self._unauthorised():
            return
        if type(self).fail_next_n > 0:
            type(self).fail_next_n -= 1
            self._send({"developerMessage": "injected deterministic failure"}, 503)
            return
        if self.failure_rate and random.random() < self.failure_rate:
            self._send({"developerMessage": "injected transient failure"}, 503)
            return

        data = self.dataset
        routes = {
            "offices": data.offices,
            "staff": data.staff,
            "loanproducts": data.loan_products,
            "savingsproducts": data.savings_products,
            "clients": data.clients,
            "loans": data.loans,
            "savingsaccounts": data.savings,
        }
        if path in routes:
            self._send(data.page(routes[path], query))
            return

        match = re.fullmatch(r"loans/(\d+)/transactions", path)
        if match:
            self._send(data.page(data.transactions.get(int(match.group(1)), []), query))
            return

        match = re.fullmatch(r"(clients|loans)/(\d+)", path)
        if match:
            collection = routes[match.group(1)]
            found = next((r for r in collection if r["id"] == int(match.group(2))), None)
            self._send(found or {"developerMessage": "not found"}, 200 if found else 404)
            return

        self._send({"developerMessage": f"unmapped path '{path}'"}, 404)


def serve(port: int = 8090, clients: int = 400, loans: int = 700, seed: int = 42,
          failure_rate: float = 0.0) -> None:
    Handler.dataset = FineractDataset(clients=clients, loans=loans, seed=seed)
    Handler.failure_rate = failure_rate
    Handler.fail_next_n = 0
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    total_tx = sum(len(v) for v in Handler.dataset.transactions.values())
    print(json.dumps({
        "event": "mock_fineract_started", "port": port,
        "clients": len(Handler.dataset.clients), "loans": len(Handler.dataset.loans),
        "loan_transactions": total_tx,
        "base_url": f"http://localhost:{port}/fineract-provider/api/v1"}), flush=True)
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock Apache Fineract v1 API.")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--clients", type=int, default=400)
    parser.add_argument("--loans", type=int, default=700)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--failure-rate", type=float, default=0.0,
                        help="Probability of injecting a 503, to exercise retries.")
    args = parser.parse_args()
    serve(args.port, args.clients, args.loans, args.seed, args.failure_rate)


if __name__ == "__main__":
    main()
