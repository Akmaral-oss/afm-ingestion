"""
Pydantic schemas matching the API spec.
"""

from pydantic import BaseModel, Field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=1, max_length=256)


class RegisterRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=6, max_length=256)


class RegisterSendCodeRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=6, max_length=256)


class RegisterConfirmRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    code: str = Field(min_length=4, max_length=6)


class UserOut(BaseModel):
    id: int
    email: str
    name: str
    role: str
    active_project_id: Optional[str] = None


class LoginResponse(BaseModel):
    token: str
    user: UserOut


class MessageResponse(BaseModel):
    message: str


class ErrorResponse(BaseModel):
    error: str


class ProjectOut(BaseModel):
    project_id: str
    name: str
    is_active: bool = False


class ProjectListResponse(BaseModel):
    items: list[ProjectOut]
    active_project_id: Optional[str] = None


class ProjectCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)


# ---------------------------------------------------------------------------
# Counterparty (shared sub-object)
# ---------------------------------------------------------------------------

class CounterpartyOut(BaseModel):
    name: str
    iin_bin: str
    account: str


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

class TransactionOut(BaseModel):
    id: str
    date: str
    sender: CounterpartyOut
    recipient: CounterpartyOut
    category: str
    operation_type: str
    purpose: str
    currency: str
    debit: float
    credit: float
    amount_tenge: float
    uploaded_by_email: str = ""


class PaginationOut(BaseModel):
    page: int
    per_page: int
    total: int
    total_pages: int


class SummaryOut(BaseModel):
    total_debit: float
    total_credit: float


class TransactionListResponse(BaseModel):
    data: list[TransactionOut]
    pagination: PaginationOut
    summary: SummaryOut


class TransactionImportResponse(BaseModel):
    inserted: int
    skipped: int
    message: str


# ---------------------------------------------------------------------------
# Analytics - Time Series
# ---------------------------------------------------------------------------

class TimeSeriesPoint(BaseModel):
    label: str
    date: str
    credit: float
    debit: float


class TimeSeriesResponse(BaseModel):
    period: str
    data: list[TimeSeriesPoint]


# ---------------------------------------------------------------------------
# Analytics - Summary (KPI)
# ---------------------------------------------------------------------------

class PeriodRange(BaseModel):
    model_config = {"populate_by_name": True}

    from_: str = Field(alias="from", serialization_alias="from")
    to: str


class AnalyticsSummaryResponse(BaseModel):
    total_credit: float
    total_debit: float
    total_turnover: float
    total_transactions: int
    period: PeriodRange


# ---------------------------------------------------------------------------
# Analytics - Top Expenses
# ---------------------------------------------------------------------------

class TopExpenseItem(BaseModel):
    counterparty: CounterpartyOut
    amount: float
    transaction_count: int
    percentage: float
    last_transaction_date: Optional[str] = None


class TopExpensesResponse(BaseModel):
    type: str
    total: float
    data: list[TopExpenseItem]


# ---------------------------------------------------------------------------
# Analytics - Top Counterparties
# ---------------------------------------------------------------------------

class TopCounterpartyItem(BaseModel):
    counterparty: CounterpartyOut
    total_credit: float
    total_debit: float
    total_turnover: float
    transaction_count: int


class TopCounterpartiesResponse(BaseModel):
    data: list[TopCounterpartyItem]


class CounterpartySearchItem(BaseModel):
    counterparty: CounterpartyOut
    total_turnover: float
    transaction_count: int


class CounterpartySearchResponse(BaseModel):
    data: list[CounterpartySearchItem]


class CashTransactionItem(BaseModel):
    id: str
    date: str
    sender_name: str
    recipient_name: str
    purpose: str
    currency: str
    debit: float
    credit: float
    amount_tenge: float


class TimeSeriesTransactionsResponse(BaseModel):
    period: str
    bucket: str
    total: int
    data: list[CashTransactionItem]


class CashTransactionsResponse(BaseModel):
    type: str
    counterparty: CounterpartyOut
    total: int
    data: list[CashTransactionItem]


class CounterpartyTransactionsResponse(BaseModel):
    counterparty: CounterpartyOut
    total: int
    data: list[CashTransactionItem]


class EdgeTransactionsResponse(BaseModel):
    source: CounterpartyOut
    target: CounterpartyOut
    total: int
    data: list[CashTransactionItem]


class CounterpartyGraphNode(BaseModel):
    id: str
    label: str
    iin_bin: str
    level: int
    total_turnover: float


class CounterpartyGraphEdge(BaseModel):
    source: str
    target: str
    amount: float
    tx_count: int


class CounterpartyGraphResponse(BaseModel):
    center_iin_bin: str
    nodes: list[CounterpartyGraphNode]
    edges: list[CounterpartyGraphEdge]


class CategorySummaryItem(BaseModel):
    category: str
    transaction_count: int
    total_turnover: float
    total_debit: float
    total_credit: float


class CategorySummaryResponse(BaseModel):
    data: list[CategorySummaryItem]


# ---------------------------------------------------------------------------
# Chat / NL2SQL
# ---------------------------------------------------------------------------

class ChatQueryRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


class ChatQueryResponse(BaseModel):
    success: bool
    question: str
    sql: str
    rows: list[dict[str, Any]]
    execution_time_s: float
    repaired: bool = False
    error: Optional[str] = None
    ai_summary: Optional[str] = None
