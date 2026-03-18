from sqladmin import Admin, ModelView
from app.models import Transaction


class TransactionAdmin(ModelView, model=Transaction):
    name = "Transaction"
    name_plural = "Transactions"

    column_list = [
        Transaction.id,
        Transaction.date,
        Transaction.sender_name,
        Transaction.recipient_name,
        Transaction.amount_tenge,
        Transaction.currency,
    ]

    column_searchable_list = [
        Transaction.sender_name,
        Transaction.recipient_name,
        Transaction.sender_iin_bin,
        Transaction.recipient_iin_bin,
    ]

    column_sortable_list = [
        Transaction.date,
        Transaction.amount_tenge,
    ]

    page_size = 50
