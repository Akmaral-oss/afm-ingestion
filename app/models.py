from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import synonym
from .database import Base


class Transaction(Base):
    __tablename__ = "transactions_core"
    __table_args__ = {"schema": "afm"}

    id = Column("tx_id", UUID(as_uuid=False), primary_key=True, index=True)
    # SQLAdmin resolves primary keys by column name, so expose tx_id as an alias.
    tx_id = synonym("id")
    file_id = Column(UUID(as_uuid=False), nullable=True, index=True)
    statement_id = Column(UUID(as_uuid=False), nullable=True, index=True)
    format_id = Column(UUID(as_uuid=False), nullable=True, index=True)

    source_bank = Column(String, nullable=True, index=True)
    source_sheet = Column(String, nullable=True)
    source_block_id = Column(Integer, nullable=True)
    source_row_no = Column(Integer, nullable=True)
    row_hash = Column(String, nullable=True, index=True)

    date = Column("operation_ts", DateTime(timezone=True), nullable=False, index=True)
    operation_date = Column(Date, nullable=True, index=True)

    currency = Column(String(3), nullable=False, default="KZT")
    amount_currency = Column(Float, nullable=False, default=0)
    amount_tenge = Column("amount_kzt", Float, nullable=False, default=0)
    credit = Column("amount_credit", Float, nullable=False, default=0)
    debit = Column("amount_debit", Float, nullable=False, default=0)
    direction = Column(String, nullable=True, index=True)

    operation_type = Column("operation_type_raw", Text, nullable=False, default="")
    category = Column("sdp_name", String, nullable=False, default="")
    purpose_code = Column(String, nullable=True)
    purpose = Column("purpose_text", Text, nullable=False, default="")
    raw_note = Column(Text, nullable=True)

    sender_name = Column("payer_name", Text, nullable=False, default="")
    sender_iin_bin = Column("payer_iin_bin", String(32), nullable=False, default="", index=True)
    payer_residency = Column(String, nullable=True)
    payer_bank = Column(String, nullable=True)
    sender_account = Column("payer_account", String(64), nullable=False, default="")

    recipient_name = Column("receiver_name", Text, nullable=False, default="")
    recipient_iin_bin = Column("receiver_iin_bin", String(32), nullable=False, default="", index=True)
    receiver_residency = Column(String, nullable=True)
    receiver_bank = Column(String, nullable=True)
    recipient_account = Column("receiver_account", String(64), nullable=False, default="")

    confidence_score = Column(Float, nullable=True)
    parse_warnings = Column(Text, nullable=True)
    raw_row_json = Column(JSONB, nullable=True)


class TransactionUploadMeta(Base):
    __tablename__ = "transaction_upload_meta"
    __table_args__ = {"schema": "afm"}

    tx_id = Column(UUID(as_uuid=False), primary_key=True, index=True)
    uploaded_by_email = Column(String, nullable=False, default="", index=True)
    created_at = Column(DateTime, nullable=False)

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "afm"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False, default="user")  # user/admin


class PendingRegistration(Base):
    __tablename__ = "pending_registrations"
    __table_args__ = {"schema": "afm"}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    verification_code = Column(String(6), nullable=False)
    expires_at = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False)
