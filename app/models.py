from __future__ import annotations

import uuid
from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Date, Text, 
    ForeignKey, Numeric, Boolean, Index, Table, func, 
    BigInteger, SmallInteger, CHAR, Computed, text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, BYTEA
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import relationship, synonym
from .database import Base


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (
        Index("idx_projects_owner", "owner_user_id"),
        {"schema": "afm"},
    )

    project_id = Column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_user_id = Column(Integer, ForeignKey("afm.users.id", ondelete="CASCADE"), nullable=False)
    name = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_afm_users_email", "email", unique=True),
        Index("ix_afm_users_id", "id"),
        {"schema": "afm"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False, default="user")  # user/admin
    active_project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)


class RawFile(Base):
    __tablename__ = "raw_files"
    __table_args__ = (
        Index("idx_raw_files_project", "project_id"),
        {"schema": "afm"},
    )

    file_id = Column(UUID(as_uuid=False), primary_key=True)
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)
    source_bank = Column(Text, nullable=False)
    original_filename = Column(Text, nullable=False)
    sha256 = Column(Text, nullable=False)
    uploaded_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    parsed_at = Column(DateTime(timezone=True), nullable=True)
    parser_version = Column(Text, nullable=False)
    notes = Column(Text, nullable=True)


class FormatRegistry(Base):
    __tablename__ = "format_registry"
    __table_args__ = (
        Index("idx_fmt_bank", "source_bank"),
        Index("idx_fmt_fingerprint", "header_fingerprint", unique=True),
        {"schema": "afm"},
    )

    format_id = Column(UUID(as_uuid=False), primary_key=True)
    source_bank = Column(Text, nullable=True)
    header_fingerprint = Column(Text, nullable=True)
    header_sample = Column(JSONB, nullable=True)
    embedding_vector = Column(Vector(1024), nullable=True)
    first_seen = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_seen = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    usage_count = Column(Integer, nullable=False, server_default=text("1"))


class Statement(Base):
    __tablename__ = "statements"
    __table_args__ = (
        Index("idx_stmt_file", "file_id"),
        Index("idx_stmt_account", "account_iban"),
        Index("idx_stmt_project", "project_id"),
        {"schema": "afm"},
    )

    statement_id = Column(UUID(as_uuid=False), primary_key=True)
    file_id = Column(UUID(as_uuid=False), ForeignKey("afm.raw_files.file_id"), nullable=False)
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)
    source_bank = Column(Text, nullable=False)
    source_sheet = Column(Text, nullable=True)
    source_block_id = Column(Integer, nullable=True)
    format_id = Column(UUID(as_uuid=False), ForeignKey("afm.format_registry.format_id"), nullable=True)

    client_name = Column(Text, nullable=True)
    client_iin_bin = Column(CHAR(12), nullable=True)
    account_iban = Column(Text, nullable=True)
    account_type = Column(Text, nullable=True)
    currency = Column(Text, nullable=True)

    statement_date = Column(Date, nullable=True)
    period_from = Column(Date, nullable=True)
    period_to = Column(Date, nullable=True)

    opening_balance = Column(Numeric(18, 2), nullable=True)
    closing_balance = Column(Numeric(18, 2), nullable=True)
    total_debit = Column(Numeric(18, 2), nullable=True)
    total_credit = Column(Numeric(18, 2), nullable=True)

    meta_json = Column(JSONB, nullable=True)


class Transaction(Base):
    __tablename__ = "transactions_core"
    __table_args__ = (
        Index("idx_tx_core_date", "operation_date"),
        Index("idx_tx_core_file", "file_id"),
        Index("idx_tx_core_project", "project_id"),
        Index("idx_tx_stmt", "statement_id"),
        Index("idx_tx_format", "format_id"),
        Index("idx_tx_category", "transaction_category"),
        Index("idx_tx_needs_review", "needs_review"),
        Index("uq_tx_project_rowhash_idx", "project_id", "row_hash", unique=True),
        {"schema": "afm"},
    )

    tx_id = Column(UUID(as_uuid=False), primary_key=True)
    # Alias for SQLAdmin
    id = synonym("tx_id")
    
    file_id = Column(UUID(as_uuid=False), ForeignKey("afm.raw_files.file_id"), nullable=False)
    statement_id = Column(UUID(as_uuid=False), ForeignKey("afm.statements.statement_id"), nullable=True)
    format_id = Column(UUID(as_uuid=False), ForeignKey("afm.format_registry.format_id"), nullable=True)
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)

    source_bank = Column(Text, nullable=False)
    source_sheet = Column(Text, nullable=True)
    source_block_id = Column(Integer, nullable=True)
    source_row_no = Column(Integer, nullable=True)
    row_hash = Column(Text, nullable=False)

    date = Column("operation_ts", DateTime(timezone=True), nullable=True)
    operation_date = Column(Date, nullable=True)

    currency = Column(Text, nullable=True)
    amount_currency = Column(Numeric(18, 2), nullable=True)
    amount_tenge = Column("amount_kzt", Numeric(18, 2), nullable=True)
    credit = Column("amount_credit", Numeric(18, 2), nullable=True)
    debit = Column("amount_debit", Numeric(18, 2), nullable=True)
    direction = Column(Text, nullable=True)

    operation_type = Column("operation_type_raw", Text, nullable=True)
    category = Column("sdp_name", Text, nullable=True)
    purpose_code = Column(Text, nullable=True)
    purpose = Column("purpose_text", Text, nullable=True)
    raw_note = Column(Text, nullable=True)

    sender_name = Column("payer_name", Text, nullable=True)
    sender_iin_bin = Column("payer_iin_bin", CHAR(12), nullable=True)
    payer_residency = Column(Text, nullable=True)
    payer_bank = Column(Text, nullable=True)
    sender_account = Column("payer_account", Text, nullable=True)

    recipient_name = Column("receiver_name", Text, nullable=True)
    recipient_iin_bin = Column("receiver_iin_bin", CHAR(12), nullable=True)
    receiver_residency = Column(Text, nullable=True)
    receiver_bank = Column(Text, nullable=True)
    recipient_account = Column("receiver_account", Text, nullable=True)

    confidence_score = Column(Float, nullable=False, server_default="1.0")
    parse_warnings = Column(Text, nullable=True)
    raw_row_json = Column(JSONB, nullable=True)
    
    transaction_category = Column(Text, nullable=False, server_default="Прочее")
    category_confidence = Column(Numeric(5, 4), nullable=True)
    category_source = Column(Text, nullable=False, server_default="other")
    category_rule_id = Column(Text, nullable=True)
    needs_review = Column(Boolean, nullable=False, server_default="false")
    
    semantic_text = Column(Text, nullable=True)
    semantic_embedding = Column(Vector(1024), nullable=True)


class TransactionUploadMeta(Base):
    __tablename__ = "transaction_upload_meta"
    __table_args__ = (
        Index("ix_afm_transaction_upload_meta_tx_id", "tx_id"),
        Index("ix_afm_transaction_upload_meta_uploaded_by_email", "uploaded_by_email"),
        {"schema": "afm"},
    )

    tx_id = Column(UUID(as_uuid=False), primary_key=True)
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)
    uploaded_by_email = Column(String, nullable=False, server_default="")
    created_at = Column(DateTime, nullable=False, server_default=func.now())


class TransactionExt(Base):
    __tablename__ = "transactions_ext"
    __table_args__ = ({"schema": "afm"},)

    tx_id = Column(UUID(as_uuid=False), ForeignKey("afm.transactions_core.tx_id"), primary_key=True)
    ext_json = Column(JSONB, nullable=False)


class EsfRecord(Base):
    __tablename__ = "esf_records"
    __table_args__ = (
        Index("idx_esf_project_issue_date", "project_id", "issue_date"),
        Index("idx_esf_project_turnover_date", "project_id", "turnover_date"),
        Index("idx_esf_project_supplier_iin", "project_id", "supplier_iin_bin"),
        Index("idx_esf_project_buyer_iin", "project_id", "buyer_iin_bin"),
        Index("idx_esf_project_status", "project_id", "esf_status"),
        Index("uq_esf_project_rowhash_idx", "project_id", "row_hash", unique=True),
        {"schema": "afm"},
    )

    id = Column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)
    file_id = Column(UUID(as_uuid=False), ForeignKey("afm.raw_files.file_id"), nullable=True)
    source_sheet = Column(Text, nullable=True)
    source_row_no = Column(Integer, nullable=True)
    row_hash = Column(Text, nullable=False)
    esf_direction = Column(Text, nullable=False, server_default="sale")

    registration_number = Column(Text, nullable=False)
    tax_authority_code = Column(Text, nullable=True)
    esf_status = Column(Text, nullable=True)
    issue_date = Column(DateTime(timezone=True), nullable=True)
    turnover_date = Column(DateTime(timezone=True), nullable=True)
    year = Column(Integer, nullable=True)

    supplier_iin_bin = Column(CHAR(12), nullable=True)
    supplier_name = Column(Text, nullable=True)
    supplier_address = Column(Text, nullable=True)

    buyer_iin_bin = Column(CHAR(12), nullable=True)
    buyer_name = Column(Text, nullable=True)
    buyer_address = Column(Text, nullable=True)

    country_code = Column(Text, nullable=True)

    consignor_iin_bin = Column(CHAR(12), nullable=True)
    consignor_name = Column(Text, nullable=True)
    ship_from_address = Column(Text, nullable=True)

    consignee_iin_bin = Column(CHAR(12), nullable=True)
    consignee_name = Column(Text, nullable=True)
    delivery_address = Column(Text, nullable=True)

    contract_number = Column(Text, nullable=True)
    contract_date = Column(DateTime(timezone=True), nullable=True)
    payment_terms = Column(Text, nullable=True)
    destination = Column(Text, nullable=True)

    origin_sign = Column(Text, nullable=True)
    tru_name = Column(Text, nullable=True)
    tnved_code = Column(Text, nullable=True)
    unit = Column(Text, nullable=True)
    quantity = Column(Numeric(18, 4), nullable=True)
    price_without_vat = Column(Numeric(18, 2), nullable=True)
    price_with_vat = Column(Numeric(18, 2), nullable=True)
    cost_without_indirect_tax = Column(Numeric(18, 2), nullable=True)
    turnover_amount = Column(Numeric(18, 2), nullable=True)
    vat_rate = Column(Numeric(10, 4), nullable=True)
    vat_amount = Column(Numeric(18, 2), nullable=True)
    cost_with_indirect_tax = Column(Numeric(18, 2), nullable=True)
    total_amount = Column(Numeric(18, 2), nullable=True)
    currency_rate = Column(Numeric(18, 6), nullable=True)
    currency_code = Column(Text, nullable=True)
    currency_name_ru = Column(Text, nullable=True)

    raw_row_json = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class FieldDiscoveryLog(Base):
    __tablename__ = "field_discovery_log"
    __table_args__ = ({"schema": "afm"},)

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    file_id = Column(UUID(as_uuid=False), ForeignKey("afm.raw_files.file_id"), nullable=True)
    source_bank = Column(Text, nullable=True)
    format_id = Column(UUID(as_uuid=False), nullable=True)
    raw_column_name = Column(Text, nullable=False)
    normalized_name = Column(Text, nullable=True)
    sample_values = Column(JSONB, nullable=True)
    suggested_field = Column(Text, nullable=True)
    confidence = Column(Float, nullable=True)
    status = Column(Text, nullable=False, server_default="new")


class SemanticCatalog(Base):
    __tablename__ = "semantic_catalog"
    __table_args__ = (
        Index("idx_sem_cat_type", "type"),
        Index("idx_sem_cat_bank", "source_bank"),
        {"schema": "afm"},
    )

    id = Column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    type = Column(Text, nullable=False, server_default="tx")
    text = Column(Text, nullable=False)
    embedding = Column(Vector(1024), nullable=True)
    meta = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    # Extended fields from init_schema.sql
    tx_id = Column(UUID(as_uuid=False), nullable=True)
    source_bank = Column(Text, nullable=True)
    semantic_text = Column(Text, nullable=True)
    source_columns = Column(JSONB, nullable=True)


class QueryHistory(Base):
    __tablename__ = "query_history"
    __table_args__ = (
        Index("idx_qh_success", "execution_success"),
        Index("idx_qh_project", "project_id"),
        {"schema": "afm"},
    )

    id = Column(UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id = Column(UUID(as_uuid=False), ForeignKey("afm.projects.project_id"), nullable=True)
    question = Column(Text, nullable=False)
    generated_sql = Column(Text, nullable=True)
    execution_success = Column(Boolean, nullable=False, server_default="false")
    user_feedback = Column(SmallInteger, nullable=True)
    embedding = Column(Vector(1024), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    execution_time_ms = Column(Integer, nullable=True)
    row_count = Column(Integer, nullable=True)
    repaired = Column(Boolean, nullable=False, server_default="false")
    error_text = Column(Text, nullable=True)


class PendingRegistration(Base):
    __tablename__ = "pending_registrations"
    __table_args__ = (
        Index("ix_afm_pending_registrations_email", "email", unique=True),
        Index("ix_afm_pending_registrations_expires_at", "expires_at"),
        Index("ix_afm_pending_registrations_id", "id"),
        {"schema": "afm"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    verification_code = Column(String(6), nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
