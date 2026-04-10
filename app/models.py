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
    embedding_vector = Column(BYTEA, nullable=True)
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
    # We use BYTEA as fallback for vector if pgvector is not specifically configured as a type
    semantic_embedding = Column(BYTEA, nullable=True)


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
    embedding = Column(BYTEA, nullable=True) # vector(1024)
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
    embedding = Column(BYTEA, nullable=True) # vector(1024)
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
