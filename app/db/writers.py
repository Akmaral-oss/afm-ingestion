from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.utils.json_utils import safe_json


class PostgresWriter:
    def __init__(self, engine: Engine, parser_version: str):
        self.engine = engine
        self.parser_version = parser_version

    def insert_raw_file(
        self,
        file_id: str,
        source_bank: str,
        filename: str,
        sha256: str,
        project_id: str | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO afm.raw_files(file_id, project_id, source_bank, original_filename, sha256, parser_version)
                    VALUES (CAST(:file_id AS uuid), CAST(:project_id AS uuid), :source_bank, :filename, :sha256, :parser_version)
                    ON CONFLICT (file_id) DO NOTHING;
                    """
                ),
                {
                    "file_id": file_id,
                    "project_id": project_id,
                    "source_bank": source_bank,
                    "filename": filename,
                    "sha256": sha256,
                    "parser_version": self.parser_version,
                },
            )

    def mark_parsed(self, file_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE afm.raw_files SET parsed_at = now() WHERE file_id = CAST(:file_id AS uuid);"),
                {"file_id": file_id},
            )

    def get_format_by_fingerprint(self, fingerprint: str) -> Optional[str]:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("SELECT format_id::text FROM afm.format_registry WHERE header_fingerprint = :h;"),
                {"h": fingerprint},
            ).scalar()
            return str(result) if result else None

    def load_format_vectors(self, source_bank: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT format_id::text, source_bank, header_fingerprint, embedding_vector FROM afm.format_registry"
        params: Dict[str, Any] = {}
        if source_bank:
            sql += " WHERE source_bank = :b"
            params["b"] = source_bank
        with self.engine.begin() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
            return [dict(row) for row in rows]

    def bump_format_usage(self, format_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE afm.format_registry
                    SET usage_count = usage_count + 1,
                        last_seen = now()
                    WHERE format_id = CAST(:fid AS uuid);
                    """
                ),
                {"fid": format_id},
            )

    def insert_new_format(
        self,
        format_id: str,
        source_bank: str,
        fp: str,
        header_sample: dict,
        embedding_vector: str | list | None,
    ) -> None:
        payload = {
            "format_id": format_id,
            "source_bank": source_bank,
            "fp": fp,
            "hs": safe_json(header_sample),
            "ev": embedding_vector,
        }
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO afm.format_registry(
                        format_id, source_bank, header_fingerprint, header_sample, embedding_vector
                    ) VALUES (
                        CAST(:format_id AS uuid),
                        :source_bank,
                        :fp,
                        CAST(:hs AS jsonb),
                        CAST(:ev AS vector)
                    )
                    ON CONFLICT (header_fingerprint) DO NOTHING;
                    """
                ),
                payload,
            )

    def insert_statement(self, row: Dict[str, Any]) -> None:
        payload = dict(row)
        payload["meta_json"] = safe_json(payload.get("meta_json") or {})
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO afm.statements(
                      statement_id, file_id, project_id, source_bank, source_sheet, source_block_id, format_id,
                      client_name, client_iin_bin, account_iban, account_type, currency,
                      statement_date, period_from, period_to,
                      opening_balance, closing_balance, total_debit, total_credit,
                      meta_json
                    )
                    VALUES (
                      CAST(:statement_id AS uuid),
                      CAST(:file_id AS uuid),
                      CAST(:project_id AS uuid),
                      :source_bank, :source_sheet, :source_block_id,
                      CAST(:format_id AS uuid),
                      :client_name, :client_iin_bin, :account_iban, :account_type, :currency,
                      :statement_date, :period_from, :period_to,
                      :opening_balance, :closing_balance, :total_debit, :total_credit,
                      CAST(:meta_json AS jsonb)
                    )
                    ON CONFLICT (statement_id) DO NOTHING;
                    """
                ),
                payload,
            )

    def bulk_insert_core_dedup(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        cols = list(rows[0].keys())
        value_expr = [f":{col}" for col in cols]

        update_cols = []
        for col in (
            "semantic_text",
            "semantic_embedding",
            "transaction_category",
            "category_confidence",
            "category_source",
            "category_rule_id",
            "needs_review",
        ):
            if col in cols:
                update_cols.append(f"{col} = EXCLUDED.{col}")

        if update_cols:
            conflict_sql = "ON CONFLICT (project_id, row_hash) DO UPDATE SET " + ", ".join(update_cols)
        else:
            conflict_sql = "ON CONFLICT (project_id, row_hash) DO NOTHING"

        sql = f"""
        INSERT INTO afm.transactions_core ({', '.join(cols)})
        VALUES ({', '.join(value_expr)})
        {conflict_sql};
        """

        with self.engine.begin() as conn:
            conn.execute(text(sql), rows)

    def bulk_insert_ext(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        for row in rows:
            row["ext_json"] = safe_json(row["ext_json"])

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO afm.transactions_ext(tx_id, ext_json)
                    VALUES (CAST(:tx_id AS uuid), CAST(:ext_json AS jsonb))
                    ON CONFLICT (tx_id) DO NOTHING;
                    """
                ),
                rows,
            )

    def insert_discovery(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return

        dedup: Dict[tuple, Dict[str, Any]] = {}
        for record in records:
            key = (record["file_id"], record["raw_column_name"], record.get("format_id"))
            if key not in dedup:
                dedup[key] = record
        records = list(dedup.values())

        for record in records:
            record["sample_values"] = safe_json(record.get("sample_values") or [])

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO afm.field_discovery_log(
                      file_id, source_bank, format_id, raw_column_name, normalized_name,
                      sample_values, suggested_field, confidence, status
                    )
                    VALUES (
                      CAST(:file_id AS uuid),
                      :source_bank,
                      CAST(:format_id AS uuid),
                      :raw_column_name, :normalized_name,
                      CAST(:sample_values AS jsonb),
                      :suggested_field, :confidence, :status
                    );
                    """
                ),
                records,
            )
