"""
app/ingestion/pipeline.py — v4.0
Порядок: classify_rows() вызывается ДО bulk_insert_core_dedup,
чтобы category поля шли в INSERT вместе с остальными данными.
"""
from __future__ import annotations
import logging, os, uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from app.config import Settings
from app.db.engine import make_engine
from app.db.schema import ensure_schema
from app.db.writers import PostgresWriter

from app.ingestion.extractor.universal_extractor import ExcelUniversalExtractor
from app.ingestion.extractor.dataframe_cleaner import clean_dataframe
from app.ingestion.metadata.statement_meta_extractor import StatementMetadataExtractor
from app.ingestion.mapping.canonical_mapper import CanonicalMapper
from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.ingestion.registry.format_registry import FormatRegistryService
from app.ingestion.registry.discovery_logger import DiscoveryLogger
from app.ingestion.extractor.adapter_loader import load_adapters
from app.utils.hashing import sha256_file

from app.semantic.semantic_service import SemanticService
from app.semantic.cluster_builder import build_semantic_text
from app.classification.category_service import CategoryService

log = logging.getLogger(__name__)

AdapterDF = Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]
_EMBED_BATCH_SIZE = 256


def _vec_to_pg_literal(vec: np.ndarray) -> str:
    arr = np.asarray(vec, dtype=np.float32).reshape(-1)
    return "[" + ",".join(f"{v:.6f}" for v in arr) + "]"


def _attach_semantic_fields(rows: List[Dict[str, Any]], embedder: EmbeddingBackend) -> None:
    for row in rows:
        row["semantic_text"] = build_semantic_text(
            operation_type_raw=row.get("operation_type_raw"),
            sdp_name=row.get("sdp_name"),
            purpose_text=row.get("purpose_text"),
            raw_note=row.get("raw_note"),
        )
    if not embedder.enabled:
        return
    idx_to_embed = [i for i, r in enumerate(rows) if r.get("semantic_text")]
    if not idx_to_embed:
        return
    texts = [rows[i]["semantic_text"] for i in idx_to_embed]
    for batch_start in range(0, len(texts), _EMBED_BATCH_SIZE):
        batch_texts = texts[batch_start: batch_start + _EMBED_BATCH_SIZE]
        batch_idx   = idx_to_embed[batch_start: batch_start + _EMBED_BATCH_SIZE]
        try:
            vecs = embedder.embed(batch_texts)
            for list_idx, vec in zip(batch_idx, vecs):
                rows[list_idx]["semantic_embedding"] = _vec_to_pg_literal(vec)
        except Exception:
            log.exception("Embedding batch %d-%d failed", batch_start, batch_start + len(batch_texts))


class IngestionPipeline:
    def __init__(self, settings: Settings):
        self.settings   = settings
        self.adapters   = load_adapters()
        self.engine     = make_engine(settings.pg_dsn)
        ensure_schema(self.engine)
        self.writer     = PostgresWriter(self.engine, parser_version=settings.parser_version)
        self.embedder   = EmbeddingBackend(settings.embedding_model_path)
        self.mapper     = CanonicalMapper(self.embedder, threshold=settings.embedding_threshold)
        self.format_registry = FormatRegistryService(
            writer=self.writer, embedder=self.embedder,
            similarity_threshold=settings.format_similarity_threshold,
        )
        self.discovery      = DiscoveryLogger(self.writer)
        self.extractor      = ExcelUniversalExtractor()
        self.meta_extractor = StatementMetadataExtractor()

        self.semantic_service = SemanticService(
            engine=self.engine, embedder=self.embedder,
            auto_catalog=True,
            cluster_rebuild_every_n=settings.cluster_rebuild_every_n,
        )
        self.category_service = CategoryService(engine=self.engine, embedder=self.embedder)

        log.info("IngestionPipeline ready  embedder=%s",
                 "ON" if self.embedder.enabled else "OFF")

    def _find_adapter(self, bank: str):
        for a in self.adapters:
            if getattr(a, "bank_name", None) == bank:
                return a
        return None

    def ingest_data_folder(self, data_root: str) -> List[Dict[str, Any]]:
        log.info("Scanning data folder: %s", data_root)
        if not os.path.isdir(data_root):
            raise FileNotFoundError(f"data_root not found: {data_root}")
        results: List[Dict[str, Any]] = []
        for adapter in self.adapters:
            bank  = adapter.bank_name
            files = adapter.list_files(data_root)
            if not files:
                continue
            log.info("bank=%s -> %d file(s)", bank, len(files))
            for f in sorted(files):
                try:
                    results.append(self.ingest_file(f, source_bank=bank))
                except Exception:
                    log.exception("Failed ingesting file=%s (bank=%s)", f, bank)
        return results

    def ingest_file(self, file_path: str, source_bank: Optional[str] = None) -> Dict[str, Any]:
        file_id  = str(uuid.uuid4())
        filename = os.path.basename(file_path)
        bank     = source_bank or "unknown"
        checksum = sha256_file(file_path)

        self.writer.insert_raw_file(file_id=file_id, source_bank=bank,
                                    filename=filename, sha256=checksum)

        all_core: List[Dict[str, Any]] = []
        all_ext:  List[Dict[str, Any]] = []
        all_disc: List[Dict[str, Any]] = []
        statements_count = 0
        adapter      = self._find_adapter(bank)
        used_adapter = False

        if adapter is not None:
            log.info("Using adapter=%s for file=%s", adapter.bank_name, filename)
            extracted: List[AdapterDF] = adapter.extract(file_path) or []
            if extracted:
                used_adapter = True
            for idx, item in enumerate(extracted, start=1):
                if isinstance(item, tuple) and len(item) == 2:
                    df, stmt_meta = item
                    stmt_meta = stmt_meta or {}
                else:
                    df, stmt_meta = item, {}  # type: ignore
                if not isinstance(df, pd.DataFrame):
                    continue
                df = clean_dataframe(df)
                if df.empty or len(df.columns) < 3:
                    continue
                format_id    = self.format_registry.register_or_get_format(
                    source_bank=bank, headers=list(df.columns))
                statement_id = str(uuid.uuid4())
                self.writer.insert_statement({
                    "statement_id": statement_id, "file_id": file_id,
                    "source_bank": bank, "source_sheet": stmt_meta.get("source_sheet"),
                    "source_block_id": stmt_meta.get("source_block_id", idx),
                    "format_id": format_id,
                    **{k: stmt_meta.get(k) for k in (
                        "client_name","client_iin_bin","account_iban","account_type",
                        "currency","statement_date","period_from","period_to",
                        "opening_balance","closing_balance","total_debit","total_credit")},
                    "meta_json": stmt_meta.get("meta_json") or {"source": "adapter"},
                })
                statements_count += 1
                mapped, unmapped = self.mapper.map_headers(df)
                ctx = {
                    "file_id": file_id, "statement_id": statement_id,
                    "format_id": format_id, "source_bank": bank,
                    "source_sheet": stmt_meta.get("source_sheet"),
                    "source_block_id": stmt_meta.get("source_block_id", idx),
                    "source_row_base": int(stmt_meta.get("source_row_base", 0)),
                    "store_raw_row_json": self.settings.store_raw_row_json,
                    "account_iban": stmt_meta.get("account_iban"),
                }
                core_rows, ext_rows, disc_rows = self.mapper.to_rows(df, mapped, ctx)
                all_core.extend(core_rows); all_ext.extend(ext_rows)
                if unmapped: all_disc.extend(disc_rows)

        if adapter is None or not used_adapter:
            log.info("Universal extractor for file=%s (bank=%s)", filename, bank)
            for sheet_name in self.extractor.get_sheet_names(file_path):
                grid   = self.extractor.load_sheet_grid(file_path, sheet_name)
                blocks = self.extractor.detect_blocks(grid, sheet_name)
                if not blocks:
                    continue
                for bidx, block in enumerate(blocks, start=1):
                    df = self.extractor.extract_block_df(grid, block)
                    df = clean_dataframe(df)
                    if df.empty or len(df.columns) < 3:
                        continue
                    format_id    = self.format_registry.register_or_get_format(
                        source_bank=bank, headers=list(df.columns))
                    stmt_meta    = self.meta_extractor.extract_for_block(
                        grid=grid, block=block, source_bank=bank,
                        max_lookback_rows=self.settings.max_meta_lookback_rows)
                    statement_id = str(uuid.uuid4())
                    self.writer.insert_statement({
                        "statement_id": statement_id, "file_id": file_id,
                        "source_bank": bank, "source_sheet": sheet_name,
                        "source_block_id": bidx, "format_id": format_id, **stmt_meta,
                    })
                    statements_count += 1
                    mapped, unmapped = self.mapper.map_headers(df)
                    ctx = {
                        "file_id": file_id, "statement_id": statement_id,
                        "format_id": format_id, "source_bank": bank,
                        "source_sheet": sheet_name, "source_block_id": bidx,
                        "source_row_base": block.data_start_row_idx,
                        "store_raw_row_json": self.settings.store_raw_row_json,
                        "account_iban": stmt_meta.get("account_iban"),
                    }
                    core_rows, ext_rows, disc_rows = self.mapper.to_rows(df, mapped, ctx)
                    all_core.extend(core_rows); all_ext.extend(ext_rows)
                    if unmapped: all_disc.extend(disc_rows)

        if all_core:
            # 1) semantic text + embeddings
            _attach_semantic_fields(all_core, self.embedder)

            # 2) classification — МУТИРУЕТ DICT ДО INSERT
            #    category поля войдут в bulk_insert вместе с транзакцией
            try:
                self.category_service.classify_rows(all_core)
            except Exception:
                log.exception("Category classification failed — non-fatal")

        # 3) INSERT в БД (category уже в каждом dict)
        self.writer.bulk_insert_core_dedup(all_core)
        self.writer.bulk_insert_ext(all_ext)
        if all_disc:
            self.discovery.log(all_disc)
        self.writer.mark_parsed(file_id=file_id)

        # 4) semantic catalog + auto cluster rebuild
        self.semantic_service.after_ingest(all_core)

        result = {
            "file_id":             file_id,
            "bank":                bank,
            "filename":            filename,
            "statements":          statements_count,
            "core_rows":           len(all_core),
            "ext_rows":            len(all_ext),
            "discovery_cols":      len({d["raw_column_name"] for d in all_disc}) if all_disc else 0,
            "semantic_embedded":   self.embedder.enabled,
            "categories_assigned": sum(1 for r in all_core
                                       if r.get("transaction_category") not in (None, "OTHER")),
        }
        log.info("Ingested %s -> %s", filename, result)
        return result
