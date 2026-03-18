from __future__ import annotations

import uuid
from typing import List, Optional
import numpy as np

from app.ingestion.mapping.embedding_mapper import EmbeddingBackend
from app.utils.hashing import compute_header_fingerprint
from app.utils.text_utils import norm_text


class FormatRegistryService:
    def __init__(
        self, writer, embedder: EmbeddingBackend, similarity_threshold: float = 0.92
    ):
        self.writer = writer
        self.embedder = embedder
        self.similarity_threshold = similarity_threshold

    def register_or_get_format(self, source_bank: str, headers: List[str]) -> str:
        fp = compute_header_fingerprint(headers)

        existing = self.writer.get_format_by_fingerprint(fp)
        if existing:
            self.writer.bump_format_usage(existing)
            return existing

        emb_bytes: Optional[bytes] = None

        headers_norm = [norm_text(h) for h in headers if norm_text(h)]

        if self.embedder.enabled and headers_norm:
            header_text = " | ".join(headers_norm)
            vec = self.embedder.embed([header_text])[0]  # normalized vector
            emb_bytes = EmbeddingBackend.vec_to_bytes(vec)

            candidates = self.writer.load_format_vectors(
                source_bank=source_bank
            ) or self.writer.load_format_vectors(None)

            best_id = None
            best_sim = -1.0

            for c in candidates:
                b = c.get("embedding_vector")
                if not b:
                    continue
                v2 = EmbeddingBackend.bytes_to_vec(b)
                sim = float(np.dot(vec, v2))  # cosine (normalized dot)
                if sim > best_sim:
                    best_sim = sim
                    best_id = c.get("format_id")

            if best_id is not None and best_sim >= self.similarity_threshold:
                self.writer.bump_format_usage(str(best_id))
                return str(best_id)

        new_id = str(uuid.uuid4())
        self.writer.insert_new_format(
            format_id=new_id,
            source_bank=source_bank,
            fp=fp,
            header_sample={"headers": headers_norm},
            embedding_vector=emb_bytes,
        )
        return new_id
