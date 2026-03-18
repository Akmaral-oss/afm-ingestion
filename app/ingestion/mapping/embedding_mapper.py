from __future__ import annotations
import numpy as np
from typing import List, Optional


class EmbeddingBackend:
    def __init__(self, model_name_or_path: Optional[str] = None):
        self.enabled = False
        self.model = None
        self.dim = None
        if model_name_or_path:
            try:
                from sentence_transformers import SentenceTransformer  # type: ignore

                self.model = SentenceTransformer(model_name_or_path)
                self.enabled = True
                v = self.model.encode(["test"], normalize_embeddings=True)
                self.dim = int(v.shape[1])
            except Exception as e:
                print(f"[WARN] Embeddings disabled: {e}")

    def embed(self, texts: List[str]) -> np.ndarray:
        if not self.enabled or self.model is None:
            return np.zeros((len(texts), 1), dtype=np.float32)
        vecs = self.model.encode(texts, normalize_embeddings=True)
        return np.asarray(vecs, dtype=np.float32)

    @staticmethod
    def vec_to_bytes(vec: np.ndarray) -> bytes:
        v = np.asarray(vec, dtype=np.float32).reshape(-1)
        return v.tobytes(order="C")

    @staticmethod
    def bytes_to_vec(b: bytes) -> np.ndarray:
        return np.frombuffer(b, dtype=np.float32)
