from __future__ import annotations

import logging
from typing import List, Optional

import httpx
import numpy as np

log = logging.getLogger(__name__)
_DEFAULT_OLLAMA_EMBEDDING_MODEL = "mxbai-embed-large"


class EmbeddingBackend:
    def __init__(
        self,
        model_name_or_path: Optional[str] = None,
        provider: Optional[str] = None,
        ollama_base_url: str = "http://localhost:11434",
        ollama_timeout_s: int = 60,
    ):
        self.enabled = False
        self.model = None
        self.dim = None
        self.model_name_or_path = model_name_or_path
        self.ollama_base_url = ollama_base_url.rstrip("/")
        self.ollama_timeout_s = None if ollama_timeout_s <= 0 else ollama_timeout_s
        self._resolved_provider = "disabled"

        if provider is None:
            provider = "sentence-transformers" if model_name_or_path else "disabled"
        self.provider = provider.strip().lower()

        if self.provider in {"disabled", "disable", "none", "off"}:
            return

        if self.provider in {"ollama", "auto"}:
            ollama_model = model_name_or_path or _DEFAULT_OLLAMA_EMBEDDING_MODEL
            if self._init_ollama(ollama_model):
                return

            if model_name_or_path and self._init_sentence_transformers(model_name_or_path):
                log.warning(
                    "Ollama embedding init failed for model '%s'; using sentence-transformers '%s'.",
                    ollama_model,
                    model_name_or_path,
                )
                return

            log.warning("Embeddings disabled: could not initialize Ollama or fallback local model.")
            return

        if self.provider in {"sentence-transformers", "sentence_transformers", "local", "hf"}:
            if model_name_or_path and self._init_sentence_transformers(model_name_or_path):
                return
            log.warning(
                "Embeddings disabled: provider '%s' requires model path/name.",
                self.provider,
            )
            return

        log.warning("Embeddings disabled: unknown provider '%s'.", self.provider)

    def embed(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, 1), dtype=np.float32)

        if not self.enabled:
            return np.zeros((len(texts), 1), dtype=np.float32)

        if self._resolved_provider == "ollama":
            return self._embed_ollama(texts)

        if self.model is None:
            return np.zeros((len(texts), 1), dtype=np.float32)

        vecs = self.model.encode(texts, normalize_embeddings=True)
        return np.asarray(vecs, dtype=np.float32)

    def _init_ollama(self, model_name: str) -> bool:
        try:
            vector = self._embed_ollama_single(model_name, "health check")
            self.model_name_or_path = model_name
            self.dim = int(vector.shape[0])
            self.enabled = True
            self._resolved_provider = "ollama"
            return True
        except Exception as exc:
            log.warning("Ollama embeddings init failed for model '%s': %s", model_name, exc)
            return False

    def _init_sentence_transformers(self, model_name_or_path: str) -> bool:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self.model = SentenceTransformer(model_name_or_path)
            self.enabled = True
            self._resolved_provider = "sentence-transformers"
            self.model_name_or_path = model_name_or_path
            vectors = self.model.encode(["test"], normalize_embeddings=True)
            self.dim = int(vectors.shape[1])
            return True
        except Exception as exc:
            log.warning("Sentence-transformers init failed for '%s': %s", model_name_or_path, exc)
            return False

    def _embed_ollama(self, texts: List[str]) -> np.ndarray:
        try:
            response = httpx.post(
                f"{self.ollama_base_url}/api/embed",
                json={"model": self.model_name_or_path, "input": texts},
                timeout=self.ollama_timeout_s,
            )
            response.raise_for_status()
            payload = response.json()
            embeddings = payload.get("embeddings")
            if isinstance(embeddings, list) and embeddings and isinstance(embeddings[0], list):
                return np.asarray(embeddings, dtype=np.float32)
        except Exception:
            log.debug("Ollama batch embedding endpoint unavailable, falling back to single calls", exc_info=True)

        vectors = [
            self._embed_ollama_single(
                self.model_name_or_path or _DEFAULT_OLLAMA_EMBEDDING_MODEL,
                text,
            )
            for text in texts
        ]
        return np.asarray(vectors, dtype=np.float32)

    def _embed_ollama_single(self, model_name: str, text: str) -> np.ndarray:
        response = httpx.post(
            f"{self.ollama_base_url}/api/embeddings",
            json={"model": model_name, "prompt": text},
            timeout=self.ollama_timeout_s,
        )
        response.raise_for_status()
        payload = response.json()
        embedding = payload.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("Ollama did not return a valid embedding vector.")
        return np.asarray(embedding, dtype=np.float32)

    @staticmethod
    def vec_to_bytes(vec: np.ndarray) -> bytes:
        value = np.asarray(vec, dtype=np.float32).reshape(-1)
        return value.tobytes(order="C")

    @staticmethod
    def bytes_to_vec(b: bytes) -> np.ndarray:
        return np.frombuffer(b, dtype=np.float32)
