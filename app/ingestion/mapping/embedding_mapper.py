from __future__ import annotations

import logging
import numpy as np
from typing import List, Optional


log = logging.getLogger(__name__)
_DEFAULT_OLLAMA_EMBEDDING_MODEL = "mxbai-embed-large"


class EmbeddingBackend:
    def __init__(
        self,
        model_name_or_path: Optional[str] = None,
        provider: str = "ollama",
        ollama_base_url: str = "http://localhost:11434",
        ollama_timeout_s: int = 60,
    ):
        self.enabled = False
        self.model = None
        self.dim = None
        self.provider = (provider or "ollama").strip().lower()
        self.model_name_or_path = model_name_or_path
        self.ollama_base_url = ollama_base_url.rstrip("/")
        # requests timeout=None means no timeout.
        self.ollama_timeout_s = None if ollama_timeout_s <= 0 else ollama_timeout_s
        self._resolved_provider = "disabled"

        if self.provider in {"disabled", "disable", "none", "off"}:
            return

        if self.provider in {"ollama", "auto"}:
            ollama_model = model_name_or_path or _DEFAULT_OLLAMA_EMBEDDING_MODEL
            if self._init_ollama(ollama_model):
                return

            # Backward compatibility: if AFM_EMBEDDING_MODEL points to local weights,
            # transparently fall back to sentence-transformers.
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
                "Embeddings disabled: provider '%s' requires AFM_EMBEDDING_MODEL path/name.",
                self.provider,
            )
            return

        log.warning("Embeddings disabled: unknown provider '%s'.", self.provider)

    def embed(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return self._zero_embeddings(0)

        if not self.enabled:
            return self._zero_embeddings(len(texts))

        try:
            if self._resolved_provider == "ollama":
                return self._embed_ollama(texts)

            if self.model is None:
                return self._zero_embeddings(len(texts))

            vecs = self.model.encode(texts, normalize_embeddings=True)
            return np.asarray(vecs, dtype=np.float32)
        except Exception as e:
            log.warning("Embedding request failed; disabling embeddings for this runtime: %s", e)
            self.enabled = False
            self._resolved_provider = "disabled"
            return self._zero_embeddings(len(texts))

    def _init_ollama(self, model_name: str) -> bool:
        try:
            vec = self._embed_ollama_single(model_name, "health check")
            self.model_name_or_path = model_name
            self.dim = int(vec.shape[0])
            self.enabled = True
            self._resolved_provider = "ollama"
            return True
        except Exception as e:
            log.warning("Ollama embeddings init failed for model '%s': %s", model_name, e)
            return False

    def _init_sentence_transformers(self, model_name_or_path: str) -> bool:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self.model = SentenceTransformer(model_name_or_path)
            self.enabled = True
            self._resolved_provider = "sentence-transformers"
            self.model_name_or_path = model_name_or_path
            v = self.model.encode(["test"], normalize_embeddings=True)
            self.dim = int(v.shape[1])
            return True
        except Exception as e:
            log.warning("Sentence-transformers init failed for '%s': %s", model_name_or_path, e)
            return False

    def _embed_ollama(self, texts: List[str]) -> np.ndarray:
        # Prefer batch endpoint when available.
        try:
            import requests

            resp = requests.post(
                f"{self.ollama_base_url}/api/embed",
                json={"model": self.model_name_or_path, "input": texts},
                timeout=self.ollama_timeout_s,
            )
            resp.raise_for_status()
            payload = resp.json()
            embeddings = payload.get("embeddings")
            if isinstance(embeddings, list) and embeddings and isinstance(embeddings[0], list):
                return np.asarray(embeddings, dtype=np.float32)
        except Exception as e:
            log.warning("Ollama batch embedding failed, falling back to single requests: %s", e)

        vectors = [self._embed_ollama_single(self.model_name_or_path or _DEFAULT_OLLAMA_EMBEDDING_MODEL, t) for t in texts]
        return np.asarray(vectors, dtype=np.float32)

    def _embed_ollama_single(self, model_name: str, text: str) -> np.ndarray:
        import requests

        resp = requests.post(
            f"{self.ollama_base_url}/api/embeddings",
            json={"model": model_name, "prompt": text},
            timeout=self.ollama_timeout_s,
        )
        resp.raise_for_status()
        payload = resp.json()
        embedding = payload.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("Ollama did not return a valid embedding vector.")
        return np.asarray(embedding, dtype=np.float32)

    @staticmethod
    def vec_to_bytes(vec: np.ndarray) -> bytes:
        v = np.asarray(vec, dtype=np.float32).reshape(-1)
        return v.tobytes(order="C")

    @staticmethod
    def bytes_to_vec(b: bytes) -> np.ndarray:
        return np.frombuffer(b, dtype=np.float32)

    def _zero_embeddings(self, count: int) -> np.ndarray:
        width = int(self.dim or 1)
        return np.zeros((count, width), dtype=np.float32)
