from __future__ import annotations

"""
SQL Generator.

Thin wrapper around the local LLM (Llama-4-Scout or Qwen2.5-Coder-14B).
Responsibility: send the prompt, return a clean SQL string.

Integration point — set up your model backend in __init__ and implement
`_call_llm`.  Two example backends are provided:
  - OllamaBackend  (local Ollama server)
  - HuggingFaceBackend (transformers pipeline, air-gap friendly)
"""

import logging
import re
from abc import ABC, abstractmethod
from typing import List

log = logging.getLogger(__name__)

_CODE_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract backend
# ─────────────────────────────────────────────────────────────────────────────

class LLMBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, max_new_tokens: int = 512) -> str: ...


# ─────────────────────────────────────────────────────────────────────────────
# Ollama backend  (recommended for local / on-prem deployment)
# ─────────────────────────────────────────────────────────────────────────────

class OllamaBackend(LLMBackend):
    """
    Calls a locally running Ollama server.

    Usage:
        backend = OllamaBackend(model="llama4-scout:latest")
        # or:
        backend = OllamaBackend(model="qwen2.5-coder:14b")
    """
    def __init__(self, model: str = "llama4-scout:latest", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        import requests
        resp = requests.post(
            f"{self.base_url}/api/generate",
            json={"model": self.model, "prompt": prompt, "stream": False},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["response"]


# ─────────────────────────────────────────────────────────────────────────────
# HuggingFace backend  (air-gap / local weights)
# ─────────────────────────────────────────────────────────────────────────────

class HuggingFaceBackend(LLMBackend):
    """
    Uses a local transformers pipeline.

    Usage:
        backend = HuggingFaceBackend("Qwen/Qwen2.5-Coder-14B-Instruct")
    """
    def __init__(self, model_name_or_path: str):
        from transformers import pipeline  # type: ignore
        self._pipe = pipeline(
            "text-generation",
            model=model_name_or_path,
            device_map="auto",
            trust_remote_code=True,
        )

    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        out = self._pipe(prompt, max_new_tokens=max_new_tokens, do_sample=False)
        return out[0]["generated_text"][len(prompt):]


# ─────────────────────────────────────────────────────────────────────────────
# SQL Generator
# ─────────────────────────────────────────────────────────────────────────────

class SQLGenerator:
    def __init__(self, backend: LLMBackend):
        self.backend = backend

    def generate(self, prompt: str) -> str:
        raw = self.backend.generate(prompt)
        clean_sql = self._clean(raw)
        return self._deduplicate_where_conditions(clean_sql)

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _clean(raw: str) -> str:
        """Strip markdown fences and leading/trailing whitespace."""
        # Extract from ```sql ... ``` if present
        m = _CODE_FENCE_RE.search(raw)
        if m:
            return m.group(1).strip()
        # Fallback: take everything after first SELECT
        upper = raw.upper()
        idx = upper.find("SELECT")
        if idx >= 0:
            return raw[idx:].strip()
        return raw.strip()

    @staticmethod
    def _deduplicate_where_conditions(sql: str) -> str:
        """
        Remove duplicate LIKE conditions in WHERE clause.
        Example: WHERE (... LIKE '%x%' OR ... LIKE '%x%' OR ...)
        becomes: WHERE (... LIKE '%x%' OR ...)
        """
        lines = sql.split('\n')
        deduplicated_lines: List[str] = []
        seen_conditions: set = set()

        for line in lines:
            stripped = line.strip()
            # For LIKE conditions, extract the pattern and track uniqueness
            if 'LIKE' in stripped and '%' in stripped:
                # Extract the like pattern (e.g., '%налог%')
                like_match = re.search(r"LIKE\s+'([^']+)'", stripped, re.IGNORECASE)
                if like_match:
                    pattern = like_match.group(1)
                    # Check if this pattern was already added
                    if pattern in seen_conditions:
                        continue  # Skip duplicate
                    seen_conditions.add(pattern)
            deduplicated_lines.append(line)

        return '\n'.join(deduplicated_lines)
