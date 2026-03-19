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
import os
import re
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)

_CODE_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract backend
# ─────────────────────────────────────────────────────────────────────────────

class LLMBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, max_new_tokens: int = 512) -> str: ...
    
    @abstractmethod
    async def agenerate(self, prompt: str, max_new_tokens: int = 512) -> str: ...

    async def astream(self, prompt: str, max_new_tokens: int = 512):
        """Async generator yielding chunks."""
        yield await self.agenerate(prompt, max_new_tokens)



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
    def __init__(
        self,
        model: str = "qwen2.5-coder:14b",
        base_url: str = "http://localhost:11434",
        timeout_s: int = 120,
    ):
        self.model = model
        self.base_url = base_url
        # requests timeout=None means no timeout.
        self.timeout_s = None if timeout_s <= 0 else timeout_s

    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        import requests
        resp = requests.post(
            f"{self.base_url}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_new_tokens},
            },
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()["response"]

    async def agenerate(self, prompt: str, max_new_tokens: int = 512) -> str:
        import httpx
        timeout = httpx.Timeout(self.timeout_s) if self.timeout_s else None
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": max_new_tokens},
                },
            )
            resp.raise_for_status()
            return resp.json()["response"]

    async def astream(self, prompt: str, max_new_tokens: int = 512):
        import httpx
        import json
        timeout = httpx.Timeout(self.timeout_s) if self.timeout_s else None
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": True,
                    "options": {"num_predict": max_new_tokens},
                },
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_lines():
                    if chunk:
                        payload = json.loads(chunk)
                        yield payload.get("response", "")


# ─────────────────────────────────────────────────────────────────────────────
# Gemini backend  (temporary testing backend)
# ─────────────────────────────────────────────────────────────────────────────

class GeminiBackend(LLMBackend):
    """
    Calls Gemini via google-genai.

    API key is read from process env: GEMINI_API_KEY
    """

    def __init__(
        self,
        model: str = "gemini-3-flash-preview",
        api_key_env: str = "GEMINI_API_KEY",
    ):
        self.model = model
        self.api_key_env = api_key_env

    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        try:
            from google import genai  # type: ignore
            from google.genai import types  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "google-genai is not installed. Run: pip install google-genai"
            ) from exc

        api_key = os.environ.get(self.api_key_env)
        if not api_key:
            raise RuntimeError(
                f"{self.api_key_env} is not set. Export it in your shell before running Gemini."
            )

        client = genai.Client(api_key=api_key)

        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=prompt)],
            )
        ]
        tools = [types.Tool(googleSearch=types.GoogleSearch())]
        generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level="HIGH",
            ),
            safety_settings=[
                types.SafetySetting(
                    category="HARM_CATEGORY_HARASSMENT",
                    threshold="BLOCK_NONE",
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_HATE_SPEECH",
                    threshold="BLOCK_NONE",
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                    threshold="BLOCK_NONE",
                ),
                types.SafetySetting(
                    category="HARM_CATEGORY_DANGEROUS_CONTENT",
                    threshold="BLOCK_NONE",
                ),
            ],
            tools=tools,
        )

        chunks: list[str] = []
        for chunk in client.models.generate_content_stream(
            model=self.model,
            contents=contents,
            config=generate_content_config,
        ):
            text_chunk = getattr(chunk, "text", None)
            if text_chunk:
                chunks.append(text_chunk)

        output = "".join(chunks).strip()
        if not output:
            raise RuntimeError("Gemini returned empty output.")
        return output

    async def agenerate(self, prompt: str, max_new_tokens: int = 512) -> str:
        # For simplicity, wrap synchronous generator in an executor or use native async if available
        import asyncio
        return await asyncio.to_thread(self.generate, prompt, max_new_tokens)

    async def astream(self, prompt: str, max_new_tokens: int = 512):
        try:
            from google import genai  # type: ignore
            from google.genai import types  # type: ignore
        except Exception as exc:
            raise RuntimeError("google-genai is not installed.") from exc

        api_key = os.environ.get(self.api_key_env)
        client = genai.Client(api_key=api_key)
        contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
        
        # We need async client for true streaming, but genai library might not support it cleanly
        # So we'll just yield the full generation as a single chunk if async streaming isn't natively trivial
        import asyncio
        # google.genai currently offers client.aio.models.generate_content_stream
        if hasattr(client, "aio"):
            async for chunk in await client.aio.models.generate_content_stream(
                model=self.model, contents=contents
            ):
                text_chunk = getattr(chunk, "text", None)
                if text_chunk:
                    yield text_chunk
        else:
            yield await self.agenerate(prompt, max_new_tokens)


def build_llm_backend(
    model_name: str,
    base_url: str = "http://localhost:11434",
    timeout_s: int = 120,
) -> LLMBackend:
    """
    Build an LLM backend from AFM_LLM_MODEL value.

    Gemini routing rules:
      - `gemini` -> use Gemini with default model gemini-3-flash-preview
      - `gemini:<model>` -> use the explicit Gemini model
      - model names starting with `gemini` -> use Gemini directly
    Otherwise, uses Ollama backend.
    """
    raw = (model_name or "").split("#", 1)[0].strip()
    lower = raw.lower()

    if lower == "gemini":
        return GeminiBackend(model="gemini-3-flash-preview")

    if lower.startswith("gemini:"):
        gemini_model = raw.split(":", 1)[1].strip() or "gemini-3-flash-preview"
        return GeminiBackend(model=gemini_model)

    if lower.startswith("gemini"):
        return GeminiBackend(model=raw)

    return OllamaBackend(model=raw or "qwen2.5-coder:14b", base_url=base_url, timeout_s=timeout_s)


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

    async def agenerate(self, prompt: str, max_new_tokens: int = 512) -> str:
        import asyncio
        return await asyncio.to_thread(self.generate, prompt, max_new_tokens)

    async def astream(self, prompt: str, max_new_tokens: int = 512):
        yield await self.agenerate(prompt, max_new_tokens)


# ─────────────────────────────────────────────────────────────────────────────
# SQL Generator
# ─────────────────────────────────────────────────────────────────────────────

class SQLGenerator:
    def __init__(self, backend: LLMBackend, max_new_tokens: int = 512):
        self.backend = backend
        self.max_new_tokens = max_new_tokens

    def generate(self, prompt: str) -> str:
        raw = self.backend.generate(prompt, max_new_tokens=self.max_new_tokens)
        return self._clean(raw)

    async def agenerate(self, prompt: str) -> str:
        raw = await self.backend.agenerate(prompt, max_new_tokens=self.max_new_tokens)
        return self._clean(raw)

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
