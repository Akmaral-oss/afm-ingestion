from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod

import httpx

_CODE_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


class LLMBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        raise NotImplementedError


class OllamaBackend(LLMBackend):
    def __init__(
        self,
        model: str = "qwen2.5-coder:14b",
        base_url: str = "http://localhost:11434",
        timeout_s: int = 120,
    ):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout_s = None if timeout_s <= 0 else timeout_s

    def generate(self, prompt: str, max_new_tokens: int = 512) -> str:
        response = httpx.post(
            f"{self.base_url}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_new_tokens},
            },
            timeout=self.timeout_s,
        )
        response.raise_for_status()
        return response.json()["response"]


class GeminiBackend(LLMBackend):
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
            raise RuntimeError(f"{self.api_key_env} is not set")

        client = genai.Client(api_key=api_key)
        contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
        config = types.GenerateContentConfig(
            tools=[types.Tool(googleSearch=types.GoogleSearch())],
        )

        chunks: list[str] = []
        for chunk in client.models.generate_content_stream(
            model=self.model,
            contents=contents,
            config=config,
        ):
            text_chunk = getattr(chunk, "text", None)
            if text_chunk:
                chunks.append(text_chunk)
        output = "".join(chunks).strip()
        if not output:
            raise RuntimeError("Gemini returned empty output")
        return output


def build_llm_backend(
    model_name: str,
    *,
    base_url: str = "http://localhost:11434",
    timeout_s: int = 120,
) -> LLMBackend:
    raw = (model_name or "").split("#", 1)[0].strip()
    lower = raw.lower()

    if lower == "gemini":
        return GeminiBackend(model="gemini-3-flash-preview")
    if lower.startswith("gemini:"):
        return GeminiBackend(model=raw.split(":", 1)[1].strip() or "gemini-3-flash-preview")
    if lower.startswith("gemini"):
        return GeminiBackend(model=raw)
    return OllamaBackend(
        model=raw or "qwen2.5-coder:14b",
        base_url=base_url,
        timeout_s=timeout_s,
    )


class SQLGenerator:
    def __init__(self, backend: LLMBackend, max_new_tokens: int = 512):
        self.backend = backend
        self.max_new_tokens = max_new_tokens

    def generate(self, prompt: str) -> str:
        raw = self.backend.generate(prompt, max_new_tokens=self.max_new_tokens)
        return self._clean(raw)

    @staticmethod
    def _clean(raw: str) -> str:
        match = _CODE_FENCE_RE.search(raw)
        if match:
            return match.group(1).strip()
        upper = raw.upper()
        index = upper.find("SELECT")
        if index >= 0:
            return raw[index:].strip()
        return raw.strip()
