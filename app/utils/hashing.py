from __future__ import annotations
import hashlib
import numpy as np
from typing import List
from app.utils.text_utils import norm_text


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_header_fingerprint(headers: List[str]) -> str:
    normalized = "|".join(sorted([norm_text(h) for h in headers if norm_text(h)]))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    denom = float(np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0.0:
        return 0.0
    return float(np.dot(a, b) / denom)
