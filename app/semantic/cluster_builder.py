"""
app/semantic/cluster_builder.py  v4

Improvements:
  1. Aggressive noise cleaning — IDs, IBAN, dates, names, amounts
  2. Op-type normalisation — raw strings → clean business labels
  3. 20-rule smart label engine → 12 canonical categories
  4. _merge_duplicate_clusters with 0.85 threshold (was 0.97)
  5. DROP INDEX IF EXISTS before rebuild
"""
from __future__ import annotations
import json, logging, re as _re, uuid
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


# ─── 1. Noise regexes ────────────────────────────────────────────────────────

_RE_CONTRACT   = _re.compile(r'[№#]?\s*[A-Za-zА-Яа-яЁё]{0,3}\d{5,}[-–]\d+', _re.I)
_RE_SHORT_ID   = _re.compile(r'\b[RrLlDdNn]\d{5,}\b')
_RE_IBAN       = _re.compile(r'\bKZ[A-Z0-9]{16,}\b|\b\d{16,}\b', _re.I)
_RE_LONG_NUM   = _re.compile(r'\b\d{8,}\b')
_RE_DATE       = _re.compile(r'\b\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,5}\b')
_RE_SHORT_NUM  = _re.compile(r'(?<!\w)\d{1,7}(?!\w)')
_RE_AMOUNT     = _re.compile(r'\b\d[\d\s,\.]*\d\s*(KZT|USD|EUR|RUB|тенге|₸)?\b', _re.I)
_RE_DOG        = _re.compile(r'(по\s+)?договор[уа]?\s*[№#]?\s*\S+', _re.I)
_RE_NAMES      = _re.compile(r'([А-ЯЁ][а-яё]{2,}\s+){1,}[А-ЯЁ][а-яё]{2,}')
_RE_LATIN_NAME = _re.compile(r'\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}\b')
_RE_PIPES      = _re.compile(r'\s*\|\s*')
_RE_SPACES     = _re.compile(r'[\s,;:\-]{2,}')


def _clean_for_embedding(raw: str) -> str:
    t = raw
    t = _RE_CONTRACT.sub(' ', t)
    t = _RE_SHORT_ID.sub(' ', t)
    t = _RE_IBAN.sub(' ', t)
    t = _RE_DOG.sub(' ', t)
    t = _RE_DATE.sub(' ', t)
    t = _RE_LONG_NUM.sub(' ', t)
    t = _RE_AMOUNT.sub(' ', t)
    t = _RE_SHORT_NUM.sub(' ', t)
    t = _RE_NAMES.sub(' ', t)
    t = _RE_LATIN_NAME.sub(' ', t)
    t = _RE_PIPES.sub(' ', t)
    t = _RE_SPACES.sub(' ', t)
    return t.strip()


# ─── 2. Op-type normalisation ─────────────────────────────────────────────────

_OP_MAP = [
    (_re.compile(r'card.*списани|card.*debit',                    _re.I), 'списание с карты'),
    (_re.compile(r'card.*пополнен|card.*credit',                  _re.I), 'пополнение карты'),
    (_re.compile(r'p2p',                                           _re.I), 'p2p перевод'),
    (_re.compile(r'atm|банкомат',                                  _re.I), 'снятие наличных'),
    (_re.compile(r'исх\.?\s*\(?дебет\)?|исх\.?doc|outgoing',      _re.I), 'исходящий платёж'),
    (_re.compile(r'вх\.?\s*\(?кредит\)?|вх\.?doc|incoming',       _re.I), 'входящий платёж'),
]

def _normalise_op(op: str) -> str:
    for pat, label in _OP_MAP:
        if pat.search(op):
            return label
    return op.strip()


# ─── 3. Canonical build_semantic_text ─────────────────────────────────────────

def build_semantic_text(
    operation_type_raw: Optional[str] = None,
    sdp_name:           Optional[str] = None,
    purpose_text:       Optional[str] = None,
    raw_note:           Optional[str] = None,
) -> Optional[str]:
    op      = _normalise_op((operation_type_raw or '').strip())
    sdp     = (sdp_name or '').strip()
    purpose = _clean_for_embedding((purpose_text or '').strip())
    note    = _clean_for_embedding((raw_note    or '').strip())

    parts = [op, sdp, purpose]
    if note and note.lower() != purpose.lower() and len(note) > 5:
        parts.append(note)

    non_empty = [p for p in parts if p]
    if not non_empty:
        return None

    deduped: List[str] = []
    for p in non_empty:
        if not deduped or p.lower() != deduped[-1].lower():
            deduped.append(p)
    return ' | '.join(deduped)


# ─── 4. Smart labelling ───────────────────────────────────────────────────────

CANONICAL_CATEGORIES = [
    'P2P перевод', 'Списание с карты', 'Пополнение карты',
    'Снятие наличных', 'Возврат средств', 'Комиссия банка',
    'Зарплата', 'Гос. выплаты', 'Налоговый платёж',
    'Кредит / займ', 'Оплата товаров/услуг', 'Прочие операции',
]

_LABEL_RULES: List[Tuple] = [
    (_re.compile(r'p2p|перевод между счет|между своим',                            _re.I), 'P2P перевод'),
    (_re.compile(r'списани.*карт|card.*debit|card.*списани|снятие.*карт',           _re.I), 'Списание с карты'),
    (_re.compile(r'пополнен.*карт|card.*credit|card.*пополнен|зачислен.*карт',     _re.I), 'Пополнение карты'),
    (_re.compile(r'снятие.*(наличн|atm)|банкомат|atm',                             _re.I), 'Снятие наличных'),
    (_re.compile(r'возврат|refund|непредоставлен|chargeback',                       _re.I), 'Возврат средств'),
    (_re.compile(r'комисси|обслуживани|сбор.*банк|commission',                     _re.I), 'Комиссия банка'),
    (_re.compile(r'зарплат|оклад|salary|payroll|аванс',                            _re.I), 'Зарплата'),
    (_re.compile(r'пенсион|пенсия|енпф|соцвыплат|пособи|алимент|гос.*выплат',     _re.I), 'Гос. выплаты'),
    (_re.compile(r'налог|ндс|кпн|иис|ипн|tax|фискал|штраф|пеня',                  _re.I), 'Налоговый платёж'),
    (_re.compile(r'займ|заем|погашен|долг|loan|repayment|депозит|вклад',           _re.I), 'Кредит / займ'),
    (_re.compile(r'аренд|rent|лизинг|коммун|электро|водо|газ|utilities',           _re.I), 'Оплата товаров/услуг'),
    (_re.compile(r'оплат.*товар|оплат.*услуг|покупк|магазин|kaspi.*продаж|kaspi pay', _re.I), 'Оплата товаров/услуг'),
    (_re.compile(r'страховани|insurance',                                            _re.I), 'Оплата товаров/услуг'),
]

_STOP = {
    'платеж','платежа','платежей','платежи','оплата','оплаты',
    'перевод','перевода','операция','операции','документ',
    'kaspi','halyk','банк','банка','card','через','между',
    'кредит','дебет','исх','вх','doc','dok','from','with',
    'счет','счёт','сумм','сумма','этот','этого','этой',
}


def _smart_label(texts: List[str]) -> str:
    combined = ' '.join(texts[:100]).lower()
    for pat, label in _LABEL_RULES:
        if pat.search(combined):
            return label
    tokens = _top_tokens(texts, 5)
    return ' / '.join(tokens[:3]) if tokens else 'Прочие операции'


def _top_tokens(texts: List[str], n: int = 8) -> List[str]:
    counter: Counter = Counter()
    for t in texts:
        for tok in _re.split(r'[\s|,;:]+', _clean_for_embedding(t).lower()):
            tok = tok.strip('.,;:!?"\'()')
            if len(tok) >= 4 and not tok.isdigit() and tok not in _STOP:
                counter[tok] += 1
    return [w for w, _ in counter.most_common(n)]


# ─── 5. Cluster deduplication ─────────────────────────────────────────────────

def _merge_clusters(clusters: List[Dict], threshold: float = 0.85) -> List[Dict]:
    if len(clusters) < 2:
        return clusters

    cents = []
    for c in clusters:
        v = np.fromstring(c['centroid_embedding'].strip('[]'), sep=',', dtype=np.float32)
        norm = np.linalg.norm(v)
        cents.append(v / norm if norm > 1e-9 else v)

    mask   = [False] * len(clusters)
    result = []

    for i in range(len(clusters)):
        if mask[i]:
            continue
        base     = clusters[i].copy()
        samples  = list(base['sample_texts'])
        for j in range(i + 1, len(clusters)):
            if mask[j]:
                continue
            sim = float(np.dot(cents[i], cents[j]))
            if sim >= threshold:
                mask[j] = True
                base['tx_count'] += clusters[j]['tx_count']
                samples += clusters[j]['sample_texts']
                if clusters[j]['tx_count'] > base['tx_count']:
                    base['cluster_label'] = clusters[j]['cluster_label']
                log.info("Merged '%s' + '%s' sim=%.3f",
                         clusters[i]['cluster_label'],
                         clusters[j]['cluster_label'], sim)
        base['sample_texts'] = list(dict.fromkeys(samples))[:10]
        result.append(base)

    log.info("Cluster merge: %d → %d (threshold=%.2f)",
             len(clusters), len(result), threshold)
    return result


# ─── ClusterBuilder ───────────────────────────────────────────────────────────

def _vec_to_pg(vec: np.ndarray) -> str:
    return '[' + ','.join(f'{v:.6f}' for v in np.asarray(vec, dtype=np.float32).reshape(-1)) + ']'


class ClusterBuilder:
    def __init__(
        self,
        engine: Engine,
        k_min: int = 8,
        k_max: Optional[int] = None,
        k_step: int = 5,
        n_init: int = 10,
        max_rows: int = 200_000,
        merge_threshold: float = 0.85,
    ):
        self.engine = engine
        self.k_min = k_min
        self.k_max = k_max
        self.k_step = k_step
        self.n_init = n_init
        self.max_rows = max_rows
        self.merge_threshold = merge_threshold

    def run(self, source_bank: Optional[str] = None) -> int:
        log.info("Loading embeddings (bank=%s)...", source_bank or 'all')
        ids, texts, matrix = self._load_embeddings(source_bank)
        n = len(ids)
        if n < self.k_min * 2:
            log.warning("Too few records (%d) to cluster — skipping", n)
            return 0

        log.info("L2-normalising %d vectors (dim=%d)...", n, matrix.shape[1])
        X = self._normalise(matrix)

        best_k = self._select_k(X, n)
        log.info("Optimal k=%d selected", best_k)

        labels   = self._kmeans_fit(X, best_k)
        clusters = self._aggregate(ids, texts, X, labels)
        self._save_clusters(clusters, source_bank)
        self._rebuild_index(n)
        return len(clusters)

    def _load_embeddings(self, bank):
        sql    = ("SELECT id::text, COALESCE(semantic_text,text,'') AS t, embedding "
                  "FROM afm.semantic_catalog WHERE embedding IS NOT NULL")
        params: Dict[str, Any] = {}
        if bank:
            sql += " AND source_bank = :b"; params['b'] = bank
        sql += " LIMIT :lim"; params['lim'] = self.max_rows

        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        if not rows:
            return [], [], np.empty((0, 1))

        ids, texts, vecs = [], [], []
        for r in rows:
            ids.append(r[0]); texts.append(r[1])
            raw = r[2]
            if isinstance(raw, str):
                vecs.append(np.fromstring(raw.strip('[]'), sep=',', dtype=np.float32))
            elif isinstance(raw, (bytes, memoryview)):
                vecs.append(np.frombuffer(bytes(raw), dtype=np.float32))
            else:
                vecs.append(np.asarray(raw, dtype=np.float32))
        return ids, texts, np.vstack(vecs)

    @staticmethod
    def _normalise(matrix):
        from sklearn.preprocessing import normalize
        return normalize(matrix, norm='l2').astype(np.float32)

    def _select_k(self, X, n):
        from sklearn.cluster import KMeans
        from sklearn.metrics import davies_bouldin_score
        hartigan  = max(self.k_min, int(np.sqrt(n / 2)))
        k_max_eff = min(self.k_max or hartigan, 60, n // 2)
        k_max_eff = max(k_max_eff, self.k_min)
        candidates = list(range(self.k_min, k_max_eff + 1, self.k_step))
        if k_max_eff not in candidates:
            candidates.append(k_max_eff)
        log.info("DBI grid: %s (Hartigan=%d)", candidates, hartigan)
        best_k, best_dbi = self.k_min, float('inf')
        for k in candidates:
            try:
                km  = KMeans(n_clusters=k, random_state=42, n_init=3, max_iter=200)
                lbl = km.fit_predict(X)
                if len(set(lbl)) < 2:
                    continue
                dbi = davies_bouldin_score(X, lbl)
                log.info("  k=%d  DBI=%.4f", k, dbi)
                if dbi < best_dbi:
                    best_dbi, best_k = dbi, k
            except Exception as e:
                log.warning("  k=%d failed: %s", k, e)
        return best_k

    def _kmeans_fit(self, X, k):
        from sklearn.cluster import KMeans
        return KMeans(n_clusters=k, random_state=42,
                      n_init=self.n_init, max_iter=300).fit_predict(X)

    def _aggregate(self, ids, texts, X, labels):
        groups: Dict[int, List[int]] = defaultdict(list)
        for idx, lbl in enumerate(labels):
            groups[int(lbl)].append(idx)

        clusters = []
        for lbl, indices in groups.items():
            ctexts = [texts[i] for i in indices]
            cvecs  = X[indices]
            c = cvecs.mean(axis=0)
            nrm = np.linalg.norm(c)
            if nrm > 1e-9:
                c = c / nrm
            clusters.append({
                'cluster_id':         str(uuid.uuid4()),
                'cluster_label':      _smart_label(ctexts),
                'cluster_keywords':   _top_tokens(ctexts, 8),
                'centroid_embedding': _vec_to_pg(c),
                'sample_texts':       ctexts[:10],
                'tx_count':           len(indices),
            })

        return _merge_clusters(clusters, self.merge_threshold)

    def _save_clusters(self, clusters, bank):
        if not clusters:
            return
        with self.engine.begin() as conn:
            if bank:
                conn.execute(text("DELETE FROM afm.semantic_clusters WHERE source_bank=:b"), {'b': bank})
            else:
                conn.execute(text("DELETE FROM afm.semantic_clusters"))
            for c in clusters:
                conn.execute(text("""
                    INSERT INTO afm.semantic_clusters
                      (cluster_id, source_bank, cluster_label, cluster_keywords,
                       centroid_embedding, sample_texts, tx_count)
                    VALUES (CAST(:cid AS uuid),:bank,:label,
                            CAST(:kw AS jsonb), CAST(:emb AS vector),
                            CAST(:st AS jsonb), :cnt)
                """), {
                    'cid':   c['cluster_id'], 'bank': bank,
                    'label': c['cluster_label'],
                    'kw':    json.dumps(c['cluster_keywords'], ensure_ascii=False),
                    'emb':   c['centroid_embedding'],
                    'st':    json.dumps(c['sample_texts'],     ensure_ascii=False),
                    'cnt':   c['tx_count'],
                })
        log.info("Saved %d clusters (bank=%s)", len(clusters), bank or 'all')

    def _rebuild_index(self, n_rows):
        lists = max(1, min(20, int(n_rows ** 0.5)))
        idx   = 'idx_sem_cl_emb'
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP INDEX IF EXISTS {idx};"))
                conn.execute(text(
                    f"CREATE INDEX {idx} ON afm.semantic_clusters "
                    f"USING ivfflat (centroid_embedding vector_cosine_ops) "
                    f"WITH (lists={lists});"
                ))
            log.info("Rebuilt %s (lists=%d)", idx, lists)
        except Exception as e:
            log.warning("Could not rebuild %s: %s", idx, e)
