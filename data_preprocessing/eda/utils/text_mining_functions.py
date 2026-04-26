# ------------------------------
# Text mining utility functions
# ------------------------------

import re
import ast
from typing import Iterable, List, Tuple
from collections import Counter
import pandas as pd


def word_tokenize(text: str, min_len: int = 2) -> List[str]:
    # Split on non-letters, lowercase, filter length
    tokens = re.split(r"[^a-zA-Z]+", text.lower())
    return [t for t in tokens if len(t) >= min_len]


def remove_stopwords(tokens: Iterable[str], stopwords: Iterable[str]) -> List[str]:
    sw = set(stopwords)
    return [t for t in tokens if t not in sw]


def top_n(counter: Counter, n: int) -> List[Tuple[str, int]]:
    return counter.most_common(n)


def bigrams(tokens: List[str]) -> List[Tuple[str, str]]:
    return [(tokens[i], tokens[i+1]) for i in range(len(tokens)-1)]


def parse_skills_cell(cell) -> List[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    if isinstance(cell, list):
        return [str(x).strip() for x in cell if str(x).strip()]
    s = str(cell).strip()
    if not s:
        return []
    # Try literal eval for list-like content
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, (list, tuple)):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    # Fallback: split by common delimiters
    parts = re.split(r"[;,]", s)
    return [p.strip() for p in parts if p.strip()]


def summarize_text_lengths(series: pd.Series) -> pd.DataFrame:
    lens = series.fillna("").astype(str).map(len)
    words = series.fillna("").astype(str).map(lambda s: len([t for t in re.split(r"\s+", s.strip()) if t]))
    sents = series.fillna("").astype(str).map(lambda s: len([x for x in re.split(r"[.!?]+", s) if x.strip()]))
    return pd.DataFrame({"char_len": lens, "word_count": words, "sentence_count": sents})