from typing import Iterable, List
import re
import pandas as pd

def clean_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
    return df


def get_present_columns(df: pd.DataFrame, candidates: Iterable[str]) -> List[str]:
    return [c for c in candidates if c in df.columns]


def is_nonempty_str(x) -> bool:
    return isinstance(x, str) and len(x.strip()) > 0