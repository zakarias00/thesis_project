from typing import Iterable, List, Tuple, Dict, Optional

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from utils.io_functions import savefig

def save_barplot(counts_df: pd.DataFrame, x: str, y: str, title: str, outfile: str, top_n: Optional[int] = None, rotate: int = 45):
    df = counts_df.copy()
    if top_n is not None and len(df) > top_n:
        df = df.iloc[:top_n]
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x=x, y=y, color="#1f77b4")
    plt.title(title)
    plt.xticks(rotation=rotate, ha="right")
    plt.xlabel(x.replace("_", " ").title())
    plt.ylabel(y.replace("_", " ").title())
    savefig(outfile)