# ----------------------------------------
# Utility functions for the IO operations
# ----------------------------------------

import os
import matplotlib.pyplot as plt


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def savefig(path: str, tight: bool = True) -> None:
    if tight:
        plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def write_markdown(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)