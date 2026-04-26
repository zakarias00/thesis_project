import argparse
import os
import sys
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

# Lazy torch import 
try:
    import torch  # noqa: F401
    TORCH_AVAILABLE = True
except Exception:
    TORCH_AVAILABLE = False

from sentence_transformers import SentenceTransformer


def detect_text_columns(df: pd.DataFrame) -> List[str]:
    # Prefer common text-like columns first if present
    preferred = [
        "course_title", "title",
        "combined_description", "description", "original_description",
        "extracted_skills"
    ]
    existing_preferred = [c for c in preferred if c in df.columns]

    # General text-like dtypes
    candidates = df.select_dtypes(include=["object", "string", "category"]).columns.tolist()

    # Keep order: preferred first, then the rest (without duplicates)
    ordered = existing_preferred + [c for c in candidates if c not in existing_preferred]
    return ordered


def build_row_text(row: pd.Series, cols: List[str], sep: str = " | ") -> str:
    parts = []
    for c in cols:
        val = row.get(c, "")
        if pd.isna(val):
            continue
        s = str(val).strip()
        if s:
            parts.append(f"{c}: {s}")
    return sep.join(parts)


def trim_text(s: str, max_chars: Optional[int]) -> str:
    if max_chars is None or max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[:max_chars]


def encode_texts(texts: List[str],
                 model: SentenceTransformer,
                 batch_size: int,
                 normalize: bool) -> np.ndarray:
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        convert_to_numpy=True,
        normalize_embeddings=normalize,
        show_progress_bar=True
    )
    return embeddings


def create_embeddings(
    input_csv: str,
    output_prefix: str,
    columns: Optional[List[str]] = None,
    mode: str = "row",
    batch_size: int = 64,
    normalize: bool = True,
    max_chars: int = 0,
    sample: int = 0,
) -> Tuple[str, str]:
    """Library-callable wrapper: create embeddings and return (emb_path, meta_path).

    Parameters
    ----------
    input_csv : str
        Path to the input CSV.
    output_prefix : str
        Output file prefix (without extension).
    columns : list[str] | None
        Column names to embed.  Auto-detects if None.
    mode : 'row' | 'cell'
    batch_size, normalize, max_chars, sample : see CLI args.

    Returns
    -------
    Tuple of (embeddings_npy_path, metadata_csv_path).
    """
    class _Args:
        pass
    args = _Args()
    args.input = input_csv
    args.output_prefix = output_prefix
    args.columns = ",".join(columns) if columns else ""
    args.mode = mode
    args.batch_size = batch_size
    args.normalize = normalize
    args.max_chars = max_chars
    args.sample = sample
    return _run_embedding(args)


def main():
    parser = argparse.ArgumentParser(description="Create embeddings for a CSV using all-MiniLM-L6-v2.")
    parser.add_argument("--input", "-i", type=str, required=True, help="Path to input CSV file.")
    parser.add_argument("--output-prefix", "-o", type=str, required=True, help="Output file prefix (without extension).")
    parser.add_argument("--columns", type=str, default="", help="Comma-separated column names to use as text fields. If omitted, auto-detects text columns.")
    parser.add_argument("--mode", type=str, choices=["row", "cell"], default="row", help="Embedding mode: 'row' or 'cell'.")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size for embedding.")
    parser.add_argument("--normalize", action="store_true", help="L2-normalize embeddings.")
    parser.add_argument("--max-chars", type=int, default=0, help="Trim each text to at most this many characters (0 = no trim).")
    parser.add_argument("--sample", type=int, default=0, help="For quick testing: only embed the first N items (0 = all).")
    args = parser.parse_args()
    _run_embedding(args)


def _run_embedding(args):
    """Core embedding logic used by both CLI main() and create_embeddings()."""

    if not os.path.exists(args.input):
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Load CSV
    df = pd.read_csv(args.input, engine="python", encoding="utf-8", on_bad_lines="skip")
    # Keep original column names for metadata; also build a lowercase mapping for detection
    lower_cols = {c: c for c in df.columns}  # identity mapping
    # For detection, create a normalized copy with lowercase colnames
    df_norm = df.copy()
    df_norm.columns = [c.strip().lower() for c in df_norm.columns]

    # Determine columns to use
    if args.columns.strip():
        # Respect user-provided columns; match case-insensitively
        requested = [c.strip() for c in args.columns.split(",") if c.strip()]
        selected_cols = []
        for rc in requested:
            # Try exact
            if rc in df.columns:
                selected_cols.append(rc)
                continue
            # Try lowercase match
            rc_lower = rc.lower()
            matches = [orig for orig in df.columns if orig.lower() == rc_lower]
            if matches:
                selected_cols.append(matches[0])
            else:
                print(f"WARNING: Column '{rc}' not found. Skipping.", file=sys.stderr)
        if not selected_cols:
            print("ERROR: No valid columns found from --columns.", file=sys.stderr)
            sys.exit(1)
    else:
        # Auto-detect from normalized df, but map back to original names
        detected_lower = detect_text_columns(df_norm)
        # Map detected lowercased names back to original names by case-insensitive match
        selected_cols = []
        for lc in detected_lower:
            matches = [orig for orig in df.columns if orig.lower() == lc]
            if matches:
                selected_cols.append(matches[0])
        if not selected_cols:
            # Fallback: use all columns as text
            selected_cols = df.columns.tolist()
            print("INFO: No text-like columns detected; using all columns.", file=sys.stderr)

    print(f"Using columns: {selected_cols}", file=sys.stderr)
    print(f"Mode: {args.mode}", file=sys.stderr)

    # Initialize model and device
    device = "cpu"
    if TORCH_AVAILABLE:
        try:
            import torch
            device = "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:
            device = "cpu"
    print(f"Loading model on device: {device}", file=sys.stderr)
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device=device)

    texts: List[str] = []
    meta_rows: List[dict] = []

    if args.mode == "row":
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Preparing row texts"):
            txt = build_row_text(row, selected_cols)
            txt = trim_text(txt, args.max_chars)
            if txt.strip():
                texts.append(txt)
                meta_rows.append({
                    "source": "row",
                    "row_index": int(idx),
                    "text": txt
                })
    elif args.mode == "cell":
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Preparing cell texts"):
            for c in selected_cols:
                val = row.get(c, "")
                if pd.isna(val):
                    continue
                txt = str(val).strip()
                if not txt:
                    continue
                txt = trim_text(txt, args.max_chars)
                if txt:
                    texts.append(txt)
                    meta_rows.append({
                        "source": "cell",
                        "row_index": int(idx),
                        "column": c,
                        "text": txt
                    })

    if args.sample and args.sample > 0:
        texts = texts[: args.sample]
        meta_rows = meta_rows[: args.sample]
        print(f"Sampling first {args.sample} texts.", file=sys.stderr)

    if not texts:
        print("ERROR: No non-empty texts prepared for embedding.", file=sys.stderr)
        sys.exit(1)

    # Encode
    embeddings = encode_texts(texts, model, batch_size=args.batch_size, normalize=args.normalize)
    assert embeddings.shape[0] == len(texts), "Embedding count mismatch."

    # Save outputs
    out_emb = f"{args.output_prefix}_embeddings.npy"
    out_meta = f"{args.output_prefix}_metadata.csv"

    np.save(out_emb, embeddings)
    pd.DataFrame(meta_rows).to_csv(out_meta, index=False)

    print(f"Saved embeddings: {out_emb}  shape={embeddings.shape}", file=sys.stderr)
    print(f"Saved metadata:   {out_meta}  rows={len(meta_rows)}", file=sys.stderr)

    return out_emb, out_meta


if __name__ == "__main__":
    main()