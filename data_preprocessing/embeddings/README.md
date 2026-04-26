# Embeddings

This folder contains precomputed vector embeddings used by the project for similarity search, clustering, or as features for downstream models.

## Overview

Keep all embeddings and related index files in this directory. Embeddings may be stored in several formats (NumPy, JSON Lines, Pickle), and may be accompanied by metadata files (e.g., mapping from index -> original document id, or a JSON/CSV with original text and fields).

Typical file types you might find here:

- `*.npy` / `*.npz` — NumPy arrays containing float vectors
- `*.pkl` / `*.joblib` — Python serialized objects (lists, dicts, numpy arrays)
- `*.jsonl` / `*.json` — line-delimited JSON with id/text/vector entries
- FAISS/Annoy/other index files — binary files created by vector index libraries

## Loading examples

Python examples to load common formats:

```python
# Load a NumPy array
import numpy as np
embeddings = np.load('embeddings/my_embeddings.npy')  # shape: (N, D)

# Load embeddings + metadata stored as JSONL
import json
vectors = []
meta = []
with open('embeddings/my_embeddings.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        obj = json.loads(line)
        vectors.append(obj.get('vector'))
        meta.append({k: obj.get(k) for k in ('id', 'text')})

# Load a pickle
import pickle
with open('embeddings/my_embeddings.pkl', 'rb') as f:
    data = pickle.load(f)
# data might be a dict: {'ids': [...], 'vectors': np.array(...)}
```

## Regenerating embeddings

If you need to regenerate embeddings, prefer using a reproducible script (for example `scripts/generate_embeddings.py` if present). When you run generation, record the following metadata alongside the vectors:

- model name and version (e.g., `sentence-transformers/all-MiniLM-L6-v2`)
- dimensionality of the embedding vectors
- preprocessing steps (tokenization, lowercasing, stopword removal, etc.)
- random seed (if applicable)
- date and git commit hash

Example using Hugging Face sentence-transformers:

```python
from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')
texts = ['first document', 'second document', '...']
embeddings = model.encode(texts, show_progress_bar=True)
np.save('embeddings/my_embeddings.npy', embeddings)
```

Example using OpenAI embeddings:

```python
from openai import OpenAI
client = OpenAI()

texts = ['first document', 'second document']
res = client.embeddings.create(model='text-embedding-3-small', input=texts)
vectors = [r.embedding for r in res.data]
# Save vectors and metadata
```

## Naming conventions

- Use clear, descriptive filenames: `{dataset}-{model}-{dim}-{date}.npy` or `{dataset}-{model}.jsonl`
- Keep a metadata file alongside embeddings, e.g. `my_embeddings.meta.json` or `my_embeddings_catalog.csv`, that maps row indices to original document ids and stores generation metadata.

## Reproducibility

Always store the model identifier, commit hash, and generation parameters near the embeddings so results can be audited or regenerated. Consider saving a `README` or `metadata.json` per embedding file with fields like:

```json
{
  "model": "all-MiniLM-L6-v2",
  "dimension": 384,
  "commit": "772728e3200da5e6e16a561b09bf99218b8392c0",
  "date": "2026-01-13",
  "notes": "generated from cleaned dataset X with lowercasing"
}
```
