# ECSF Cybersecurity Education Analytics Pipeline

A 17-stage analytics pipeline that evaluates European cybersecurity Master's programs against the ENISA ECSF, NIST NICE, and JRC taxonomy frameworks.

## Structure

```
ecsf_pipeline_pkg/
├── config.py              # Pipeline configuration (dataclass)
├── orchestrator.py        # Main pipeline orchestrator
├── runner.py              # CLI entry point
├── schemas.py             # Data models and standardization
├── graph/
│   └── graph_pipeline.py  # Knowledge graph construction + GraphML export
├── scoring/
│   ├── quality_scores.py  # Coverage, depth, latency, composite scoring
│   └── validation.py      # Ablation, rank correlation, method comparison
├── stages/
│   ├── ingestion.py       # CSV/RDF data loading
│   ├── framework_mapping.py  # ECSF, NICE, JRC role matching
│   ├── extraction.py      # Learning outcome & assessment extraction
│   ├── soa.py             # State-of-the-Art matrix construction
│   ├── analysis.py        # Breadth, depth, progression, immersion
│   ├── semantic.py        # NLP, ontology alignment, embeddings
│   └── feedback.py        # Curriculum recommendations
├── utils/
    └──  reporting.py       # File export (CSV, JSON, Markdown)
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional: NLP stage
python -m spacy download en_core_web_sm
```

## Input data

Place these files in the working directory (or configure paths via `pipeline_config.json`):

| File | Description |
|------|-------------|
| `course detail description.csv` | Scraped program descriptions (36 programs) |
| `enisa_skill_set.csv` | ENISA ECSF role profiles |
| `NICE Framework Components v2.1.0.csv` | NIST NICE work roles |
| `cybersecurity-taxonomy-skos-ap-eu.rdf` | JRC cybersecurity taxonomy (SKOS) |

## Usage

```bash
# Full pipeline
python -m ecsf_pipeline_pkg

```

## Output

Results are written to `pipeline_output/`:

- `scores.csv` — composite quality scores per program
- `soa_flat.csv` — flattened State-of-the-Art matrix
- `recommendations.json` — per-program improvement suggestions
- `cybersecurity_education_kg.graphml` — knowledge graph (v2)
- `pipeline_summary.json` — run metadata
- `pipeline_report.md` — human-readable summary
