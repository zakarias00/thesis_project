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
│   ├── graph_pipeline.py        # Knowledge graph construction + GraphML export
│   ├── convert_graphml_to_d3.py # GraphML → D3 JSON + PNG/SVG subgraph exports
│   └── __main__.py              # CLI entry (python -m …graph)
├── scoring/
│   ├── quality_scores.py  # Coverage, depth, latency, composite scoring
│   └── validation.py      # Ablation, rank correlation, method comparison
├── stages/
│   ├── ingestion.py       # CSV/RDF data loading
│   ├── framework_mapping.py  # ECSF, NICE, JRC role matching
│   ├── extraction.py      # Learning outcome & assessment extraction
│   ├── soa.py             # Skill-Outcome-Assessment matrix construction
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

### Knowledge-Graph Visualisation

After the pipeline has produced `pipeline_output/cybersecurity_education_kg_v2.graphml`, generate readable PNG / SVG exports with:

```bash
# All subgraph exports (default paths)
python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3

# Custom input / output
python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3 \
    --input  pipeline_output/cybersecurity_education_kg_v2.graphml \
    --outdir pipeline_output/graph_figures

# Render specific subgraphs only
python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3 -s overview ecsf_roles universities
```

#### Available subgraphs

| Name | Description |
|------|-------------|
| `overview` | Structural skeleton — center hub, ECSF roles, NICE categories, countries |
| `ecsf_roles` | 12 ECSF roles with their top skills & knowledge items |
| `nice_framework` | NICE categories → work roles |
| `universities` | Universities → programs → countries |
| `jrc_taxonomy` | JRC domains → top concepts per domain |
| `role_program` | Role–program alignment (ECSF ↔ university programs) |

A **readable full-graph** (capped subset ~140 nodes) is always generated alongside the subgraphs.

## Output

Results are written to `pipeline_output/`:

- `scores.csv` — composite quality scores per program
- `soa_flat.csv` — flattened Skill-Outcome-Assessment matrix
- `recommendations.json` — per-program improvement suggestions
- `cybersecurity_education_kg.graphml` — knowledge graph (v2)
- `pipeline_summary.json` — run metadata
- `pipeline_report.md` — human-readable summary
- `graph_figures/kg_d3_data.json` — D3-force-compatible JSON
- `graph_figures/kg_overview.png/.svg` — structural overview
- `graph_figures/kg_ecsf_roles.png/.svg` — ECSF roles + skills/knowledge
- `graph_figures/kg_nice_framework.png/.svg` — NICE categories & work roles
- `fgraph_figuresg` — universities, programs & countries
- `graph_figures/kg_jrc_taxonomy.png/.svg` — JRC domains & concepts
- `graph_figures/kg_role_program.png/.svg` — role–program alignment
- `graph_figures/kg_full_readable.png/.svg` — readable subset of the full graph
