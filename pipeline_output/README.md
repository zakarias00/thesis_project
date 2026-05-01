# Pipeline Output

Result files from the ECSF analytics pipeline.

## Core Results

| File | Description |
|------|-------------|
| `scores.csv` | Composite quality scores for all 36 programs (coverage, depth, latency, grade, …) |
| `soa_flat.csv` | Flattened Skill-Outcome-Assessment matrix |
| `recommendations.json` | Per-program curriculum improvement suggestions |
| `pipeline_summary.json` | Run metadata (timestamp, stage timings, counts) |
| `pipeline_report.md` | Human-readable summary of the full run |
| `artifact_manifest.json` | Inventory of all generated output files |

## Knowledge Graphs

| File | Description |
|------|-------------|
| `cybersecurity_education_kg.graphml` | Full knowledge graph (ECSF + NICE + JRC) |
| `kg_d3_data.json` | D3.js-compatible JSON for interactive visualisation |
| `kg_visualization.html` | Interactive force-directed graph — open in browser |

## Validation & Analysis

| File | Description |
|------|-------------|
| `validation_summary.json` | Ablation and cross-validation results |
| `ablation_results.csv` | Results of ablation experiments |
| `method_comparison.csv` | Scoring method comparison (Jaccard overlap) |
| `inter_program_redundancy.csv` | Redundancy analysis across programs |
| `semantic_gap_items.csv` | Identified gaps in semantic coverage |
| `program_clusters.csv` | Embedding-based program clusters |
| `ecsf_ontology.ttl` | Generated ECSF ontology in RDF/Turtle |

## Graph Figures (`graph_figures/`)

| File | Description |
|------|-------------|
| `kg_d3_data.json` | D3-force-compatible JSON |
| `kg_overview.png/.svg` | Structural overview |
| `kg_ecsf_roles.png/.svg` | ECSF roles + skills/knowledge |
| `kg_nice_framework.png/.svg` | NICE categories & work roles |
| `kg_universities.png/.svg` | Universities, programs & countries |
| `kg_jrc_taxonomy.png/.svg` | JRC domains & concepts |
| `kg_role_program.png/.svg` | Role–program alignment |
| `kg_full_readable.png/.svg` | Readable subset of the full graph |

## Regeneration

```bash
# Re-run the full pipeline
cd ..
python -m ecsf_pipeline_pkg

# Re-generate graph figures only
python -m ecsf_pipeline_pkg.graph.convert_graphml_to_d3 \
    --input  pipeline_output/cybersecurity_education_kg.graphml \
    --outdir pipeline_output/graph_figures
```

## Interactive Viewer

```bash
python -m http.server 8000
# Open http://localhost:8000/kg_visualization.html
```
