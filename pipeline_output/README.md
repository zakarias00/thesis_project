# Pipeline Output

Auto-generated artefacts from the ECSF analytics pipeline.

## Key files

| File | Description |
|------|-------------|
| `scores.csv` | Quality scores for all 36 programs |
| `soa_flat.csv` | Flattened State-of-the-Art alignment matrix |
| `recommendations.json` | Per-program curriculum recommendations |
| `cybersecurity_education_kg_v2.graphml` | Knowledge graph (v2: ECSF + NICE + JRC) |
| `cybersecurity_education_kg.graphml` | Knowledge graph (v1: ECSF core) |
| `kg_d3_data.json` | D3.js-compatible JSON for interactive visualization |
| `kg_visualization.html` | Interactive force-directed graph (open in browser) |
| `validation_summary.json` | Ablation and cross-validation results |
| `program_clusters.csv` | Embedding-based program clusters |

## Regeneration

```bash
cd ..
python -m ecsf_pipeline_pkg
```
