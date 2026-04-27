# Running Guide — Thesis Project

## 1. Project Overview

This project evaluates European cybersecurity Master's programs against three professional frameworks:

| Framework | Description |
|-----------|-------------|
| **ENISA ECSF** | European Cybersecurity Skills Framework — 12 role profiles |
| **NIST NICE** | US National Initiative for Cybersecurity Education — work roles |
| **JRC Taxonomy** | EU Joint Research Centre cybersecurity taxonomy (SKOS/RDF) |

The project has **two main parts**:

- **Part A — Data Preprocessing** (`data_preprocessing/`): EDA, sentence embeddings, Neo4j graph creation, and OWL ontology generation.
- **Part B — ECSF Analytics Pipeline** (`ecsf_pipeline_pkg/`): A 17-stage pipeline that ingests course data, maps it to the frameworks, scores programs, builds a knowledge graph, and produces recommendations.

---

## 2. Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| **Python** | 3.9+ | 3.10 or 3.11 recommended |
| **pip** | latest | Comes with Python |
| **Git** | any | To clone the repo |
| **Neo4j** | 5.x | *Optional* — only needed for the graph database scripts in `data_preprocessing/embeddings/` |
| **RAM** | ≥ 8 GB | Embedding generation and NLP stages are memory-intensive |
| **Disk** | ~ 2 GB free | For model downloads (sentence-transformers, spaCy) |

---

## 3. Repository Structure

```
thesis_project/
├── data_preprocessing/            # Part A: Data exploration & preparation
│   ├── courses_dataset.csv        #   Raw course descriptions (36 programs)
│   ├── enisa_skill_set.csv        #   ENISA ECSF skill/role profiles
│   ├── tbox.owl                   #   Pre-built TBox ontology (schema)
│   ├── abox.owl                   #   Pre-built ABox ontology (instances)
│   ├── requirements.txt           #   Python dependencies for Part A
│   ├── eda/                       #   Exploratory data analysis
│   │   ├── eda_courses.py         #     Main EDA script
│   │   ├── eda_output/            #     Pre-generated EDA results
│   │   └── utils/                 #     Helper functions
│   ├── embeddings/                #   Sentence embeddings + Neo4j
│   │   ├── create_embeddings.py   #     Generate 384-dim embeddings
│   │   ├── create_neo4j_graph.py  #     Build Neo4j course graph
│   │   ├── create_neo4j_enisa_graph.py  # Build ENISA Neo4j graph
│   │   ├── integrate_graphs.py    #     Merge course + ENISA graphs
│   │   ├── *.npy / *.csv          #     Pre-computed embeddings
│   │   └── *.sh                   #     Quickstart shell scripts
│   └── ontology/                  #   OWL ontology tools
│       ├── create_tbox_ontology.py
│       ├── create_abox_examples.py
│       ├── convert_json_to_cytoscape.py
│       └── viewer.html            #     Browser-based ontology viewer
│
├── ecsf_pipeline_pkg/             # Part B: Main analytics pipeline
│   ├── __main__.py                #   Entry point (python -m ecsf_pipeline_pkg)
│   ├── runner.py                  #   CLI argument parsing
│   ├── config.py                  #   PipelineConfig dataclass
│   ├── orchestrator.py            #   Main pipeline logic (17 stages)
│   ├── schemas.py                 #   Data models & standardization
│   ├── stages/                    #   Individual pipeline stages
│   ├── scoring/                   #   Quality scoring & validation
│   ├── graph/                     #   Knowledge graph construction
│   ├── utils/                     #   Reporting / file export
│   └── requirements.txt           #   Python dependencies for Part B
│
├── pipeline_output/               # Pre-generated pipeline results
│   ├── scores.csv                 #   Composite quality scores
│   ├── recommendations.json       #   Per-program suggestions
│   ├── cybersecurity_education_kg_v2.graphml  # Knowledge graph
│   ├── kg_visualization.html      #   Interactive graph viewer
│   └── ...                        #   More result files
│
└── figures/                       # Exported figures for the thesis
```

---

## 4. Environment Setup

### 4.1 Create a virtual environment

```bash
cd thesis_project
python3 -m venv .venv
```

### 4.2 Activate the virtual environment

```bash
# Linux / macOS
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (CMD)
.venv\Scripts\activate.bat
```

### 4.3 Install dependencies

Install from both requirements files:

```bash
pip install --upgrade pip

# Part A — data preprocessing
pip install -r data_preprocessing/requirements.txt

# Part B — ECSF analytics pipeline
pip install -r ecsf_pipeline_pkg/requirements.txt
```

### 4.4 Download the spaCy language model (needed for NLP stages)

```bash
python -m spacy download en_core_web_sm
```

### 4.5 Verify the installation

```bash
python -c "
import pandas, numpy, networkx, rdflib, scipy
import sklearn, spacy, sentence_transformers, torch
print('All packages imported successfully.')
"
```

---

## 5. Input Data

The pipeline expects **four input files**

- `courses_dataset.csv`:  36 European cybersecurity Master's program descriptions
- `enisa_skill_set.csv`: ENISA ECSF role profiles and skills
- `NICE Framework Components v2.1.0.csv` - NIST NICE work roles |
- `cybersecurity-taxonomy-skos-ap-eu.rdf` - JRC cybersecurity taxonomy (SKOS/RDF)

---

## 6. Part A — Data Preprocessing

All commands in this section assume you have activated the virtual environment and are inside the `thesis_project/` directory.

### 6.1 Exploratory Data Analysis (EDA)

Runs text mining and statistical analysis on the course dataset.

```bash
cd data_preprocessing/eda

python eda_courses.py \
  --input ../courses_dataset.csv \
  --output-dir eda_output
```

**What it produces** (in `eda_output/`):

| File | Content |
|------|---------|
| `EDA_REPORT.md` | Human-readable summary |
| `missing_values.csv` | Column-level missing value counts |
| `description_lengths.csv` | Text length distributions |
| `top_tokens.csv` | Most frequent words |
| `top_bigrams.csv` | Most frequent word pairs |
| `top_skills.csv` | Most frequent extracted skills |

> **Note:** Pre-generated results already exist in `eda_output/`. You only need to re-run if the dataset changes.

### 6.2 Embeddings

Generates 384-dimensional sentence embeddings using the `all-MiniLM-L6-v2` model.

```bash
cd data_preprocessing/embeddings

python create_embeddings.py \
  --input ../courses_dataset.csv \
  --output-prefix course_embeddings \
  --mode row \
  --normalize
```

**What it produces:**

- `course_embeddings_embeddings.npy` — NumPy array of shape `(N, 384)`
- `course_embeddings_metadata.csv` — Row-level metadata (course title, text used, etc.)

> **First run warning:** The script will download the sentence-transformer model (~80 MB) on first use.

### 6.3 Neo4j Graph

These scripts build an interactive graph database from the embeddings. You need a running Neo4j instance.

```bash
cd data_preprocessing/embeddings

# 1. Build the course graph
python create_neo4j_graph.py --uri bolt://localhost:7687

# 2. Build the ENISA profile graph
python create_neo4j_enisa_graph.py --uri bolt://localhost:7687

# 3. Integrate both graphs into one
python integrate_graphs.py --uri bolt://localhost:7687
```

**Quickstart shell scripts** (interactive, with colored output):

```bash
# Course graph
./create_graph_quickstart.sh

# ENISA graph
./create_enisa_graph_quickstart.sh <neo4j_password>
```

### 6.4 Ontology Generation

Generates OWL ontology files and a browser-based visualisation.

```bash
cd data_preprocessing/ontology

# 1. Generate TBox (schema) → outputs tbox.owl
python create_tbox_ontology.py

# 2. Generate ABox (instances) → outputs abox.owl
python create_abox_examples.py

# 3. Convert to Cytoscape JSON → outputs cytoscape_ontology.json
python convert_json_to_cytoscape.py
```

**To view the ontology in a browser:**

```bash
# From the ontology/ directory:
python -m http.server 8000
# Then open: http://localhost:8000/viewer.html
```

---

## 7. Part B — ECSF Analytics Pipeline

This is the main pipeline. It performs 17 stages of analysis.

### 7.1 Running the Full Pipeline

```bash
cd thesis_project

python -m ecsf_pipeline_pkg
```

This runs **all stages** with default configuration and writes results to `pipeline_output/`.

### 7.2 Pipeline Stages

The pipeline runs 17 stages in sequence:

| # | Stage | Module | Description |
|---|-------|--------|-------------|
| 1 | **Ingestion** | `stages/ingestion.py` | Load CSV/RDF input files |
| 2 | **Preprocessing** | `schemas.py` | Standardize column names and data types |
| 3 | **ECSF Mapping** | `stages/framework_mapping.py` | Map courses → ENISA ECSF roles |
| 4 | **NICE Mapping** | `stages/framework_mapping.py` | Map courses → NIST NICE work roles |
| 5 | **JRC Mapping** | `stages/framework_mapping.py` | Map courses → JRC taxonomy concepts |
| 6 | **Cross-Framework Alignment** | `stages/framework_mapping.py` | Align ECSF ↔ NICE ↔ JRC |
| 7 | **LO Extraction** | `stages/extraction.py` | Extract learning outcomes from descriptions |
| 8 | **Assessment Extraction** | `stages/extraction.py` | Detect assessment methods (lab, exam, project…) |
| 9 | **SoA Matrix** | `stages/soa.py` | Build State-of-the-Art alignment matrix |
| 10 | **Breadth Analysis** | `stages/analysis.py` | Coverage breadth across framework roles |
| 11 | **Depth Analysis** | `stages/analysis.py` | Coverage depth per skill/competency |
| 12 | **Progression Analysis** | `stages/analysis.py` | Bloom's taxonomy progression coherence |
| 13 | **Immersion Analysis** | `stages/analysis.py` | Practical/hands-on immersion scoring |
| 14 | **NLP Competency Extraction** | `stages/semantic.py` | spaCy-based competency extraction |
| 15 | **Ontology Alignment** | `stages/semantic.py` | Ontology-boosted scoring |
| 16 | **Embedding Analysis** | `stages/semantic.py` | Semantic similarity via sentence embeddings |
| 17 | **Scoring & Validation** | `scoring/` | Composite scores, ablation, reporting |

---

## 8. Pipeline Outputs

After a successful run, `pipeline_output/` contains:

| File | Format | Description |
|------|--------|-------------|
| `scores.csv` | CSV | Composite quality scores for all 36 programs |
| `soa_flat.csv` | CSV | Flattened State-of-the-Art alignment matrix |
| `recommendations.json` | JSON | Per-program curriculum improvement suggestions |
| `cybersecurity_education_kg_v2.graphml` | GraphML | Knowledge graph (ECSF + NICE + JRC) |
| `cybersecurity_education_kg.graphml` | GraphML | Knowledge graph (ECSF core only) |
| `kg_d3_data.json` | JSON | D3.js-compatible data for interactive visualization |
| `kg_visualization.html` | HTML | Interactive force-directed graph — open in browser |
| `pipeline_summary.json` | JSON | Run metadata (timestamp, stage timings, counts) |
| `pipeline_report.md` | Markdown | Human-readable summary of the full run |
| `validation_summary.json` | JSON | Ablation and cross-validation results |
| `method_comparison.csv` | CSV | Comparison of scoring methods |
| `program_clusters.csv` | CSV | Embedding-based program clusters |
| `ablation_results.csv` | CSV | Results of ablation experiments |
| `semantic_gap_items.csv` | CSV | Identified gaps in semantic coverage |
| `inter_program_redundancy.csv` | CSV | Redundancy analysis across programs |
| `ecsf_ontology.ttl` | Turtle | Generated ECSF ontology in RDF/Turtle |
| `artifact_manifest.json` | JSON | Inventory of all generated files |

**To view the interactive knowledge graph:**

```bash
cd pipeline_output
python -m http.server 8000
# Then open: http://localhost:8000/kg_visualization.html
```

## 9. Quick-Start 
```bash
# 1. Setup
cd thesis_project
python3 -m venv .venv
source .venv/bin/activate
pip install -r ecsf_pipeline_pkg/requirements.txt
python -m spacy download en_core_web_sm

# 2. Run the pipeline 
python -m ecsf_pipeline_pkg

# 3. View results
cat pipeline_output/pipeline_report.md
python -m http.server 8000 -d pipeline_output
# Open http://localhost:8000/kg_visualization.html
```
