# Data Preprocessing

Exploratory data analysis, embedding generation, ontology construction, and Neo4j graph creation for the cybersecurity education dataset.

## Structure

```
data_preprocessing/
├── courses_dataset.csv       # Raw course descriptions
├── enisa_skill_set.csv       # ENISA ECSF skill profiles
├── tbox.owl                  # TBox ontology (schema)
├── abox.owl                  # ABox ontology (instances)
├── eda/                      # Exploratory data analysis
├── embeddings/               # Sentence embeddings + Neo4j graph scripts
└── ontology/                 # OWL ontology generation + visualization
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## EDA

```bash
cd eda
python eda_courses.py --input ../courses_dataset.csv --output-dir eda_output
```

Outputs: missing value analysis, text length distributions, top tokens/bigrams, skill frequencies.

## Embeddings

```bash
cd embeddings

# Generate 384-dim sentence embeddings
python create_embeddings.py --input ../courses_dataset.csv \
  --output-prefix course_embeddings --mode row --normalize

# Build Neo4j course graph (requires running Neo4j instance)
python create_neo4j_graph.py --uri bolt://localhost:7687

# Build Neo4j ENISA profile graph
python create_neo4j_enisa_graph.py --uri bolt://localhost:7687

# Integrate both graphs
python integrate_graphs.py --uri bolt://localhost:7687
```

## Ontology

```bash
cd ontology

# Generate TBox (schema)
python create_tbox_ontology.py

# Generate ABox (instances from CSV)
python create_abox_examples.py

# Convert to Cytoscape JSON for visualization
python convert_json_to_cytoscape.py
```

Open `viewer.html` in a browser to visualize the ontology.
