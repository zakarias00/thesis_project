# Ontology

This directory contains a small, self-contained ontology representation for the course dataset and tools to:

- convert the canonical JSON ontology to a Cytoscape-friendly JSON,
- generate a TBox (schema) OWL file,
- generate example ABox (instance) OWL files from the courses CSV,
- view the ontology graph in the browser using Cytoscape.js.

## Contents
- [ontology.json](ontology.json) — canonical JSON representation of the ontology (graphs → nodes + domainRangeAxioms).
- [cytoscape_ontology.json](cytoscape_ontology.json) — Cytoscape-formatted JSON produced from `ontology.json`.
- [convert_json_to_cytoscape.py](convert_json_to_cytoscape.py) — script that converts `ontology.json` → `cytoscape_ontology.json`.
- [create_tbox_ontology.py](create_tbox_ontology.py) — script that writes a TBox OWL file (`tbox.owl`) describing classes, object properties and data properties.
- [create_abox_examples.py](create_abox_examples.py) — script that reads a courses CSV and generates an ABox OWL file (`abox.owl`) with individuals and links to the TBox schema.
- [viewer.html](viewer.html) — simple Cytoscape.js viewer that loads `cytoscape_ontology.json` and displays nodes/edges.
- This README (this file).

### Quick usage

1. Convert JSON ontology to Cytoscape format
- The conversion script expects `ontology.json` in the same directory and writes `cytoscape_ontology.json`.
- Run:
  ```
  python3 convert_json_to_cytoscape.py
  ```

2. Generate the TBox (schema) OWL
- Run:
  ```
  python3 create_tbox_ontology.py
  ```
- This writes `tbox.owl` in the current directory.

3. Generate example ABox (instances) from the courses CSV
- `create_abox_examples.py` uses pandas and expects a CSV path. By default it looks for `courses_dataset.csv` at the repository root when run as a script and writes `abox.owl`.
- Install the dependency and run:
  ```
  pip install pandas
  python3 create_abox_examples.py
  ```
- Or call the function directly:
  ```
  from create_abox_examples import generate_abox
  generate_abox("path/to/courses_dataset.csv", "path/to/output_abox.owl")
  ```

4. Viewing the ontology in the browser
- `viewer.html` loads `cytoscape_ontology.json` using `fetch`. Because `fetch` can be blocked when opening the file over `file://`, serve the repository over HTTP and open the viewer:
  ```
  # from the repository root:
  python3 -m http.server 8000
  # then open in browser:
  http://localhost:8000/ontology/viewer.html
  ```
- The viewer uses Cytoscape.js from a CDN and applies basic styling for CLASS vs PROPERTY nodes.

## File formats and expectations

- ontology.json
  - Top-level `graphs` array; this project uses `graphs[0]`.
  - Nodes: objects with keys: `id` (full IRI), `type` (`CLASS` or `PROPERTY`), optional `propertyType` (`OBJECT` or `DATA`).
  - domainRangeAxioms: objects with `predicateId`, `domainClassIds` (array), `rangeClassIds` (array).

- cytoscape_ontology.json (generated)
  - Structure: `{ "elements": { "nodes": [...], "edges": [...] } }`
  - Node `data` contains `id`, `label` (short name), `type`, and optional `propertyType`.
  - Edge `data` contains `id`, `source`, `target`, `label`.

## Notes and dependencies
- Scripts require Python 3.
- `create_abox_examples.py` requires `pandas` for CSV handling; other scripts use only the Python standard library (`xml.etree`, `xml.dom.minidom`, `json`, `pathlib`, etc.).
- `viewer.html` depends on network access to the Cytoscape.js CDN; if you need an offline viewer, replace the CDN script with a local copy of Cytoscape.js and serve it alongside the HTML.
