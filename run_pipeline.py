#!/usr/bin/env python3
"""
Top-level convenience script: run the full ECSF pipeline.

    python run_pipeline.py [--no-embeddings] [--no-nlp] [-o pipeline_output]
"""
from ecsf_pipeline_pkg.runner import main

if __name__ == "__main__":
    main()
