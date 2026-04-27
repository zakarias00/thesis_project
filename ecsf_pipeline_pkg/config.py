"""
Pipeline configuration
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path

logger = logging.getLogger("ecsf_pipeline")
PACKAGE_DIR = Path(__file__).resolve().parent


@dataclass
class PipelineConfig:
    
    courses_csv: str = "input_data/course detail description.csv"
    enisa_csv: str = "input_data/enisa_skill_set.csv"
    nice_csv: str = "input_data/NICE Framework Components v2.1.0.csv"
    jrc_rdf: str = "input_data/cybersecurity-taxonomy-skos-ap-eu.rdf"

    reference_year: int = 2026

    enable_ecsf: bool = True
    enable_nice: bool = True
    enable_jrc: bool = True

    run_eda: bool = True
    run_preprocessing: bool = True
    run_framework_mapping: bool = True
    run_lo_extraction: bool = True
    run_assessment_extraction: bool = True
    run_soa: bool = True
    run_breadth: bool = True
    run_depth: bool = True
    run_progression: bool = True
    run_immersion: bool = True
    run_nlp: bool = True
    run_ontology: bool = True
    run_embeddings: bool = True
    run_scoring: bool = True
    run_validation: bool = True
    run_graph: bool = False 
    run_reporting: bool = True

    co_occurrence_threshold: int = 2
    coverage_depth_weights: dict = field(default_factory=lambda: {
        "skill_match": 0.4,
        "knowledge_match": 0.3,
        "task_match": 0.2,
        "deliverable_match": 0.1,
    })

    ecsf_match_threshold: float = 0.08
    nice_match_threshold: float = 0.06
    jrc_match_threshold: float = 0.10

    ablation_regex_threshold: float = 0.15   # token-overlap for regex layer
    ablation_nlp_threshold: float = 0.10     # NLP score to count as hit
    ablation_ontology_threshold: float = 0.08  # ontology-boosted score threshold
    ablation_embedding_threshold: float = 0.25 # embedding score to count as hit

    composite_weights: dict = field(default_factory=lambda: {
        "coverage": 0.25,
        "redundancy": 0.15,
        "depth": 0.20,
        "latency": 0.15,
        "composite_soa": 0.25,
    })

    grade_thresholds: dict = field(default_factory=lambda: {
        "A": 0.80,
        "B": 0.65,
        "C": 0.50,
        "D": 0.35,
        "E": 0.20,
        # below E → F
    })

    assessment_methods: dict = field(default_factory=lambda: {
        "practical_lab": {
            "keywords": ["lab", "practical", "hands-on", "exercise",
                         "simulation", "range", "cyber range"],
            "weight": 1.0, "bloom_level": "Apply",
        },
        "project": {
            "keywords": ["project", "capstone", "thesis", "dissertation",
                         "research project"],
            "weight": 0.9, "bloom_level": "Create",
        },
        "exam_written": {
            "keywords": ["exam", "test", "written", "midterm", "final"],
            "weight": 0.6, "bloom_level": "Understand",
        },
        "certification": {
            "keywords": ["certification", "certificate", "certified", "accredit"],
            "weight": 0.85, "bloom_level": "Evaluate",
        },
        "internship": {
            "keywords": ["internship", "placement", "work experience",
                         "industry", "company", "practice-oriented"],
            "weight": 0.95, "bloom_level": "Apply",
        },
        "group_work": {
            "keywords": ["group", "team", "collaborative", "peer"],
            "weight": 0.7, "bloom_level": "Apply",
        },
        "ctf_competition": {
            "keywords": ["ctf", "capture the flag", "competition",
                         "challenge", "hackathon"],
            "weight": 1.0, "bloom_level": "Evaluate",
        },
        "case_study": {
            "keywords": ["case study", "scenario", "real-world", "case-based"],
            "weight": 0.8, "bloom_level": "Analyze",
        },
        "presentation": {
            "keywords": ["presentation", "seminar", "report", "oral"],
            "weight": 0.65, "bloom_level": "Apply",
        },
    })

    bloom_verbs: dict = field(default_factory=lambda: {
        "Remember": ["define", "list", "recall", "recognize", "identify",
                      "name", "describe"],
        "Understand": ["explain", "summarize", "interpret", "classify",
                        "compare", "discuss", "understand"],
        "Apply": ["apply", "implement", "use", "execute", "demonstrate",
                   "solve", "perform", "operate", "manage"],
        "Analyze": ["analyze", "examine", "investigate", "differentiate",
                     "assess", "evaluate", "diagnose", "audit"],
        "Evaluate": ["evaluate", "judge", "justify", "recommend",
                      "critique", "appraise", "validate", "test"],
        "Create": ["create", "design", "develop", "construct",
                    "formulate", "build", "propose", "architect"],
    })

    modern_tech_keywords: list = field(default_factory=lambda: [
        "cloud", "aws", "azure", "gcp", "kubernetes", "docker",
        "devops", "devsecops", "ci/cd", "zero trust", "sase",
        "ai", "machine learning", "deep learning", "llm",
        "blockchain", "iot", "5g", "quantum", "ransomware",
        "siem", "soar", "xdr", "edr", "mitre att&ck",
        "nist csf", "iso 27001:2022", "gdpr", "nis2",
        "threat intelligence", "supply chain security",
    ])

    embedding_model: str = "all-MiniLM-L6-v2"
    spacy_model: str = "en_core_web_sm"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""

    output_dir: str = "pipeline_output"
    report_format: str = "markdown"

    def __post_init__(self) -> None:
        self.courses_csv = self._resolve_input_path(self.courses_csv)
        self.enisa_csv = self._resolve_input_path(self.enisa_csv)
        self.nice_csv = self._resolve_input_path(self.nice_csv)
        self.jrc_rdf = self._resolve_input_path(self.jrc_rdf)

    @staticmethod
    def _resolve_input_path(path_value: str) -> str:
        p = Path(path_value).expanduser()
        if p.is_absolute():
            return str(p)

        candidate_roots = [
            Path.cwd(),
            PACKAGE_DIR,
            PACKAGE_DIR.parent,
            PACKAGE_DIR.parent.parent,
        ]

        candidates = [root / p for root in candidate_roots]
        if p.parent == Path("."):
            candidates.extend((root / "input_data" / p for root in candidate_roots))
        
        for candidate in candidates:
            if candidate.exists():
                return str(candidate.resolve())

        return str((PACKAGE_DIR / p).resolve())


    def save(self, path: str = "pipeline_config.json") -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)
        logger.info("Config saved → %s", path)

    @classmethod
    def load(cls, path: str = "pipeline_config.json") -> "PipelineConfig":
        with open(path) as f:
            data = json.load(f)
        logger.info("Config loaded ← %s", path)
        return cls(**data)
