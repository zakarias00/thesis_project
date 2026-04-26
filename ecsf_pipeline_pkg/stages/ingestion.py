"""
Data Ingestion
Load and validate all source datasets (courses, ENISA, NICE, JRC).
Returns canonical DataFrames.
"""

from __future__ import annotations

import re
import logging

import pandas as pd

from ..config import PipelineConfig
from ..schemas import standardize_course_df

logger = logging.getLogger("ecsf_pipeline.ingestion")


# Load and all source datasets
class DataIngestion:

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.courses_df: pd.DataFrame | None = None
        self.enisa_df: pd.DataFrame | None = None
        self.nice_df: pd.DataFrame | None = None
        self.taxonomy_df: pd.DataFrame | None = None

    # Courses
    def ingest_courses(self) -> pd.DataFrame:
        df = pd.read_csv(self.config.courses_csv)
        df = standardize_course_df(df, reference_year=self.config.reference_year)
        self.courses_df = df
        logger.info("Ingested %d course programs", len(df))
        return df

    # ENISA ECSF
    def ingest_enisa(self) -> pd.DataFrame:
        df = pd.read_csv(self.config.enisa_csv)
        for col in ["deliverables", "main_tasks", "key_skills", "key_knowledge"]:
            if col in df.columns:
                df[f"{col}_list"] = df[col].apply(self._parse_bullets)
        self.enisa_df = df
        logger.info("Ingested %d ECSF roles", len(df))
        return df

    # NICE Framework
    def ingest_nice(self) -> pd.DataFrame:
        raw = pd.read_csv(self.config.nice_csv)
        roles = raw.dropna(subset=["Work Role ID"]).copy()
        roles = roles[["Work Role", "Work Role Description", "Work Role ID"]].reset_index(drop=True)
        roles.columns = ["role_name", "description", "role_id"]
        roles["category_code"] = roles["role_id"].str.extract(r"^([A-Z]{2})-")
        cat_map = {
            "OG": "Oversight & Governance",
            "DD": "Design & Development",
            "IO": "Implementation & Operation",
            "PD": "Protection & Defense",
            "IN": "Investigation",
        }
        roles["category"] = roles["category_code"].map(cat_map)
        self.nice_df = roles
        logger.info("Ingested %d NICE work roles", len(roles))
        return roles

    # JRC Taxonomy
    def ingest_jrc_taxonomy(self) -> pd.DataFrame:
        from lxml import etree

        NS = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
        }
        tree = etree.parse(self.config.jrc_rdf)
        root = tree.getroot()

        # Concept schemes
        schemes: dict[str, str] = {}
        for desc in root.findall("rdf:Description", NS):
            types = [
                t.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
                for t in desc.findall("rdf:type", NS)
            ]
            if "http://www.w3.org/2004/02/skos/core#ConceptScheme" in types:
                uri = desc.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
                lbl = desc.find("skos:prefLabel", NS)
                schemes[uri] = lbl.text if lbl is not None else uri.split("/")[-1]

        # Concepts
        rows = []
        for desc in root.findall("rdf:Description", NS):
            types = [
                t.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
                for t in desc.findall("rdf:type", NS)
            ]
            if "http://www.w3.org/2004/02/skos/core#Concept" not in types:
                continue
            uri = desc.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
            lbl_el = desc.find("skos:prefLabel", NS)
            label = lbl_el.text if lbl_el is not None else uri.split("/")[-1]
            defn_el = desc.find("skos:definition", NS)
            definition = defn_el.text if defn_el is not None else ""
            broader = [
                b.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
                for b in desc.findall("skos:broader", NS)
            ]
            in_schemes = [
                s.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource")
                for s in desc.findall("skos:inScheme", NS)
            ]
            is_top = len(desc.findall("skos:topConceptOf", NS)) > 0
            facets = [
                schemes.get(s, s.split("/")[-1]) for s in in_schemes if s in schemes
            ]
            specific = [f for f in facets if f != "Cybersecurity taxonomy"]

            # Parent label (first broader concept's label)
            parent_label = ""
            if broader:
                for b_desc in root.findall("rdf:Description", NS):
                    b_uri = b_desc.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about")
                    if b_uri in broader:
                        b_lbl = b_desc.find("skos:prefLabel", NS)
                        if b_lbl is not None:
                            parent_label = b_lbl.text
                            break

            rows.append({
                "uri": uri,
                "label": label,
                "definition": definition,
                "facet": specific[0] if specific else "General",
                "is_top_concept": is_top,
                "parent_label": parent_label,
                "broader_uris": broader,
            })

        df = pd.DataFrame(rows)
        self.taxonomy_df = df
        logger.info("Ingested %d JRC taxonomy concepts", len(df))
        return df

    @staticmethod
    def _parse_bullets(text) -> list[str]:
        if pd.isna(text):
            return []
        items = re.split(
            r"[\n\r]+\s*(?:[\u2022\u2023\u25E6\u2043\u2219•●○◦▪▸►]|-|\d+\.?\)?)?\s*",
            str(text),
        )
        expanded = []
        for item in items:
            expanded.extend(re.split(r"\s{2,}(?=[A-Z])", item.strip()))
        return [i.strip() for i in expanded if len(i.strip()) > 3]
