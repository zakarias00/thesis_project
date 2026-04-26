from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..config import PipelineConfig
from ..schemas import (
    FrameworkItem,
    build_framework_items_ecsf,
    build_framework_items_nice,
    build_framework_items_jrc,
)

logger = logging.getLogger("ecsf_pipeline.framework_mapping")

STOP_WORDS = frozenset([
    "and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
    "is", "are", "be", "or", "as", "by", "that", "this", "from",
    "their", "its", "can", "such", "may", "also", "etc", "will",
])


def _tokenize(text: str) -> set[str]:
    words = re.findall(r"[a-z]{3,}", str(text).lower())
    return {w for w in words if w not in STOP_WORDS}

class ECSFRoleMatcher:
#Match program descriptions against ECSF roles

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._role_vocab: dict[str, set[str]] = {}
        self._role_items: dict[str, list[FrameworkItem]] = {}
        self._is_built = False

    def build_index(self, enisa_df: pd.DataFrame) -> None:
        items = build_framework_items_ecsf(enisa_df)
        for item in items:
            self._role_items.setdefault(item.parent, []).append(item)
            tokens = _tokenize(item.label)
            self._role_vocab.setdefault(item.parent, set()).update(tokens)

        # Add mission and tasks
        for _, row in enisa_df.iterrows():
            role = row["profile_title"]
            mission = str(row.get("mission", ""))
            self._role_vocab.setdefault(role, set()).update(_tokenize(mission))
            for task in row.get("main_tasks_list", []):
                self._role_vocab[role].update(_tokenize(task))

        self._is_built = True
        logger.info("ECSF index: %d roles, %d total items",
                     len(self._role_vocab), len(items))

    def match_program(self, description: str) -> dict:
        if not self._is_built or pd.isna(description):
            return {"matched_roles": [], "role_scores": {}, "evidence": {}}

        desc_tokens = _tokenize(description)
        desc_lower = str(description).lower()
        role_scores: dict[str, float] = {}
        evidence: dict[str, list[str]] = {}

        for role, vocab in self._role_vocab.items():
            if not vocab:
                continue
            overlap = desc_tokens & vocab
            score = len(overlap) / len(vocab)
            role_scores[role] = round(score, 4)
            if overlap:
                evidence[role] = sorted(overlap)[:10]

        _ROLE_ALIASES: dict[str, list[str]] = {
            "Chief Information Security Officer (CISO)": [
                "chief information security officer", "ciso",
            ],
            "Cyber Incident Responder": [
                "cyber incident responder", "incident responder",
                "incident response",
            ],
            "Cyber Legal, Policy & Compliance Officer": [
                "cyber legal", "policy and compliance",
                "policy & compliance", "compliance officer",
            ],
            "Cyber Threat Intelligence Specialist": [
                "cyber threat intelligence", "threat intelligence specialist",
            ],
            "Cybersecurity Architect": ["cybersecurity architect"],
            "Cybersecurity Auditor": ["cybersecurity auditor", "security auditor"],
            "Cybersecurity Educator": [
                "cybersecurity educator", "security educator",
            ],
            "Cybersecurity Implementer": [
                "cybersecurity implementer", "security implementer",
            ],
            "Cybersecurity Researcher": [
                "cybersecurity researcher", "security researcher",
            ],
            "Cybersecurity Risk Manager": [
                "cybersecurity risk manager", "risk manager",
                "risk management",
            ],
            "Digital Forensics Investigator": [
                "digital forensics investigator", "digital forensics",
                "forensics investigator",
            ],
            "Penetration Tester": [
                "penetration tester", "penetration testing",
                "pentest",
            ],
        }
        for role, aliases in _ROLE_ALIASES.items():
            for alias in aliases:
                if alias in desc_lower:
                    # Set a guaranteed-above-threshold score if not already high
                    current = role_scores.get(role, 0.0)
                    boosted = max(current, self.config.ecsf_match_threshold + 0.02)
                    role_scores[role] = round(boosted, 4)
                    evidence.setdefault(role, []).append(f"[name:{alias}]")
                    break  # one alias match is enough per role

        threshold = self.config.ecsf_match_threshold
        matched = [r for r, s in role_scores.items() if s >= threshold]
        return {
            "matched_roles": matched,
            "role_scores": role_scores,
            "evidence": evidence,
        }

    def batch_match(self, courses_df: pd.DataFrame, desc_col: str = "description") -> pd.DataFrame:
        
        results = courses_df[desc_col].apply(self.match_program)
        courses_df["ecsf_roles"] = results.apply(lambda r: r["matched_roles"])
        courses_df["ecsf_role_scores"] = results.apply(lambda r: r["role_scores"])
        courses_df["ecsf_evidence"] = results.apply(lambda r: r["evidence"])
        logger.info("ECSF batch match complete: %d programs", len(courses_df))
        return courses_df


# Match program descriptions against NICE work roles
class NICERoleMatcher:

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._role_vocab: dict[str, set[str]] = {}
        self._role_category: dict[str, str] = {}
        self._is_built = False

    def build_index(self, nice_df: pd.DataFrame) -> None:
        for _, row in nice_df.iterrows():
            role = row["role_name"]
            desc = str(row.get("description", ""))
            self._role_vocab[role] = _tokenize(f"{role} {desc}")
            self._role_category[role] = row.get("category", "")
        self._is_built = True
        logger.info("NICE index: %d work roles", len(self._role_vocab))

    def match_program(self, description: str) -> dict:
        if not self._is_built or pd.isna(description):
            return {"matched_roles": [], "role_scores": {}, "evidence": {}}

        desc_tokens = _tokenize(description)
        role_scores: dict[str, float] = {}
        evidence: dict[str, list[str]] = {}

        for role, vocab in self._role_vocab.items():
            if not vocab:
                continue
            overlap = desc_tokens & vocab
            score = len(overlap) / len(vocab)
            role_scores[role] = round(score, 4)
            if overlap:
                evidence[role] = sorted(overlap)[:10]

        threshold = self.config.nice_match_threshold
        matched = [r for r, s in role_scores.items() if s >= threshold]
        return {
            "matched_roles": matched,
            "role_scores": role_scores,
            "evidence": evidence,
        }

    def batch_match(self, courses_df: pd.DataFrame, desc_col: str = "description") -> pd.DataFrame:
        results = courses_df[desc_col].apply(self.match_program)
        courses_df["nice_roles"] = results.apply(lambda r: r["matched_roles"])
        courses_df["nice_role_scores"] = results.apply(lambda r: r["role_scores"])
        courses_df["nice_evidence"] = results.apply(lambda r: r["evidence"])
        logger.info("NICE batch match complete: %d programs", len(courses_df))
        return courses_df


# match program descriptions against JRC cybersecurity taxonomy concepts
class JRCTaxonomyMatcher:

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._concept_vocab: dict[str, set[str]] = {}
        self._concept_facet: dict[str, str] = {}
        self._concept_is_top: dict[str, bool] = {}
        self._is_built = False

    def build_index(self, taxonomy_df: pd.DataFrame) -> None:
        for _, row in taxonomy_df.iterrows():
            label = row["label"]
            defn = str(row.get("definition", ""))
            self._concept_vocab[label] = _tokenize(f"{label} {defn}")
            self._concept_facet[label] = row.get("facet", "General")
            self._concept_is_top[label] = bool(
                row.get("is_top_concept") or row.get("is_top", False)
            )
        self._is_built = True
        logger.info("JRC index: %d concepts", len(self._concept_vocab))

    def match_program(self, description: str) -> dict:
        if not self._is_built or pd.isna(description):
            return {
                "matched_concepts": [], "concept_scores": {},
                "knowledge_domains": [], "technologies": [],
                "evidence": {},
            }

        desc_tokens = _tokenize(description)
        concept_scores: dict[str, float] = {}
        evidence: dict[str, list[str]] = {}

        for concept, vocab in self._concept_vocab.items():
            if not vocab:
                continue
            overlap = desc_tokens & vocab
            score = len(overlap) / len(vocab)
            concept_scores[concept] = round(score, 4)
            if overlap:
                evidence[concept] = sorted(overlap)[:8]

        threshold = self.config.jrc_match_threshold
        matched = [c for c, s in concept_scores.items() if s >= threshold]
        domains = [c for c in matched if self._concept_is_top.get(c, False)]
        technologies = [c for c in matched if not self._concept_is_top.get(c, False)]

        return {
            "matched_concepts": matched,
            "concept_scores": concept_scores,
            "knowledge_domains": domains,
            "technologies": technologies,
            "evidence": evidence,
        }

    def batch_match(self, courses_df: pd.DataFrame, desc_col: str = "description") -> pd.DataFrame:
        results = courses_df[desc_col].apply(self.match_program)
        courses_df["jrc_concepts"] = results.apply(lambda r: r["matched_concepts"])
        courses_df["jrc_knowledge_domains"] = results.apply(lambda r: r["knowledge_domains"])
        courses_df["jrc_technologies"] = results.apply(lambda r: r["technologies"])
        courses_df["jrc_concept_scores"] = results.apply(lambda r: r["concept_scores"])
        courses_df["jrc_evidence"] = results.apply(lambda r: r["evidence"])
        logger.info("JRC batch match complete: %d programs", len(courses_df))
        return courses_df


class CrossFrameworkAligner:

    @staticmethod
    def summarize(courses_df: pd.DataFrame) -> pd.DataFrame:
        def _count_frameworks(row):
            n = 0
            if len(row.get("ecsf_roles", []) or []) > 0:
                n += 1
            if len(row.get("nice_roles", []) or []) > 0:
                n += 1
            if len(row.get("jrc_concepts", []) or []) > 0:
                n += 1
            return n

        def _total_roles(row):
            return (
                len(row.get("ecsf_roles", []) or [])
                + len(row.get("nice_roles", []) or [])
                + len(row.get("jrc_concepts", []) or [])
            )

        courses_df["frameworks_matched"] = courses_df.apply(_count_frameworks, axis=1)
        courses_df["total_framework_roles"] = courses_df.apply(_total_roles, axis=1)
        return courses_df
