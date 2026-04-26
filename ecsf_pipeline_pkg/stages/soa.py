"""
Skills - Outcomes - Assessment (SOA) Matrix
Build multi-framework SOA matrices that trace:
  framework item ↔ learning outcome ↔ assessment method
  """

from __future__ import annotations

import re
import logging

import numpy as np
import pandas as pd

from ..config import PipelineConfig
from ..schemas import FrameworkItem

logger = logging.getLogger("ecsf_pipeline.soa")

STOP_WORDS = frozenset([
    "and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
    "is", "are", "be", "or", "as", "by", "that", "this", "from",
    "their", "its", "can", "such", "may", "also", "etc", "will",
])

BLOOM_ORDER = ["Remember", "Understand", "Apply", "Analyze", "Evaluate", "Create"]


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[a-z]{3,}", text.lower())
    return [w for w in words if w not in STOP_WORDS]


class SkillAligner:
    """Build the three-layer Skills → Outcomes → Assessment matrix.

    Accepts ``List[FrameworkItem]`` for multi-framework SOA.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config

    # Pairwise scores

    def align_skill_to_outcome(self, skill_text: str, outcome_text: str) -> float:
        s_tokens = set(_tokenize(skill_text))
        o_tokens = set(_tokenize(outcome_text))
        if not s_tokens or not o_tokens:
            return 0.0
        overlap = s_tokens & o_tokens
        return len(overlap) / max(len(s_tokens), 1)

    def align_outcome_to_assessment(self, outcome: dict, assessment: dict) -> float:
        o_idx = BLOOM_ORDER.index(outcome.get("bloom_level", "Apply"))
        a_idx = BLOOM_ORDER.index(assessment.get("bloom_level", "Apply"))
        if a_idx >= o_idx:
            bloom_score = 1.0
        else:
            bloom_score = max(0, 1.0 - (o_idx - a_idx) * 0.25)
        return bloom_score * assessment.get("weight", 0.5)

    # Full matrix builder

    def build_soa_matrix(
        self,
        program_row: pd.Series,
        framework_items: list[FrameworkItem],
        outcomes: list[dict],
        assessments: list[dict],
    ) -> dict:
        
        desc = str(program_row.get("description", ""))

        if not outcomes and desc and desc != "nan":
            outcomes = self._synthesise_implicit_outcomes(desc)

        if not assessments and desc and desc != "nan":
            assessments = self._synthesise_implicit_assessments(desc)

        matrix: list[dict] = []
        for item in framework_items:
            for outcome in outcomes:
                s2o_score = self.align_skill_to_outcome(item.label, outcome["text"])
                if s2o_score < 0.05:
                    continue
                for assessment in assessments:
                    o2a_score = self.align_outcome_to_assessment(outcome, assessment)
                    if o2a_score < 0.1:
                        continue
                    composite = s2o_score * 0.6 + o2a_score * 0.4
                    matrix.append({
                        "framework": item.framework,
                        "item_type": item.item_type,
                        "parent": item.parent,
                        "skill": item.label,
                        "learning_outcome": outcome["text"][:100],
                        "lo_bloom_level": outcome["bloom_level"],
                        "assessment_method": assessment["method"],
                        "assessment_bloom": assessment["bloom_level"],
                        "skill_outcome_score": round(s2o_score, 3),
                        "outcome_assessment_score": round(o2a_score, 3),
                        "composite_score": round(composite, 3),
                    })

        program_name = (
            program_row.get("study_program_name")
            or program_row.get("study program name", "")
        )
        university = (
            program_row.get("university_name")
            or program_row.get("university name", "")
        )

        return {
            "program": program_name,
            "university": university,
            "n_items_matched": len(set(r["skill"] for r in matrix)),
            "n_outcomes": len(outcomes),
            "n_assessments": len(assessments),
            "matrix": matrix,
        }

    # View helpers

    @staticmethod
    # Return a copy of soa_result containing only rows for framework
    def filter_soa_by_framework(soa_result: dict, framework: str) -> dict:
        filtered = [r for r in soa_result["matrix"] if r["framework"] == framework]
        return {
            **soa_result,
            "matrix": filtered,
            "n_items_matched": len(set(r["skill"] for r in filtered)),
        }

    @staticmethod
    def soa_to_dataframe(soa_result: dict) -> pd.DataFrame:
        return pd.DataFrame(soa_result["matrix"])

    @staticmethod
    def flatten_all_soa(soa_results: list[dict]) -> pd.DataFrame:
        rows: list[dict] = []
        for soa in soa_results:
            prog = soa["program"]
            uni = soa["university"]
            for entry in soa["matrix"]:
                entry_copy = dict(entry)
                entry_copy["program"] = prog
                entry_copy["university"] = uni
                rows.append(entry_copy)
        return pd.DataFrame(rows)

    @staticmethod
    def _synthesise_implicit_outcomes(description: str) -> list[dict]:
        import re as _re

        text = description.lower()
        outcomes: list[dict] = []
        seen: set[str] = set()

        # Split on sentence boundaries and commas for topic phrases
        chunks = _re.split(r"[.;]\s*", text)
        for chunk in chunks:
            chunk = chunk.strip()
            if len(chunk) < 15 or len(chunk) > 200:
                continue
            sig = chunk[:30]
            if sig in seen:
                continue
            seen.add(sig)

            # Classify bloom level from leading words
            bloom = "Apply"  # default
            first_words = chunk.split()[:3]
            for w in first_words:
                if w in ("develop", "design", "create", "build", "propose"):
                    bloom = "Create"
                    break
                elif w in ("evaluate", "assess", "audit", "judge"):
                    bloom = "Evaluate"
                    break
                elif w in ("analyze", "analyse", "investigate", "examine"):
                    bloom = "Analyze"
                    break
                elif w in ("implement", "manage", "configure", "deploy", "apply"):
                    bloom = "Apply"
                    break
                elif w in ("explain", "describe", "understand", "discuss"):
                    bloom = "Understand"
                    break

            outcomes.append({
                "text": chunk[:120],
                "bloom_level": bloom,
                "verbs": [],
            })

        return outcomes[:15]  # cap to avoid explosion

    @staticmethod
    def _synthesise_implicit_assessments(description: str) -> list[dict]:
        text_lower = description.lower()
        assessments: list[dict] = []

        if any(w in text_lower for w in [
            "master", "bachelor", "degree", "programme", "program",
            "semester", "ects", "credit",
        ]):
            assessments.append({
                "method": "exam_written",
                "weight": 0.5,
                "bloom_level": "Understand",
            })

        if any(w in text_lower for w in [
            "master", "msc", "m.sc", "postgraduate",
        ]):
            assessments.append({
                "method": "project",
                "weight": 0.7,
                "bloom_level": "Create",
            })

        if any(w in text_lower for w in [
            "practical", "lab", "hands-on", "exercise", "cyber range",
        ]):
            assessments.append({
                "method": "practical_lab",
                "weight": 0.9,
                "bloom_level": "Apply",
            })

        # Fallback: at minimum assume general coursework
        if not assessments:
            assessments.append({
                "method": "exam_written",
                "weight": 0.4,
                "bloom_level": "Understand",
            })

        return assessments
