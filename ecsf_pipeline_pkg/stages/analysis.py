"""
Coverage breadth, coverage depth, progression coherence, and practical immersion analyzers
"""

from __future__ import annotations

import re
import logging

import numpy as np
import pandas as pd

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.analysis")

STOP_WORDS = frozenset([
    "and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
    "is", "are", "be", "or", "as", "by", "that", "this", "from",
    "their", "its", "can", "such", "may", "also", "etc", "will",
])


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[a-z]{3,}", str(text).lower())
    return [w for w in words if w not in STOP_WORDS]

class CoverageBreadthAnalyzer:
    """How many individual skill/knowledge items per ECSF role are
    addressed by a program description"""

    def __init__(self, config: PipelineConfig):
        self.config = config

    def analyze_program(self, description: str, skills_df: pd.DataFrame) -> dict:
        if pd.isna(description):
            return self._empty_result(skills_df)

        desc_tokens = set(_tokenize(description))
        role_scores: list[dict] = []
        total_covered, total_items = 0, 0

        for _, role_row in skills_df.iterrows():
            role_name = role_row["profile_title"]
            sk_items = role_row.get("key_skills_list", [])
            kn_items = role_row.get("key_knowledge_list", [])

            sk_matched = self._count_matched(desc_tokens, sk_items)
            kn_matched = self._count_matched(desc_tokens, kn_items)
            total_sk = max(len(sk_items), 1)
            total_kn = max(len(kn_items), 1)
            breadth = (sk_matched / total_sk + kn_matched / total_kn) / 2.0

            role_scores.append({
                "role": role_name,
                "skills_matched": sk_matched,
                "skills_total": len(sk_items),
                "knowledge_matched": kn_matched,
                "knowledge_total": len(kn_items),
                "breadth": round(breadth, 3),
            })
            total_covered += sk_matched + kn_matched
            total_items += len(sk_items) + len(kn_items)

        overall = np.mean([r["breadth"] for r in role_scores]) if role_scores else 0.0
        return {
            "role_scores": role_scores,
            "overall_breadth": round(float(overall), 3),
            "items_covered": total_covered,
            "items_total": total_items,
        }

    def _count_matched(self, desc_tokens: set, items: list) -> int:
        matched = 0
        for item in items:
            item_tokens = set(_tokenize(item))
            if len(desc_tokens & item_tokens) >= 2:
                matched += 1
        return matched

    def _empty_result(self, skills_df: pd.DataFrame) -> dict:
        role_scores = []
        for _, row in skills_df.iterrows():
            role_scores.append({
                "role": row["profile_title"],
                "skills_matched": 0,
                "skills_total": len(row.get("key_skills_list", [])),
                "knowledge_matched": 0,
                "knowledge_total": len(row.get("key_knowledge_list", [])),
                "breadth": 0.0,
            })
        return {"role_scores": role_scores, "overall_breadth": 0.0,
                "items_covered": 0, "items_total": 0}


class CoverageDepthAnalyzer:
    """How thoroughly a program addresses each matched role's items.
    Tiers: shallow (< 0.15), moderate (0.15 – 0.35), deep (≥ 0.35)"""

    DEPTH_THRESHOLDS = {"shallow": 0.15, "moderate": 0.35}

    def __init__(self, config: PipelineConfig):
        self.config = config

    def analyze_program(
        self, description: str, skills_df: pd.DataFrame, matched_roles: list[str]
    ) -> dict:
        if pd.isna(description) or not matched_roles:
            return self._empty_result()

        desc_tokens = set(_tokenize(description))
        text_density = min(len(desc_tokens) / 300.0, 1.0)
        role_depth: list[dict] = []
        all_scores: list[float] = []

        for _, role_row in skills_df.iterrows():
            rname = role_row["profile_title"]
            if rname not in matched_roles:
                continue
            items = (role_row.get("key_skills_list", [])
                     + role_row.get("key_knowledge_list", []))
            item_scores: list[dict] = []
            for item in items:
                item_tokens = set(_tokenize(item))
                if not item_tokens:
                    continue
                overlap = desc_tokens & item_tokens
                score = len(overlap) / len(item_tokens)
                item_scores.append({
                    "item": item[:80],
                    "score": round(score, 3),
                    "tier": self._tier(score),
                    "matched_tokens": sorted(overlap)[:8],
                })
            avg = float(np.mean([s["score"] for s in item_scores])) if item_scores else 0.0
            tier_counts = (
                pd.Series([s["tier"] for s in item_scores]).value_counts().to_dict()
                if item_scores else {}
            )
            role_depth.append({
                "role": rname,
                "n_items": len(item_scores),
                "mean_depth": round(avg, 3),
                "tier_distribution": tier_counts,
                "items": item_scores,
            })
            all_scores.extend(s["score"] for s in item_scores)

        depth_profile = {"shallow": 0, "moderate": 0, "deep": 0}
        for s in all_scores:
            depth_profile[self._tier(s)] += 1

        overall = float(np.mean(all_scores)) if all_scores else 0.0
        return {
            "role_depth": role_depth,
            "depth_profile": depth_profile,
            "overall_depth": round(overall, 3),
            "text_density": round(text_density, 3),
        }

    def _tier(self, score: float) -> str:
        if score < self.DEPTH_THRESHOLDS["shallow"]:
            return "shallow"
        elif score < self.DEPTH_THRESHOLDS["moderate"]:
            return "moderate"
        return "deep"

    def _empty_result(self) -> dict:
        return {
            "role_depth": [],
            "depth_profile": {"shallow": 0, "moderate": 0, "deep": 0},
            "overall_depth": 0.0,
            "text_density": 0.0,
        }

class ProgressionCoherenceAnalyzer:
    """Whether a program follows a foundational → intermediate → advanced
    knowledge progression.  Score 0–1"""

    TIER_KEYWORDS = {
        "foundational": [
            "basic", "introduct", "fundamental", "foundation", "beginner",
            "overview", "survey", "core", "essentials", "principles",
            "elementary", "prerequisite", "background",
        ],
        "intermediate": [
            "intermediate", "applied", "practice", "practical",
            "hands-on", "workshop", "exercise", "implement",
            "case study", "module", "lab",
        ],
        "advanced": [
            "advanced", "speciali", "in-depth", "expert", "research",
            "master", "thesis", "dissert", "cutting-edge", "state-of-the-art",
            "novel", "emerging", "innovative", "deep dive", "capstone",
            "complex", "sophistic",
        ],
    }

    ORDERING_PATTERNS = [
        r"from\s+(?:basic|fundamental|introduct)\w*\s+to\s+(?:advanced|speciali)",
        r"progress(?:ion|ively)\s+(?:from|through)",
        r"build(?:ing)?\s+(?:on|upon)\s+(?:basic|fundamental|core)",
        r"first.{1,30}then.{1,30}(?:advanced|speciali)",
        r"foundation.{1,30}(?:then|followed|leading).{1,30}(?:advanced|speciali)",
        r"(?:semester|year)\s*[12].{1,50}(?:semester|year)\s*[34]",
        r"introduct\w+.{1,80}advanced",
    ]

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._tier_patterns: dict[str, re.Pattern] = {}
        for tier, keywords in self.TIER_KEYWORDS.items():
            self._tier_patterns[tier] = re.compile(
                "|".join(re.escape(k) if len(k) < 6 else k for k in keywords),
                re.IGNORECASE,
            )
        self._ordering_pats = [re.compile(p, re.IGNORECASE) for p in self.ORDERING_PATTERNS]

    def analyze_program(self, description: str) -> dict:
        if pd.isna(description):
            return self._empty_result()
        text = str(description).lower()

        tier_evidence: dict[str, list[str]] = {}
        tiers_detected: list[str] = []
        for tier, pat in self._tier_patterns.items():
            hits = pat.findall(text)
            if hits:
                tier_evidence[tier] = sorted(set(h.lower().strip() for h in hits))
                tiers_detected.append(tier)

        ordering = any(p.search(text) for p in self._ordering_pats)
        bloom_levels_found = self._bloom_progression(text)

        n_tiers = len(tiers_detected)
        if n_tiers == 0:
            score, explanation = 0.0, "No progression signals detected."
        elif n_tiers == 1:
            score = 0.25
            explanation = f"Only {tiers_detected[0]}-level content mentioned."
        elif n_tiers == 2:
            if set(tiers_detected) == {"foundational", "advanced"}:
                score, explanation = 0.60, "Foundational and advanced present but intermediate missing."
            else:
                score = 0.50
                explanation = f"Two adjacent tiers: {', '.join(tiers_detected)}."
        else:
            score = 0.75 if not ordering else 1.0
            explanation = ("All three progression tiers detected"
                           + (" with explicit ordering cues." if ordering
                              else ", but no explicit ordering language found."))

        bloom_range = (
            max(bloom_levels_found.values()) - min(bloom_levels_found.values())
            if bloom_levels_found else 0
        )
        bloom_bonus = min(bloom_range / 50.0, 0.10)
        score = min(round(score + bloom_bonus, 3), 1.0)

        return {
            "tiers_detected": tiers_detected,
            "tier_evidence": tier_evidence,
            "ordering_detected": ordering,
            "coherence_score": score,
            "bloom_progression": bloom_levels_found,
            "explanation": explanation,
        }

    def _bloom_progression(self, text: str) -> dict:
        bloom_counts: dict[str, int] = {}
        for level, verbs in self.config.bloom_verbs.items():
            count = sum(1 for v in verbs if re.search(r"\b" + re.escape(v) + r"\w*\b", text))
            if count > 0:
                bloom_counts[level] = count
        return bloom_counts

    def _empty_result(self) -> dict:
        return {
            "tiers_detected": [], "tier_evidence": {},
            "ordering_detected": False, "coherence_score": 0.0,
            "bloom_progression": {}, "explanation": "No description available.",
        }



class PracticalImmersionAnalyzer:
#Practical to theoretical ratio and quality-weighted immersion index

    PRACTICAL_INDICATORS = {
        "cyber_range":   {"keywords": ["cyber range", "cyber-range", "cyberrange"],
                          "weight": 1.0, "category": "simulation"},
        "ctf":           {"keywords": ["ctf", "capture the flag", "capture-the-flag"],
                          "weight": 1.0, "category": "competition"},
        "hackathon":     {"keywords": ["hackathon", "hack-a-thon"],
                          "weight": 0.95, "category": "competition"},
        "lab":           {"keywords": ["lab", "laboratory", "lab session", "computer lab"],
                          "weight": 0.85, "category": "lab"},
        "hands_on":      {"keywords": ["hands-on", "hands on", "experiential"],
                          "weight": 0.90, "category": "practice"},
        "simulation":    {"keywords": ["simulation", "simulated", "emulation"],
                          "weight": 0.90, "category": "simulation"},
        "internship":    {"keywords": ["internship", "placement", "work experience",
                                       "work-experience", "apprentice"],
                          "weight": 0.95, "category": "work_based"},
        "industry":      {"keywords": ["industry partner", "industry collaboration",
                                       "industry project", "company project", "real-world project"],
                          "weight": 0.85, "category": "work_based"},
        "project":       {"keywords": ["project", "capstone", "group project",
                                       "team project", "final project"],
                          "weight": 0.75, "category": "project"},
        "exercise":      {"keywords": ["exercise", "practical exercise", "drill"],
                          "weight": 0.70, "category": "practice"},
        "workshop":      {"keywords": ["workshop", "bootcamp", "boot camp"],
                          "weight": 0.80, "category": "practice"},
        "thesis":        {"keywords": ["thesis", "dissertation", "research project"],
                          "weight": 0.65, "category": "research"},
        "practice_oriented": {
            "keywords": ["practice-oriented", "practice oriented",
                         "practically oriented", "applied learning"],
            "weight": 0.80, "category": "practice",
        },
    }

    THEORETICAL_INDICATORS = {
        "lecture":       {"keywords": ["lecture", "lecturing"], "weight": 1.0},
        "theory":        {"keywords": ["theory", "theoretical", "theoretic"], "weight": 1.0},
        "seminar":       {"keywords": ["seminar"], "weight": 0.7},
        "reading":       {"keywords": ["reading", "literature", "textbook"], "weight": 0.8},
        "exam":          {"keywords": ["exam", "written exam", "midterm", "final exam"], "weight": 0.6},
        "fundamentals":  {"keywords": ["fundamentals", "foundation", "principles"], "weight": 0.5},
    }

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._practical_pats: dict[str, tuple[re.Pattern, dict]] = {}
        for name, info in self.PRACTICAL_INDICATORS.items():
            kw_pat = "|".join(re.escape(k) for k in info["keywords"])
            self._practical_pats[name] = (re.compile(kw_pat, re.IGNORECASE), info)
        self._theoretical_pats: dict[str, tuple[re.Pattern, dict]] = {}
        for name, info in self.THEORETICAL_INDICATORS.items():
            kw_pat = "|".join(re.escape(k) for k in info["keywords"])
            self._theoretical_pats[name] = (re.compile(kw_pat, re.IGNORECASE), info)

    def analyze_program(self, description: str) -> dict:
        if pd.isna(description):
            return self._empty_result()

        text = str(description)
        practical_found: list[dict] = []
        practical_score = 0.0
        categories: dict[str, int] = {}

        for name, (pat, info) in self._practical_pats.items():
            hits = pat.findall(text)
            if hits:
                n = len(hits)
                practical_score += info["weight"] * min(n, 3)
                cat = info["category"]
                categories[cat] = categories.get(cat, 0) + n
                practical_found.append({
                    "indicator": name,
                    "evidence": sorted(set(h.lower() for h in hits)),
                    "count": n, "weight": info["weight"], "category": cat,
                })

        theoretical_found: list[dict] = []
        theoretical_score = 0.0
        for name, (pat, info) in self._theoretical_pats.items():
            hits = pat.findall(text)
            if hits:
                n = len(hits)
                theoretical_score += info["weight"] * min(n, 3)
                theoretical_found.append({
                    "indicator": name,
                    "evidence": sorted(set(h.lower() for h in hits)),
                    "count": n, "weight": info["weight"],
                })

        total = practical_score + theoretical_score
        ratio = practical_score / total if total > 0 else 0.0
        n_categories = len(categories)
        diversity_bonus = min(n_categories / 5.0, 0.3)
        intensity = min(practical_score / 8.0, 0.7)
        immersion_index = round(min(diversity_bonus + intensity, 1.0), 3)

        if not practical_found and not theoretical_found:
            explanation = "No practical or theoretical indicators detected."
        elif not practical_found:
            explanation = "Only theoretical indicators detected."
        elif not theoretical_found:
            explanation = "Only practical indicators — strong hands-on orientation."
        else:
            explanation = (f"{len(practical_found)} practical types "
                           f"({n_categories} categories) vs. "
                           f"{len(theoretical_found)} theoretical types. "
                           f"Ratio = {ratio:.0%} practical.")

        return {
            "practical_indicators": practical_found,
            "theoretical_indicators": theoretical_found,
            "practical_score": round(practical_score, 3),
            "theoretical_score": round(theoretical_score, 3),
            "immersion_ratio": round(ratio, 3),
            "immersion_index": immersion_index,
            "category_profile": categories,
            "explanation": explanation,
        }

    def _empty_result(self) -> dict:
        return {
            "practical_indicators": [], "theoretical_indicators": [],
            "practical_score": 0.0, "theoretical_score": 0.0,
            "immersion_ratio": 0.0, "immersion_index": 0.0,
            "category_profile": {}, "explanation": "No description available.",
        }
