from __future__ import annotations

import re
import hashlib
import logging

import pandas as pd

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.extraction")

# Extract structured learning outcomes from free-text program description
class LearningOutcomeExtractor:

    # ── Explicit outcome patterns (original set) ──────────────────────────
    OUTCOME_PATTERNS = [
        # "students will/can/are able to <verb>..."
        r"(?:students?\s+(?:will|can|shall|are?\s+(?:able|expected|prepared))\s+(?:to\s+)?)"
        r"([a-z][^.;]{15,120})",
        # "prepared to <verb>..."
        r"(?:prepared?\s+to\s+)([a-z][^.;]{10,100})",
        # "equipped with knowledge/skills/competencies to/in/for..."
        r"(?:equip(?:ped|s)?\s+(?:with\s+)?(?:knowledge|skills?|competenc)\w*\s+(?:to|in|for)\s+)"
        r"([a-z][^.;]{10,100})",
        # "qualified to <verb>..."
        r"(?:qualified?\s+to\s+)([a-z][^.;]{10,100})",
        # Sentence starting with action verb
        r"(?:^|\.\s+)((?:identify|assess|design|implement|manage|develop|evaluate|analyse|analyze"
        r"|plan|maintain|conduct|monitor|protect|configure|audit|investigate|respond|deploy"
        r"|test|create|build|secure|detect|prevent|mitigate)"
        r"[^.;]{10,120})",
    ]

    # ── Implicit / indirect patterns (match real-world description style) ──
    IMPLICIT_PATTERNS = [
        # "aims at/to <gerund/verb>..."
        r"(?:aims?\s+(?:at|to)\s+)([a-z][^.;]{10,120})",
        # "the aim/goal/objective is to..."
        r"(?:(?:the\s+)?(?:aim|goal|objective)\s+is\s+to\s+)([a-z][^.;]{10,120})",
        # "graduates will acquire/gain/obtain/have..."
        r"(?:graduates?\s+(?:will\s+)?(?:acquire|gain|have|obtain|possess|develop|know|learn)\w*\s+)"
        r"([a-z][^.;]{10,120})",
        # "focuses on / addressing..."
        r"(?:(?:focus(?:es|ing)?|focuss?ed)\s+on\s+)([a-z][^.;]{10,120})",
        # "covers / covering areas such as..."
        r"(?:covers?\s+(?:areas?\s+(?:such\s+as|including|like)\s+)?)([a-z][^.;]{10,120})",
        # "providing/supplying students with..."
        r"(?:provid(?:es?|ing)\s+(?:students?\s+)?(?:with\s+)?)([a-z][^.;]{12,120})",
        # "developing/forming [noun] skills/knowledge/view..."
        r"(?:develop(?:ing|s)?\s+)([a-z][^.;]{8,100}(?:skills?|knowledge|thinking|view|competenc)\w*)",
        # "topics/modules include..."
        r"(?:(?:topics?|modules?|courses?|subjects?)\s+(?:include|cover|address)\w*\s+)"
        r"([a-z][^.;]{10,120})",
        # "enable them to / enabling..."
        r"(?:enabl(?:es?|ing)\s+(?:them\s+)?(?:to\s+)?)([a-z][^.;]{10,120})",
        # "training/preparing [for] ..."
        r"(?:(?:training|preparing)\s+(?:for\s+)?(?:roles?\s+)?(?:in\s+|as\s+)?)([a-z][^.;]{10,100})",
        # "learn how to..."
        r"(?:learn\s+(?:how\s+)?to\s+)([a-z][^.;]{10,120})",
    ]

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._compiled = [re.compile(p, re.IGNORECASE) for p in self.OUTCOME_PATTERNS]
        self._implicit_compiled = [re.compile(p, re.IGNORECASE) for p in self.IMPLICIT_PATTERNS]

    def extract_outcomes(self, description: str) -> list[dict]:
        """Return list of {text, bloom_level, verbs} dicts."""
        if pd.isna(description):
            return []
        text = str(description)
        outcomes: list[dict] = []
        seen: set[str] = set()

        # First pass: explicit patterns
        for pat in self._compiled:
            for m in pat.finditer(text.lower()):
                raw = m.group(1) if m.lastindex else m.group(0)
                raw = raw.strip(" ,;")
                sig = hashlib.md5(raw[:40].encode()).hexdigest()
                if sig in seen or len(raw) < 12:
                    continue
                seen.add(sig)
                bloom = self._classify_bloom(raw)
                outcomes.append({
                    "text": raw,
                    "bloom_level": bloom,
                    "verbs": self._extract_verbs(raw),
                })

        # Second pass: implicit patterns (only if we found few explicit ones)
        for pat in self._implicit_compiled:
            for m in pat.finditer(text.lower()):
                raw = m.group(1) if m.lastindex else m.group(0)
                raw = raw.strip(" ,;")
                sig = hashlib.md5(raw[:40].encode()).hexdigest()
                if sig in seen or len(raw) < 12:
                    continue
                seen.add(sig)
                bloom = self._classify_bloom(raw)
                outcomes.append({
                    "text": raw,
                    "bloom_level": bloom,
                    "verbs": self._extract_verbs(raw),
                })

        return outcomes

    def _classify_bloom(self, text: str) -> str:
        text_lower = text.lower().split()[:5]
        for level in reversed(list(self.config.bloom_verbs.keys())):
            for verb in self.config.bloom_verbs[level]:
                if verb in text_lower:
                    return level
        return "Apply"

    def _extract_verbs(self, text: str) -> list[str]:
        words = text.lower().split()[:6]
        all_verbs = {v for vs in self.config.bloom_verbs.values() for v in vs}
        return [w for w in words if w in all_verbs]


class AssessmentMethodExtractor:
    # Additional assessment keywords not in the user-facing taxonomy but
    # that map to existing categories
    _EXTRA_KEYWORDS: dict[str, list[str]] = {
        "practical_lab": [
            "laboratory", "hands on", "cyber range", "virtual lab",
            "sandbox", "emulation", "practicum",
        ],
        "project": [
            "master thesis", "master's thesis", "bachelor thesis",
            "final thesis", "thesis defense", "thesis only",
            "research work", "individual project", "term project",
        ],
        "exam_written": [
            "examination", "assessment", "coursework",
            "assignment", "homework", "quiz",
        ],
        "internship": [
            "work-based learning", "on-the-job", "traineeship",
            "industry professional", "industry expert",
            "police forensic", "consulting",
        ],
        "group_work": [
            "teamwork", "team work", "group project",
            "collaborative work", "independent and group work",
        ],
        "case_study": [
            "use case", "real world", "real-life scenario",
        ],
        "presentation": [
            "seminar presentation", "conference", "poster",
            "report writing",
        ],
    }

    def __init__(self, config: PipelineConfig):
        self.config = config
        # Build a combined keyword → method mapping
        self._patterns: dict[str, re.Pattern] = {}
        for method, info in config.assessment_methods.items():
            all_kws = list(info["keywords"])
            all_kws.extend(self._EXTRA_KEYWORDS.get(method, []))
            kw_pattern = "|".join(re.escape(k) for k in all_kws)
            self._patterns[method] = re.compile(kw_pattern, re.IGNORECASE)

    def extract_methods(self, description: str) -> list[dict]:
        if pd.isna(description):
            return []
        text = str(description)
        found: list[dict] = []
        for method, pat in self._patterns.items():
            hits = pat.findall(text)
            if hits:
                info = self.config.assessment_methods[method]
                found.append({
                    "method": method,
                    "evidence_keywords": list(set(h.lower() for h in hits)),
                    "weight": info["weight"],
                    "bloom_level": info["bloom_level"],
                })

        # If a description clearly describes a master's/bachelor's program but
        # no assessment methods were found, infer basic defaults
        if not found:
            text_lower = text.lower()
            implicit: list[dict] = []
            # Master's/Bachelor's programs typically have exams
            if any(w in text_lower for w in [
                "master", "bachelor", "degree", "programme", "program",
                "semester", "ects", "credit",
            ]):
                implicit.append({
                    "method": "exam_written",
                    "evidence_keywords": ["[inferred:degree_program]"],
                    "weight": 0.4,  # lower confidence
                    "bloom_level": "Understand",
                })
            # Master's thesis is standard for master's
            if any(w in text_lower for w in [
                "master", "msc", "m.sc", "postgraduate",
            ]):
                implicit.append({
                    "method": "project",
                    "evidence_keywords": ["[inferred:masters_thesis]"],
                    "weight": 0.6,  # lower confidence
                    "bloom_level": "Create",
                })
            found.extend(implicit)

        return found
