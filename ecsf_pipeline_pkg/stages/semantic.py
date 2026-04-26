# NLP, Ontology & Embedding semantic methods

from __future__ import annotations

import re
import logging

import numpy as np
import pandas as pd

from ..config import PipelineConfig

logger = logging.getLogger("ecsf_pipeline.semantic")


# NLP Competency Extractor (spaCy-based)
class NLPCompetencyExtractor:

    def __init__(self, config: PipelineConfig, model_name: str | None = None):
        self.config = config
        model_name = model_name or config.spacy_model
        self._nlp = None
        self._model_name = model_name
        self._framework_vocab: dict[str, set[str]] = {}
        self._is_built = False

    def _load_nlp(self):
        if self._nlp is None:
            import spacy
            try:
                self._nlp = spacy.load(self._model_name)
            except OSError:
                logger.warning("spaCy model '%s' not found; falling back to blank 'en'.",
                               self._model_name)
                self._nlp = spacy.blank("en")

    def build_vocabulary(self, skills_df: pd.DataFrame) -> None:
        self._load_nlp()
        self._framework_vocab = {}
        for _, row in skills_df.iterrows():
            role = row["profile_title"]
            texts = (
                row.get("key_skills_list", [])
                + row.get("key_knowledge_list", [])
                + [str(row.get("mission", ""))]
                + row.get("main_tasks_list", [])
            )
            all_lemmas: set[str] = set()
            for t in texts:
                doc = self._nlp(str(t).lower())
                all_lemmas.update(
                    tok.lemma_ for tok in doc
                    if not tok.is_stop and not tok.is_punct and len(tok.lemma_) > 2
                )
            self._framework_vocab[role] = all_lemmas
        self._is_built = True
        total_lemmas = sum(len(v) for v in self._framework_vocab.values())
        logger.info("NLP vocabulary: %d roles, %d unique lemmas",
                     len(self._framework_vocab), total_lemmas)

    def extract_competencies(self, description: str) -> dict:
        if pd.isna(description) or not self._is_built:
            return self._empty_result()

        self._load_nlp()
        doc = self._nlp(str(description))

        cyber_terms = {
            "security", "cyber", "network", "forensic", "threat",
            "risk", "incident", "audit", "compliance", "intelligence",
            "pentest", "penetration", "cryptograph", "malware", "data",
            "privacy", "governance", "vulnerability", "defense", "defence",
            "architecture", "software", "system", "cloud", "iot",
        }
        noun_chunks = [
            chunk.text.strip()
            for chunk in doc.noun_chunks
            if any(t in chunk.text.lower() for t in cyber_terms)
        ][:30]

        verb_objects: list[dict] = []
        for token in doc:
            if token.pos_ == "VERB" and token.dep_ in ("ROOT", "conj", "advcl", "relcl", "xcomp"):
                for child in token.children:
                    if child.dep_ in ("dobj", "attr", "pobj"):
                        span_text = " ".join(t.text for t in child.subtree)
                        if len(span_text) > 5:
                            verb_objects.append({
                                "verb": token.lemma_.lower(),
                                "object": span_text.strip()[:80],
                            })
        verb_objects = verb_objects[:20]

        desc_lemmas: set[str] = set()
        for tok in doc:
            if not tok.is_stop and not tok.is_punct and len(tok.lemma_) > 2:
                desc_lemmas.add(tok.lemma_.lower())
        normalised_text = " ".join(sorted(desc_lemmas))

        lemma_overlap: dict[str, dict] = {}
        nlp_role_scores: dict[str, float] = {}
        for role, vocab in self._framework_vocab.items():
            if not vocab:
                nlp_role_scores[role] = 0.0
                continue
            overlap = desc_lemmas & vocab
            score = len(overlap) / len(vocab)
            lemma_overlap[role] = {
                "overlap_count": len(overlap),
                "vocab_size": len(vocab),
                "overlap_ratio": round(score, 4),
                "top_shared": sorted(overlap)[:10],
            }
            nlp_role_scores[role] = round(score, 4)

        threshold = 0.10
        top_roles = [r for r, s in nlp_role_scores.items() if s >= threshold]

        return {
            "noun_chunks": noun_chunks,
            "verb_objects": verb_objects,
            "lemma_overlap": lemma_overlap,
            "nlp_role_scores": nlp_role_scores,
            "top_roles": top_roles,
            "normalised_text": normalised_text[:500],
        }

    def _empty_result(self):
        return {
            "noun_chunks": [], "verb_objects": [], "lemma_overlap": {},
            "nlp_role_scores": {}, "top_roles": [], "normalised_text": "",
        }


# ECSF ontology builder + alignment scorer using rdflib
class OntologyAligner:

    ECSF_NS = "http://enisa.europa.eu/ecsf#"

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._graph = None
        self._node_labels: dict[str, str] = {}
        self._role_uris: dict[str, object] = {}

    def build_ontology(self, skills_df: pd.DataFrame) -> None:
        from rdflib import Graph, Namespace, Literal, RDF, RDFS, OWL

        g = Graph()
        ECSF = Namespace(self.ECSF_NS)
        g.bind("ecsf", ECSF)
        g.bind("owl", OWL)

        for cls_name in ("Role", "Skill", "KnowledgeItem", "Task"):
            g.add((ECSF[cls_name], RDF.type, OWL.Class))
        for prop in ("hasSkill", "hasKnowledge", "hasTask"):
            g.add((ECSF[prop], RDF.type, OWL.ObjectProperty))
        g.add((ECSF.relatedTo, RDF.type, OWL.SymmetricProperty))

        def _uri_safe(text):
            return re.sub(r"[^a-zA-Z0-9_]", "_", text.strip())[:60]

        for _, row in skills_df.iterrows():
            role_name = row["profile_title"]
            role_uri = ECSF[_uri_safe(role_name)]
            g.add((role_uri, RDF.type, ECSF.Role))
            g.add((role_uri, RDFS.label, Literal(role_name)))
            self._role_uris[role_name] = role_uri
            self._node_labels[str(role_uri)] = role_name

            for sk in row.get("key_skills_list", []):
                sk_uri = ECSF["skill_" + _uri_safe(sk)]
                g.add((sk_uri, RDF.type, ECSF.Skill))
                g.add((sk_uri, RDFS.label, Literal(sk)))
                g.add((role_uri, ECSF.hasSkill, sk_uri))
                self._node_labels[str(sk_uri)] = sk

            for kn in row.get("key_knowledge_list", []):
                kn_uri = ECSF["knowledge_" + _uri_safe(kn)]
                g.add((kn_uri, RDF.type, ECSF.KnowledgeItem))
                g.add((kn_uri, RDFS.label, Literal(kn)))
                g.add((role_uri, ECSF.hasKnowledge, kn_uri))
                self._node_labels[str(kn_uri)] = kn

            for task in row.get("main_tasks_list", []):
                task_uri = ECSF["task_" + _uri_safe(task)]
                g.add((task_uri, RDF.type, ECSF.Task))
                g.add((task_uri, RDFS.label, Literal(task)))
                g.add((role_uri, ECSF.hasTask, task_uri))
                self._node_labels[str(task_uri)] = task

        self._add_cross_role_relations(g, ECSF)
        self._graph = g
        logger.info("Ontology: %d triples, %d roles, %d labelled nodes",
                     len(g), len(self._role_uris), len(self._node_labels))

    def _add_cross_role_relations(self, g, ECSF) -> None:
        from rdflib import RDF
        stop = {"and", "the", "of", "to", "in", "for", "a", "an", "on", "with",
                "is", "are", "be", "or", "as", "by"}
        skills_by_uri: dict = {}
        for s, _, _ in g.triples((None, RDF.type, ECSF.Skill)):
            label = self._node_labels.get(str(s), "")
            tokens = set(w for w in re.findall(r"[a-z]{3,}", label.lower()) if w not in stop)
            skills_by_uri[s] = tokens
        uris = list(skills_by_uri.keys())
        count = 0
        for i in range(len(uris)):
            for j in range(i + 1, len(uris)):
                t1, t2 = skills_by_uri[uris[i]], skills_by_uri[uris[j]]
                if t1 and t2 and len(t1 & t2) / min(len(t1), len(t2)) >= 0.5:
                    g.add((uris[i], ECSF.relatedTo, uris[j]))
                    count += 1
        logger.info("  Added %d cross-skill relatedTo edges", count)

    def export_ontology(self, path: str = "ecsf_ontology.ttl", fmt: str = "turtle") -> None:
        if self._graph is None:
            raise ValueError("Ontology not built yet.")
        self._graph.serialize(destination=path, format=fmt)
        logger.info("Ontology exported → %s", path)

    def align_description(
        self, description: str, nlp_extractor: NLPCompetencyExtractor
    ) -> dict:
        if self._graph is None:
            return {"direct_alignment": {}, "graph_boosted": {},
                    "ontology_coverage": 0.0, "aligned_nodes": 0, "total_nodes": 0}

        nlp_result = nlp_extractor.extract_competencies(description)
        direct = nlp_result.get("nlp_role_scores", {})
        boosted = dict(direct)

        try:
            query = """
            PREFIX ecsf: <%s>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?role ?roleName (COUNT(DISTINCT ?related) AS ?relatedCount)
            WHERE {
                ?role a ecsf:Role .
                ?role rdfs:label ?roleName .
                ?role ecsf:hasSkill ?skill .
                ?skill ecsf:relatedTo ?otherSkill .
                ?otherRole ecsf:hasSkill ?otherSkill .
                ?otherRole a ecsf:Role .
                FILTER(?role != ?otherRole)
                BIND(?otherRole AS ?related)
            }
            GROUP BY ?role ?roleName
            """ % self.ECSF_NS
            results = self._graph.query(query)
            role_connectivity: dict[str, int] = {}
            for row in results:
                role_connectivity[str(row.roleName)] = int(row.relatedCount)

            for role in boosted:
                connectivity = role_connectivity.get(role, 0)
                if connectivity > 0:
                    related_boost = sum(
                        s * 0.15 for r, s in direct.items() if r != role and s > 0
                    )
                    boosted[role] = round(boosted.get(role, 0) + min(related_boost, 0.10), 4)
        except Exception as e:
            logger.warning("SPARQL boost failed: %s", e)

        desc_lemmas = set(nlp_result.get("normalised_text", "").split())
        aligned = sum(
            1 for label in self._node_labels.values()
            if len(desc_lemmas & set(re.findall(r"[a-z]{3,}", label.lower()))) >= 2
        )
        total_nodes = len(self._node_labels)

        return {
            "direct_alignment": direct,
            "graph_boosted": boosted,
            "ontology_coverage": round(aligned / max(total_nodes, 1), 4),
            "aligned_nodes": aligned,
            "total_nodes": total_nodes,
        }

    def get_ontology_stats(self) -> dict:
        if self._graph is None:
            return {}
        return {
            "total_triples": len(self._graph),
            "roles": len(self._role_uris),
            "labelled_nodes": len(self._node_labels),
        }

# Sentence-BERT embedding similarity, gap detection, clustering
class EmbeddingAnalyzer:

    def __init__(self, config: PipelineConfig, model_name: str | None = None):
        self.config = config
        self.model_name = model_name or config.embedding_model
        self._model = None
        self._framework_embeddings: dict[str, np.ndarray] = {}
        self._framework_texts: dict[str, str] = {}
        self._framework_roles: dict[str, str] = {}
        self._program_embeddings: dict[str, np.ndarray] = {}
        self._is_built = False

    def _load_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            logger.info("Loaded embedding model: %s (dim=%d)",
                         self.model_name, self._model.get_sentence_embedding_dimension())

    def build_framework_index(self, skills_df: pd.DataFrame) -> None:
        self._load_model()
        texts, keys, roles = [], [], []
        for _, row in skills_df.iterrows():
            role = row["profile_title"]
            for sk in row.get("key_skills_list", []):
                key = f"skill|{role}|{sk[:50]}"
                texts.append(sk); keys.append(key); roles.append(role)
            for kn in row.get("key_knowledge_list", []):
                key = f"knowledge|{role}|{kn[:50]}"
                texts.append(kn); keys.append(key); roles.append(role)
        if not texts:
            logger.warning("No framework texts to embed."); return
        embeddings = self._model.encode(texts, show_progress_bar=False, batch_size=64)
        for i, key in enumerate(keys):
            self._framework_embeddings[key] = embeddings[i]
            self._framework_texts[key] = texts[i]
            self._framework_roles[key] = roles[i]
        self._is_built = True
        logger.info("Framework index: %d items (dim=%d)", len(keys), embeddings.shape[1])

    def encode_program(self, program_name: str, description: str) -> np.ndarray:
        self._load_model()
        if pd.isna(description):
            emb = np.zeros(self._model.get_sentence_embedding_dimension())
        else:
            emb = self._model.encode(str(description), show_progress_bar=False)
        self._program_embeddings[program_name] = emb
        return emb

    def compute_similarity_matrix(self, description: str) -> dict:
        if not self._is_built or pd.isna(description):
            return self._empty_sim_result()
        self._load_model()
        from sklearn.metrics.pairwise import cosine_similarity
        desc_emb = self._model.encode(str(description), show_progress_bar=False)
        fw_keys = list(self._framework_embeddings.keys())
        fw_matrix = np.stack([self._framework_embeddings[k] for k in fw_keys])
        sims = cosine_similarity(desc_emb.reshape(1, -1), fw_matrix)[0]

        items, role_sums, role_counts = [], {}, {}
        for i, key in enumerate(fw_keys):
            role = self._framework_roles[key]
            score = float(sims[i])
            items.append({"key": key, "text": self._framework_texts[key],
                          "role": role, "score": round(score, 4)})
            role_sums[role] = role_sums.get(role, 0) + score
            role_counts[role] = role_counts.get(role, 0) + 1
        items.sort(key=lambda x: x["score"], reverse=True)
        role_scores = {r: round(role_sums[r] / role_counts[r], 4) for r in role_sums}
        return {
            "similarities": items,
            "role_scores": role_scores,
            "top_matches": items[:15],
            "weak_items": [it for it in items if it["score"] < 0.20],
            "mean_score": round(float(np.mean(sims)), 4),
        }

    def detect_semantic_gaps(self, courses_df: pd.DataFrame) -> dict:
        if not self._is_built:
            return {"gap_items": [], "coverage_heatmap": {}, "gap_summary": {}}
        self._load_model()
        from sklearn.metrics.pairwise import cosine_similarity
        desc_col = "description" if "description" in courses_df.columns else "study program description"
        descriptions = courses_df[desc_col].tolist()
        prog_embs = self._model.encode(
            [str(d) if not pd.isna(d) else "" for d in descriptions],
            show_progress_bar=False, batch_size=16)
        fw_keys = list(self._framework_embeddings.keys())
        fw_matrix = np.stack([self._framework_embeddings[k] for k in fw_keys])
        sim_matrix = cosine_similarity(prog_embs, fw_matrix)
        max_sims = sim_matrix.max(axis=0)
        mean_sims = sim_matrix.mean(axis=0)

        gap_threshold = 0.25
        gap_items, coverage_heatmap, gap_summary = [], {}, {}
        for i, key in enumerate(fw_keys):
            role = self._framework_roles[key]
            coverage_heatmap.setdefault(role, {})[key] = round(float(max_sims[i]), 4)
            if max_sims[i] < gap_threshold:
                gap_items.append({
                    "key": key, "text": self._framework_texts[key],
                    "role": role,
                    "max_similarity": round(float(max_sims[i]), 4),
                    "mean_similarity": round(float(mean_sims[i]), 4),
                })
                gap_summary[role] = gap_summary.get(role, 0) + 1
        gap_items.sort(key=lambda x: x["max_similarity"])
        return {
            "gap_items": gap_items,
            "coverage_heatmap": coverage_heatmap,
            "gap_summary": gap_summary,
            "sim_matrix": sim_matrix,
            "fw_keys": fw_keys,
        }

    def cluster_programs(self, courses_df: pd.DataFrame, n_clusters: int = 5) -> dict:
        self._load_model()
        desc_col = "description" if "description" in courses_df.columns else "study program description"
        descriptions = courses_df[desc_col].tolist()
        name_col = "study_program_name" if "study_program_name" in courses_df.columns else "study program name"
        uni_col = "university_name" if "university_name" in courses_df.columns else "university name"
        names = courses_df[name_col].tolist()
        universities = courses_df[uni_col].tolist()
        embeddings = self._model.encode(
            [str(d) if not pd.isna(d) else "" for d in descriptions],
            show_progress_bar=False, batch_size=16)
        from sklearn.cluster import KMeans
        n_clusters = min(n_clusters, len(descriptions))
        km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = km.fit_predict(embeddings)
        cluster_info = [
            {"cluster_id": c, "size": sum(1 for l in labels if l == c),
             "programs": [(names[i], universities[i]) for i in range(len(labels)) if labels[i] == c]}
            for c in range(n_clusters)
        ]
        return {"labels": labels.tolist(), "centroids": km.cluster_centers_,
                "cluster_info": cluster_info, "embeddings": embeddings}

    def _empty_sim_result(self):
        return {"similarities": [], "role_scores": {}, "top_matches": [],
                "weak_items": [], "mean_score": 0.0}
