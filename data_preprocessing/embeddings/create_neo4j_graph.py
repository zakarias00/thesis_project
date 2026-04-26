"""
Create a Neo4j graph from course embeddings and metadata.

This script:
1. Loads embeddings and metadata created by create_embeddings.py
2. Connects to Neo4j database
3. Creates nodes for courses with their embeddings
4. Creates relationships based on:
   - Similarity scores between courses
   - Shared skills
   - Other metadata relationships
"""

import argparse
import os
import sys
from typing import List, Optional, Dict, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("WARNING: neo4j package not installed. Install with: pip install neo4j", file=sys.stderr)


class Neo4jGraphBuilder:
    """Build a Neo4j graph from course embeddings and metadata."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j package is required. Install with: pip install neo4j")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships from the database."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.", file=sys.stderr)
    
    def create_constraints(self):
        """Create constraints and indexes for better performance."""
        with self.driver.session() as session:
            # Create uniqueness constraint on course_id
            try:
                session.run("CREATE CONSTRAINT course_id_unique IF NOT EXISTS FOR (c:Course) REQUIRE c.course_id IS UNIQUE")
            except Exception as e:
                print(f"Note: Could not create constraint: {e}", file=sys.stderr)
            
            # Create index on course title
            try:
                session.run("CREATE INDEX course_title_idx IF NOT EXISTS FOR (c:Course) ON (c.title)")
            except Exception as e:
                print(f"Note: Could not create index: {e}", file=sys.stderr)
    
    def create_course_node(self, course_data: Dict) -> None:
        """Create a Course node in Neo4j."""
        with self.driver.session() as session:
            query = """
            CREATE (c:Course {
                course_id: $course_id,
                title: $title,
                description: $description,
                original_description: $original_description,
                row_index: $row_index,
                embedding: $embedding
            })
            """
            session.run(query, **course_data)
    
    def create_skill_node(self, skill: str) -> None:
        """Create a Skill node in Neo4j."""
        with self.driver.session() as session:
            query = """
            MERGE (s:Skill {name: $skill})
            """
            session.run(query, skill=skill)
    
    def create_course_skill_relationship(self, course_id: int, skill: str) -> None:
        """Create a relationship between a Course and a Skill."""
        with self.driver.session() as session:
            query = """
            MATCH (c:Course {course_id: $course_id})
            MATCH (s:Skill {name: $skill})
            CREATE (c)-[:HAS_SKILL]->(s)
            """
            session.run(query, course_id=course_id, skill=skill)
    
    def create_similarity_relationship(self, course_id_1: int, course_id_2: int, similarity: float) -> None:
        """Create a similarity relationship between two courses."""
        with self.driver.session() as session:
            query = """
            MATCH (c1:Course {course_id: $course_id_1})
            MATCH (c2:Course {course_id: $course_id_2})
            CREATE (c1)-[:SIMILAR_TO {similarity: $similarity}]->(c2)
            """
            session.run(query, course_id_1=course_id_1, course_id_2=course_id_2, similarity=similarity)
    
    def batch_create_courses(self, courses: List[Dict]) -> None:
        """Create multiple course nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $courses AS course
            CREATE (c:Course {
                course_id: course.course_id,
                title: course.title,
                description: course.description,
                original_description: course.original_description,
                row_index: course.row_index,
                embedding: course.embedding
            })
            """
            session.run(query, courses=courses)
    
    def batch_create_skills(self, skills: List[str]) -> None:
        """Create multiple skill nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $skills AS skill
            MERGE (s:Skill {name: skill})
            """
            session.run(query, skills=skills)
    
    def batch_create_course_skill_relationships(self, relationships: List[Dict]) -> None:
        """Create multiple course-skill relationships in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $relationships AS rel
            MATCH (c:Course {course_id: rel.course_id})
            MATCH (s:Skill {name: rel.skill})
            CREATE (c)-[:HAS_SKILL]->(s)
            """
            session.run(query, relationships=relationships)
    
    def batch_create_similarity_relationships(self, relationships: List[Dict]) -> None:
        """Create multiple similarity relationships in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $relationships AS rel
            MATCH (c1:Course {course_id: rel.course_id_1})
            MATCH (c2:Course {course_id: rel.course_id_2})
            CREATE (c1)-[:SIMILAR_TO {similarity: rel.similarity}]->(c2)
            """
            session.run(query, relationships=relationships)


def load_embeddings(embeddings_path: str, metadata_path: str) -> Tuple[np.ndarray, pd.DataFrame]:
    """Load embeddings and metadata from files."""
    if not os.path.exists(embeddings_path):
        raise FileNotFoundError(f"Embeddings file not found: {embeddings_path}")
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    
    embeddings = np.load(embeddings_path)
    metadata = pd.read_csv(metadata_path)
    
    print(f"Loaded embeddings: {embeddings.shape}", file=sys.stderr)
    print(f"Loaded metadata: {metadata.shape}", file=sys.stderr)
    
    return embeddings, metadata


def load_original_dataset(dataset_path: str) -> pd.DataFrame:
    """Load the original courses dataset."""
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset file not found: {dataset_path}")
    
    df = pd.read_csv(dataset_path, encoding="utf-8")
    print(f"Loaded original dataset: {df.shape}", file=sys.stderr)
    
    return df


def compute_similarity_matrix(embeddings: np.ndarray) -> np.ndarray:
    """Compute cosine similarity matrix for embeddings."""
    print("Computing similarity matrix...", file=sys.stderr)
    similarity_matrix = cosine_similarity(embeddings)
    return similarity_matrix


def get_top_similar_courses(similarity_matrix: np.ndarray, 
                            top_k: int = 5, 
                            threshold: float = 0.5) -> List[Tuple[int, int, float]]:
    """Get top-k similar courses for each course above a threshold."""
    similar_courses = []
    
    for i in range(len(similarity_matrix)):
        # Get similarity scores for course i
        scores = similarity_matrix[i]
        
        # Get indices sorted by similarity (excluding self)
        sorted_indices = np.argsort(scores)[::-1]
        
        count = 0
        for j in sorted_indices:
            if i == j:  # Skip self-similarity
                continue
            if scores[j] < threshold:  # Below threshold
                break
            if count >= top_k:  # Reached top-k
                break
            
            similar_courses.append((i, j, float(scores[j])))
            count += 1
    
    return similar_courses


def parse_skills(skills_str: str) -> List[str]:
    """Parse skills from comma-separated string."""
    if pd.isna(skills_str) or not skills_str:
        return []
    
    # Split by comma and clean
    skills = [s.strip() for s in str(skills_str).split(",")]
    skills = [s for s in skills if s]  # Remove empty strings
    
    return skills


def main():
    parser = argparse.ArgumentParser(description="Create Neo4j graph from course embeddings.")
    parser.add_argument("--embeddings", "-e", type=str, required=True, 
                       help="Path to embeddings .npy file.")
    parser.add_argument("--metadata", "-m", type=str, required=True, 
                       help="Path to metadata .csv file.")
    parser.add_argument("--dataset", "-d", type=str, required=True, 
                       help="Path to original courses dataset .csv file.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687", 
                       help="Neo4j database URI.")
    parser.add_argument("--user", "-u", type=str, default="neo4j", 
                       help="Neo4j username.")
    parser.add_argument("--password", "-p", type=str, required=True, 
                       help="Neo4j password.")
    parser.add_argument("--clear", action="store_true", 
                       help="Clear existing data before import.")
    parser.add_argument("--top-k", type=int, default=5, 
                       help="Number of top similar courses to link.")
    parser.add_argument("--similarity-threshold", type=float, default=0.5, 
                       help="Minimum similarity score for creating relationships.")
    parser.add_argument("--batch-size", type=int, default=100, 
                       help="Batch size for database operations.")
    
    args = parser.parse_args()
    
    # Load data
    print("Loading data...", file=sys.stderr)
    embeddings, metadata = load_embeddings(args.embeddings, args.metadata)
    df = load_original_dataset(args.dataset)
    
    # Initialize Neo4j connection
    print(f"Connecting to Neo4j at {args.uri}...", file=sys.stderr)
    graph_builder = Neo4jGraphBuilder(args.uri, args.user, args.password)
    
    try:
        # Clear database if requested
        if args.clear:
            graph_builder.clear_database()
        
        # Create constraints and indexes
        print("Creating constraints and indexes...", file=sys.stderr)
        graph_builder.create_constraints()
        
        # Prepare course nodes
        print("Preparing course nodes...", file=sys.stderr)
        courses = []
        for idx, row in tqdm(metadata.iterrows(), total=len(metadata), desc="Preparing courses"):
            row_index = int(row['row_index'])
            
            # Get course data from original dataset
            course_row = df.iloc[row_index]
            
            course_data = {
                'course_id': int(idx),
                'title': str(course_row.get('course_title', '')),
                'description': str(course_row.get('Description', '')),
                'original_description': str(course_row.get('original_description', '')),
                'row_index': row_index,
                'embedding': embeddings[idx].tolist()
            }
            courses.append(course_data)
        
        # Create course nodes in batches
        print("Creating course nodes...", file=sys.stderr)
        for i in tqdm(range(0, len(courses), args.batch_size), desc="Creating courses"):
            batch = courses[i:i + args.batch_size]
            graph_builder.batch_create_courses(batch)
        
        # Extract and create skill nodes
        print("Extracting skills...", file=sys.stderr)
        all_skills = set()
        course_skills = []
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing skills"):
            skills = parse_skills(row.get('extracted_skills', ''))
            for skill in skills:
                all_skills.add(skill)
                course_skills.append({
                    'course_id': int(idx),
                    'skill': skill
                })
        
        print(f"Found {len(all_skills)} unique skills.", file=sys.stderr)
        
        # Create skill nodes in batches
        print("Creating skill nodes...", file=sys.stderr)
        skills_list = list(all_skills)
        for i in tqdm(range(0, len(skills_list), args.batch_size), desc="Creating skills"):
            batch = skills_list[i:i + args.batch_size]
            graph_builder.batch_create_skills(batch)
        
        # Create course-skill relationships in batches
        print("Creating course-skill relationships...", file=sys.stderr)
        for i in tqdm(range(0, len(course_skills), args.batch_size), desc="Creating relationships"):
            batch = course_skills[i:i + args.batch_size]
            graph_builder.batch_create_course_skill_relationships(batch)
        
        # Compute similarity and create similarity relationships
        print("Computing course similarities...", file=sys.stderr)
        similarity_matrix = compute_similarity_matrix(embeddings)
        
        print("Finding similar courses...", file=sys.stderr)
        similar_courses = get_top_similar_courses(
            similarity_matrix, 
            top_k=args.top_k, 
            threshold=args.similarity_threshold
        )
        
        print(f"Found {len(similar_courses)} similarity relationships.", file=sys.stderr)
        
        # Create similarity relationships in batches
        print("Creating similarity relationships...", file=sys.stderr)
        similarity_rels = [
            {'course_id_1': int(c1), 'course_id_2': int(c2), 'similarity': float(sim)}
            for c1, c2, sim in similar_courses
        ]
        
        for i in tqdm(range(0, len(similarity_rels), args.batch_size), desc="Creating similarities"):
            batch = similarity_rels[i:i + args.batch_size]
            graph_builder.batch_create_similarity_relationships(batch)
        
        print("\nGraph creation complete!", file=sys.stderr)
        print(f"- Created {len(courses)} course nodes", file=sys.stderr)
        print(f"- Created {len(all_skills)} skill nodes", file=sys.stderr)
        print(f"- Created {len(course_skills)} course-skill relationships", file=sys.stderr)
        print(f"- Created {len(similar_courses)} similarity relationships", file=sys.stderr)
        
    finally:
        graph_builder.close()


if __name__ == "__main__":
    main()
