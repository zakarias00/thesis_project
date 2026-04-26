"""
Create a Neo4j graph from ENISA cybersecurity skill set dataset.

This script:
1. Loads the ENISA skill set CSV file
2. Connects to Neo4j database
3. Creates nodes for:
   - Cybersecurity Profiles (roles)
   - Skills
   - Knowledge areas
   - Deliverables
4. Creates relationships based on:
   - Profile HAS_SKILL
   - Profile REQUIRES_KNOWLEDGE
   - Profile PRODUCES_DELIVERABLE
   - Skill/Knowledge shared between profiles
"""

import argparse
import os
import sys
from typing import List, Dict, Set

import pandas as pd
from tqdm import tqdm

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("WARNING: neo4j package not installed. Install with: pip install neo4j", file=sys.stderr)


class EnisaGraphBuilder:
    """Build a Neo4j graph from ENISA cybersecurity profiles dataset."""
    
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
            # Create uniqueness constraints
            constraints = [
                "CREATE CONSTRAINT profile_title_unique IF NOT EXISTS FOR (p:Profile) REQUIRE p.title IS UNIQUE",
                "CREATE CONSTRAINT skill_name_unique IF NOT EXISTS FOR (s:Skill) REQUIRE s.name IS UNIQUE",
                "CREATE CONSTRAINT knowledge_name_unique IF NOT EXISTS FOR (k:Knowledge) REQUIRE k.name IS UNIQUE",
                "CREATE CONSTRAINT deliverable_name_unique IF NOT EXISTS FOR (d:Deliverable) REQUIRE d.name IS UNIQUE",
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Note: Could not create constraint: {e}", file=sys.stderr)
            
            # Create indexes
            indexes = [
                "CREATE INDEX profile_no_idx IF NOT EXISTS FOR (p:Profile) ON (p.profile_no)",
                "CREATE INDEX skill_name_idx IF NOT EXISTS FOR (s:Skill) ON (s.name)",
                "CREATE INDEX knowledge_name_idx IF NOT EXISTS FOR (k:Knowledge) ON (k.name)",
            ]
            
            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    print(f"Note: Could not create index: {e}", file=sys.stderr)
    
    def batch_create_profiles(self, profiles: List[Dict]) -> None:
        """Create multiple profile nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $profiles AS profile
            CREATE (p:Profile {
                profile_no: profile.profile_no,
                title: profile.title,
                mission: profile.mission,
                main_tasks: profile.main_tasks
            })
            """
            session.run(query, profiles=profiles)
    
    def batch_create_skills(self, skills: List[str]) -> None:
        """Create multiple skill nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $skills AS skill
            MERGE (s:Skill {name: skill})
            """
            session.run(query, skills=list(skills))
    
    def batch_create_knowledge(self, knowledge_items: List[str]) -> None:
        """Create multiple knowledge nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $knowledge_items AS knowledge
            MERGE (k:Knowledge {name: knowledge})
            """
            session.run(query, knowledge_items=list(knowledge_items))
    
    def batch_create_deliverables(self, deliverables: List[str]) -> None:
        """Create multiple deliverable nodes in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $deliverables AS deliverable
            MERGE (d:Deliverable {name: deliverable})
            """
            session.run(query, deliverables=list(deliverables))
    
    def batch_create_profile_skill_relationships(self, relationships: List[Dict]) -> None:
        """Create multiple profile-skill relationships in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $relationships AS rel
            MATCH (p:Profile {title: rel.profile_title})
            MATCH (s:Skill {name: rel.skill})
            CREATE (p)-[:HAS_SKILL]->(s)
            """
            session.run(query, relationships=relationships)
    
    def batch_create_profile_knowledge_relationships(self, relationships: List[Dict]) -> None:
        """Create multiple profile-knowledge relationships in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $relationships AS rel
            MATCH (p:Profile {title: rel.profile_title})
            MATCH (k:Knowledge {name: rel.knowledge})
            CREATE (p)-[:REQUIRES_KNOWLEDGE]->(k)
            """
            session.run(query, relationships=relationships)
    
    def batch_create_profile_deliverable_relationships(self, relationships: List[Dict]) -> None:
        """Create multiple profile-deliverable relationships in a single transaction."""
        with self.driver.session() as session:
            query = """
            UNWIND $relationships AS rel
            MATCH (p:Profile {title: rel.profile_title})
            MATCH (d:Deliverable {name: rel.deliverable})
            CREATE (p)-[:PRODUCES_DELIVERABLE]->(d)
            """
            session.run(query, relationships=relationships)
    
    def create_skill_similarity_relationships(self, threshold: int = 2) -> None:
        """Create relationships between profiles that share skills."""
        with self.driver.session() as session:
            query = """
            MATCH (p1:Profile)-[:HAS_SKILL]->(s:Skill)<-[:HAS_SKILL]-(p2:Profile)
            WHERE id(p1) < id(p2)
            WITH p1, p2, COUNT(s) AS shared_skills
            WHERE shared_skills >= $threshold
            CREATE (p1)-[:SHARES_SKILLS_WITH {count: shared_skills}]->(p2)
            """
            session.run(query, threshold=threshold)
    
    def create_knowledge_similarity_relationships(self, threshold: int = 2) -> None:
        """Create relationships between profiles that share knowledge areas."""
        with self.driver.session() as session:
            query = """
            MATCH (p1:Profile)-[:REQUIRES_KNOWLEDGE]->(k:Knowledge)<-[:REQUIRES_KNOWLEDGE]-(p2:Profile)
            WHERE id(p1) < id(p2)
            WITH p1, p2, COUNT(k) AS shared_knowledge
            WHERE shared_knowledge >= $threshold
            CREATE (p1)-[:SHARES_KNOWLEDGE_WITH {count: shared_knowledge}]->(p2)
            """
            session.run(query, threshold=threshold)


def parse_multiline_field(field_value: str) -> List[str]:
    """Parse a field that contains multiple items separated by newlines or bullets."""
    if pd.isna(field_value) or not field_value:
        return []
    
    field_str = str(field_value)
    
    # Split by newline
    items = field_str.split('\n')
    
    parsed_items = []
    for item in items:
        # Clean the item
        item = item.strip()
        
        # Remove bullet points and leading/trailing whitespace
        item = item.lstrip('•').lstrip('-').lstrip('*').lstrip('○').strip()
        
        # Skip empty items
        if not item:
            continue
        
        parsed_items.append(item)
    
    return parsed_items


def load_enisa_dataset(filepath: str) -> pd.DataFrame:
    """Load the ENISA skill set dataset."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"ENISA dataset file not found: {filepath}")
    
    df = pd.read_csv(filepath, encoding="utf-8")
    print(f"Loaded ENISA dataset: {df.shape}", file=sys.stderr)
    print(f"Columns: {list(df.columns)}", file=sys.stderr)
    
    return df


def main():
    parser = argparse.ArgumentParser(description="Create Neo4j graph from ENISA cybersecurity skill set.")
    parser.add_argument("--dataset", "-d", type=str, required=True,
                       help="Path to ENISA skill set .csv file.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687",
                       help="Neo4j database URI.")
    parser.add_argument("--user", "-u", type=str, default="neo4j",
                       help="Neo4j username.")
    parser.add_argument("--password", "-p", type=str, required=True,
                       help="Neo4j password.")
    parser.add_argument("--clear", action="store_true",
                       help="Clear existing data before import.")
    parser.add_argument("--batch-size", type=int, default=100,
                       help="Batch size for database operations.")
    parser.add_argument("--skill-similarity-threshold", type=int, default=2,
                       help="Minimum number of shared skills to create similarity relationship.")
    parser.add_argument("--knowledge-similarity-threshold", type=int, default=2,
                       help="Minimum number of shared knowledge areas to create similarity relationship.")
    
    args = parser.parse_args()
    
    # Load data
    print("Loading ENISA dataset...", file=sys.stderr)
    df = load_enisa_dataset(args.dataset)
    
    # Initialize Neo4j connection
    print(f"Connecting to Neo4j at {args.uri}...", file=sys.stderr)
    graph_builder = EnisaGraphBuilder(args.uri, args.user, args.password)
    
    try:
        # Clear database if requested
        if args.clear:
            graph_builder.clear_database()
        
        # Create constraints and indexes
        print("Creating constraints and indexes...", file=sys.stderr)
        graph_builder.create_constraints()
        
        # Prepare profile nodes
        print("Preparing profile nodes...", file=sys.stderr)
        profiles = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Preparing profiles"):
            profile_data = {
                'profile_no': int(row['no']),
                'title': str(row['profile_title']),
                'mission': str(row.get('mission', '')),
                'main_tasks': str(row.get('main_tasks', ''))
            }
            profiles.append(profile_data)
        
        # Create profile nodes
        print("Creating profile nodes...", file=sys.stderr)
        for i in tqdm(range(0, len(profiles), args.batch_size), desc="Creating profiles"):
            batch = profiles[i:i + args.batch_size]
            graph_builder.batch_create_profiles(batch)
        
        # Extract and create skill nodes
        print("Extracting skills...", file=sys.stderr)
        all_skills: Set[str] = set()
        profile_skills = []
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing skills"):
            profile_title = str(row['profile_title'])
            skills = parse_multiline_field(row.get('key_skills', ''))
            
            for skill in skills:
                all_skills.add(skill)
                profile_skills.append({
                    'profile_title': profile_title,
                    'skill': skill
                })
        
        print(f"Found {len(all_skills)} unique skills.", file=sys.stderr)
        
        # Create skill nodes in batches
        print("Creating skill nodes...", file=sys.stderr)
        skills_list = list(all_skills)
        for i in tqdm(range(0, len(skills_list), args.batch_size), desc="Creating skills"):
            batch = skills_list[i:i + args.batch_size]
            graph_builder.batch_create_skills(batch)
        
        # Create profile-skill relationships in batches
        print("Creating profile-skill relationships...", file=sys.stderr)
        for i in tqdm(range(0, len(profile_skills), args.batch_size), desc="Creating skill relationships"):
            batch = profile_skills[i:i + args.batch_size]
            graph_builder.batch_create_profile_skill_relationships(batch)
        
        # Extract and create knowledge nodes
        print("Extracting knowledge areas...", file=sys.stderr)
        all_knowledge: Set[str] = set()
        profile_knowledge = []
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing knowledge"):
            profile_title = str(row['profile_title'])
            knowledge_items = parse_multiline_field(row.get('key_knowledge', ''))
            
            for knowledge in knowledge_items:
                all_knowledge.add(knowledge)
                profile_knowledge.append({
                    'profile_title': profile_title,
                    'knowledge': knowledge
                })
        
        print(f"Found {len(all_knowledge)} unique knowledge areas.", file=sys.stderr)
        
        # Create knowledge nodes in batches
        print("Creating knowledge nodes...", file=sys.stderr)
        knowledge_list = list(all_knowledge)
        for i in tqdm(range(0, len(knowledge_list), args.batch_size), desc="Creating knowledge"):
            batch = knowledge_list[i:i + args.batch_size]
            graph_builder.batch_create_knowledge(batch)
        
        # Create profile-knowledge relationships in batches
        print("Creating profile-knowledge relationships...", file=sys.stderr)
        for i in tqdm(range(0, len(profile_knowledge), args.batch_size), desc="Creating knowledge relationships"):
            batch = profile_knowledge[i:i + args.batch_size]
            graph_builder.batch_create_profile_knowledge_relationships(batch)
        
        # Extract and create deliverable nodes
        print("Extracting deliverables...", file=sys.stderr)
        all_deliverables: Set[str] = set()
        profile_deliverables = []
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing deliverables"):
            profile_title = str(row['profile_title'])
            deliverables = parse_multiline_field(row.get('deliverables', ''))
            
            for deliverable in deliverables:
                all_deliverables.add(deliverable)
                profile_deliverables.append({
                    'profile_title': profile_title,
                    'deliverable': deliverable
                })
        
        print(f"Found {len(all_deliverables)} unique deliverables.", file=sys.stderr)
        
        # Create deliverable nodes in batches
        print("Creating deliverable nodes...", file=sys.stderr)
        deliverables_list = list(all_deliverables)
        for i in tqdm(range(0, len(deliverables_list), args.batch_size), desc="Creating deliverables"):
            batch = deliverables_list[i:i + args.batch_size]
            graph_builder.batch_create_deliverables(batch)
        
        # Create profile-deliverable relationships in batches
        print("Creating profile-deliverable relationships...", file=sys.stderr)
        for i in tqdm(range(0, len(profile_deliverables), args.batch_size), desc="Creating deliverable relationships"):
            batch = profile_deliverables[i:i + args.batch_size]
            graph_builder.batch_create_profile_deliverable_relationships(batch)
        
        # Create similarity relationships
        print("Creating skill similarity relationships...", file=sys.stderr)
        graph_builder.create_skill_similarity_relationships(args.skill_similarity_threshold)
        
        print("Creating knowledge similarity relationships...", file=sys.stderr)
        graph_builder.create_knowledge_similarity_relationships(args.knowledge_similarity_threshold)
        
        print("\nGraph creation complete!", file=sys.stderr)
        print(f"- Created {len(profiles)} profile nodes", file=sys.stderr)
        print(f"- Created {len(all_skills)} skill nodes", file=sys.stderr)
        print(f"- Created {len(all_knowledge)} knowledge nodes", file=sys.stderr)
        print(f"- Created {len(all_deliverables)} deliverable nodes", file=sys.stderr)
        print(f"- Created {len(profile_skills)} profile-skill relationships", file=sys.stderr)
        print(f"- Created {len(profile_knowledge)} profile-knowledge relationships", file=sys.stderr)
        print(f"- Created {len(profile_deliverables)} profile-deliverable relationships", file=sys.stderr)
        
    finally:
        graph_builder.close()


if __name__ == "__main__":
    main()
