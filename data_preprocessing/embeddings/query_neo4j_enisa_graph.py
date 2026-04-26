"""
Query the ENISA Cybersecurity Profiles Neo4j graph.

This script provides common queries for exploring the ENISA skill set graph.
"""

import argparse
import sys
from typing import List, Dict, Any

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("WARNING: neo4j package not installed. Install with: pip install neo4j", file=sys.stderr)


class EnisaGraphQuerier:
    """Query the ENISA cybersecurity profiles Neo4j graph."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j package is required. Install with: pip install neo4j")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
    
    def get_all_profiles(self) -> List[Dict[str, Any]]:
        """Get all cybersecurity profiles."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile)
                RETURN p.profile_no AS no, p.title AS title, p.mission AS mission
                ORDER BY p.profile_no
            """)
            return [dict(record) for record in result]
    
    def get_profile_details(self, profile_title: str) -> Dict[str, Any]:
        """Get detailed information about a specific profile."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile {title: $title})
                OPTIONAL MATCH (p)-[:HAS_SKILL]->(s:Skill)
                OPTIONAL MATCH (p)-[:REQUIRES_KNOWLEDGE]->(k:Knowledge)
                OPTIONAL MATCH (p)-[:PRODUCES_DELIVERABLE]->(d:Deliverable)
                RETURN 
                    p.profile_no AS no,
                    p.title AS title,
                    p.mission AS mission,
                    p.main_tasks AS main_tasks,
                    COLLECT(DISTINCT s.name) AS skills,
                    COLLECT(DISTINCT k.name) AS knowledge,
                    COLLECT(DISTINCT d.name) AS deliverables
            """, title=profile_title)
            
            record = result.single()
            if record:
                return dict(record)
            return None
    
    def get_most_common_skills(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most commonly required skills across profiles."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Skill)<-[:HAS_SKILL]-(p:Profile)
                RETURN s.name AS skill, COUNT(p) AS profile_count
                ORDER BY profile_count DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]
    
    def get_most_common_knowledge(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most commonly required knowledge areas across profiles."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (k:Knowledge)<-[:REQUIRES_KNOWLEDGE]-(p:Profile)
                RETURN k.name AS knowledge, COUNT(p) AS profile_count
                ORDER BY profile_count DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]
    
    def get_profiles_sharing_skills(self, min_shared: int = 2, limit: int = 20) -> List[Dict[str, Any]]:
        """Get profiles that share skills."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p1:Profile)-[r:SHARES_SKILLS_WITH]->(p2:Profile)
                WHERE r.count >= $min_shared
                RETURN p1.title AS profile1, p2.title AS profile2, r.count AS shared_skills
                ORDER BY r.count DESC
                LIMIT $limit
            """, min_shared=min_shared, limit=limit)
            return [dict(record) for record in result]
    
    def get_profiles_sharing_knowledge(self, min_shared: int = 2, limit: int = 20) -> List[Dict[str, Any]]:
        """Get profiles that share knowledge areas."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p1:Profile)-[r:SHARES_KNOWLEDGE_WITH]->(p2:Profile)
                WHERE r.count >= $min_shared
                RETURN p1.title AS profile1, p2.title AS profile2, r.count AS shared_knowledge
                ORDER BY r.count DESC
                LIMIT $limit
            """, min_shared=min_shared, limit=limit)
            return [dict(record) for record in result]
    
    def find_profiles_by_skill(self, skill_keyword: str) -> List[Dict[str, Any]]:
        """Find profiles that require a specific skill (case-insensitive search)."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile)-[:HAS_SKILL]->(s:Skill)
                WHERE toLower(s.name) CONTAINS toLower($keyword)
                RETURN p.title AS profile, s.name AS skill
                ORDER BY p.profile_no
            """, keyword=skill_keyword)
            return [dict(record) for record in result]
    
    def find_profiles_by_knowledge(self, knowledge_keyword: str) -> List[Dict[str, Any]]:
        """Find profiles that require specific knowledge (case-insensitive search)."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile)-[:REQUIRES_KNOWLEDGE]->(k:Knowledge)
                WHERE toLower(k.name) CONTAINS toLower($keyword)
                RETURN p.title AS profile, k.name AS knowledge
                ORDER BY p.profile_no
            """, keyword=knowledge_keyword)
            return [dict(record) for record in result]
    
    def get_skill_gap(self, current_profile: str, target_profile: str) -> List[Dict[str, Any]]:
        """Get skills required by target profile but not in current profile."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (target:Profile {title: $target_title})-[:HAS_SKILL]->(s:Skill)
                WHERE NOT EXISTS {
                    MATCH (current:Profile {title: $current_title})-[:HAS_SKILL]->(s)
                }
                RETURN s.name AS skill_gap
                ORDER BY s.name
            """, current_title=current_profile, target_title=target_profile)
            return [dict(record) for record in result]
    
    def get_knowledge_gap(self, current_profile: str, target_profile: str) -> List[Dict[str, Any]]:
        """Get knowledge required by target profile but not in current profile."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (target:Profile {title: $target_title})-[:REQUIRES_KNOWLEDGE]->(k:Knowledge)
                WHERE NOT EXISTS {
                    MATCH (current:Profile {title: $current_title})-[:REQUIRES_KNOWLEDGE]->(k)
                }
                RETURN k.name AS knowledge_gap
                ORDER BY k.name
            """, current_title=current_profile, target_title=target_profile)
            return [dict(record) for record in result]
    
    def get_career_paths(self, start_profile: str, max_hops: int = 2) -> List[Dict[str, Any]]:
        """Find potential career progression paths from a starting profile."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH path = (start:Profile {title: $start_title})-[:SHARES_SKILLS_WITH*1..$max_hops]-(related:Profile)
                WHERE start <> related
                RETURN 
                    start.title AS from_profile,
                    related.title AS to_profile,
                    length(path) AS steps,
                    [rel in relationships(path) | rel.count] AS shared_skills_counts
                ORDER BY steps, shared_skills_counts DESC
                LIMIT 20
            """, start_title=start_profile, max_hops=max_hops)
            return [dict(record) for record in result]
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """Get overall graph statistics."""
        with self.driver.session() as session:
            # Count nodes by type
            node_counts = session.run("""
                MATCH (n)
                RETURN labels(n)[0] AS node_type, COUNT(n) AS count
                ORDER BY count DESC
            """)
            
            # Count relationships by type
            rel_counts = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) AS relationship_type, COUNT(r) AS count
                ORDER BY count DESC
            """)
            
            return {
                'nodes': [dict(record) for record in node_counts],
                'relationships': [dict(record) for record in rel_counts]
            }


def print_results(results: Any, title: str = None):
    """Pretty print query results."""
    if title:
        print(f"\n{'=' * 80}")
        print(f"{title}")
        print('=' * 80)
    
    if isinstance(results, list):
        if not results:
            print("No results found.")
            return
        
        for i, result in enumerate(results, 1):
            print(f"\n{i}. ", end="")
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, list):
                        print(f"\n   {key}:")
                        for item in value:
                            if item:  # Skip empty items
                                print(f"     - {item}")
                    else:
                        print(f"\n   {key}: {value}", end="")
            else:
                print(result)
    elif isinstance(results, dict):
        for key, value in results.items():
            print(f"\n{key}:")
            if isinstance(value, list):
                for item in value:
                    print(f"  {item}")
            else:
                print(f"  {value}")
    else:
        print(results)
    
    print()


def main():
    parser = argparse.ArgumentParser(description="Query ENISA cybersecurity profiles Neo4j graph.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687",
                       help="Neo4j database URI.")
    parser.add_argument("--user", "-u", type=str, default="neo4j",
                       help="Neo4j username.")
    parser.add_argument("--password", "-p", type=str, required=True,
                       help="Neo4j password.")
    
    # Query options
    parser.add_argument("--list-profiles", action="store_true",
                       help="List all cybersecurity profiles.")
    parser.add_argument("--profile", type=str,
                       help="Get details for a specific profile.")
    parser.add_argument("--top-skills", type=int, metavar="N",
                       help="Get top N most common skills.")
    parser.add_argument("--top-knowledge", type=int, metavar="N",
                       help="Get top N most common knowledge areas.")
    parser.add_argument("--shared-skills", type=int, metavar="MIN",
                       help="Get profiles sharing at least MIN skills.")
    parser.add_argument("--shared-knowledge", type=int, metavar="MIN",
                       help="Get profiles sharing at least MIN knowledge areas.")
    parser.add_argument("--find-skill", type=str, metavar="KEYWORD",
                       help="Find profiles requiring a specific skill.")
    parser.add_argument("--find-knowledge", type=str, metavar="KEYWORD",
                       help="Find profiles requiring specific knowledge.")
    parser.add_argument("--skill-gap", nargs=2, metavar=("CURRENT", "TARGET"),
                       help="Get skill gap between current and target profile.")
    parser.add_argument("--knowledge-gap", nargs=2, metavar=("CURRENT", "TARGET"),
                       help="Get knowledge gap between current and target profile.")
    parser.add_argument("--career-paths", type=str, metavar="PROFILE",
                       help="Find career progression paths from a profile.")
    parser.add_argument("--statistics", action="store_true",
                       help="Get graph statistics.")
    
    args = parser.parse_args()
    
    # Check if at least one query is specified
    if not any([
        args.list_profiles, args.profile, args.top_skills, args.top_knowledge,
        args.shared_skills, args.shared_knowledge, args.find_skill, args.find_knowledge,
        args.skill_gap, args.knowledge_gap, args.career_paths, args.statistics
    ]):
        parser.print_help()
        print("\nError: Please specify at least one query option.", file=sys.stderr)
        sys.exit(1)
    
    # Initialize querier
    print(f"Connecting to Neo4j at {args.uri}...", file=sys.stderr)
    querier = EnisaGraphQuerier(args.uri, args.user, args.password)
    
    try:
        if args.list_profiles:
            results = querier.get_all_profiles()
            print_results(results, "All Cybersecurity Profiles")
        
        if args.profile:
            results = querier.get_profile_details(args.profile)
            if results:
                print_results([results], f"Profile Details: {args.profile}")
            else:
                print(f"Profile not found: {args.profile}", file=sys.stderr)
        
        if args.top_skills:
            results = querier.get_most_common_skills(args.top_skills)
            print_results(results, f"Top {args.top_skills} Most Common Skills")
        
        if args.top_knowledge:
            results = querier.get_most_common_knowledge(args.top_knowledge)
            print_results(results, f"Top {args.top_knowledge} Most Common Knowledge Areas")
        
        if args.shared_skills:
            results = querier.get_profiles_sharing_skills(args.shared_skills)
            print_results(results, f"Profiles Sharing At Least {args.shared_skills} Skills")
        
        if args.shared_knowledge:
            results = querier.get_profiles_sharing_knowledge(args.shared_knowledge)
            print_results(results, f"Profiles Sharing At Least {args.shared_knowledge} Knowledge Areas")
        
        if args.find_skill:
            results = querier.find_profiles_by_skill(args.find_skill)
            print_results(results, f"Profiles Requiring Skill: {args.find_skill}")
        
        if args.find_knowledge:
            results = querier.find_profiles_by_knowledge(args.find_knowledge)
            print_results(results, f"Profiles Requiring Knowledge: {args.find_knowledge}")
        
        if args.skill_gap:
            current, target = args.skill_gap
            results = querier.get_skill_gap(current, target)
            print_results(results, f"Skill Gap: {current} → {target}")
        
        if args.knowledge_gap:
            current, target = args.knowledge_gap
            results = querier.get_knowledge_gap(current, target)
            print_results(results, f"Knowledge Gap: {current} → {target}")
        
        if args.career_paths:
            results = querier.get_career_paths(args.career_paths)
            print_results(results, f"Career Paths from: {args.career_paths}")
        
        if args.statistics:
            results = querier.get_graph_statistics()
            print_results(results, "Graph Statistics")
    
    finally:
        querier.close()


if __name__ == "__main__":
    main()
