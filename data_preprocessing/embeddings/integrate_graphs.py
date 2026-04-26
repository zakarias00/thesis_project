"""
Integration script to combine ENISA profiles graph with course embeddings graph.

This script creates relationships between:
- ENISA cybersecurity profiles and relevant courses
- Skills required by profiles and courses that teach those skills
- Career pathways with recommended course sequences

Prerequisites:
- Both ENISA graph and course embeddings graph must be created in the same Neo4j database
- Run create_neo4j_enisa_graph.py first
- Run create_neo4j_graph.py second (or vice versa)
"""

import argparse
import sys
from typing import List, Dict

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    print("WARNING: neo4j package not installed. Install with: pip install neo4j", file=sys.stderr)


class GraphIntegrator:
    """Integrate ENISA profiles graph with course embeddings graph."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        if not NEO4J_AVAILABLE:
            raise ImportError("neo4j package is required. Install with: pip install neo4j")
        
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
    
    def check_graphs_exist(self) -> Dict[str, bool]:
        """Check if both graphs exist in the database."""
        with self.driver.session() as session:
            profile_count = session.run("MATCH (p:Profile) RETURN count(p) AS count").single()["count"]
            course_count = session.run("MATCH (c:Course) RETURN count(c) AS count").single()["count"]
            
            return {
                'enisa_graph': profile_count > 0,
                'course_graph': course_count > 0,
                'profile_count': profile_count,
                'course_count': course_count
            }
    
    def create_profile_course_relationships_by_skill(self, similarity_threshold: float = 0.3) -> int:
        """
        Create relationships between profiles and courses based on skill matching.
        
        Matches course titles/descriptions containing skill keywords.
        Returns number of relationships created.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile)-[:HAS_SKILL]->(s:Skill)
                MATCH (c:Course)
                WHERE toLower(c.title) CONTAINS toLower(s.name) 
                   OR toLower(c.description) CONTAINS toLower(s.name)
                WITH p, c, COUNT(DISTINCT s) AS matching_skills
                WHERE matching_skills > 0
                MERGE (p)-[r:RELEVANT_COURSE {matching_skills: matching_skills}]->(c)
                RETURN COUNT(r) AS relationships_created
            """)
            count = result.single()["relationships_created"]
            return count
    
    def create_profile_course_relationships_by_knowledge(self) -> int:
        """
        Create relationships between profiles and courses based on knowledge area matching.
        
        Returns number of relationships created.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Profile)-[:REQUIRES_KNOWLEDGE]->(k:Knowledge)
                MATCH (c:Course)
                WHERE toLower(c.title) CONTAINS toLower(k.name) 
                   OR toLower(c.description) CONTAINS toLower(k.name)
                WITH p, c, COUNT(DISTINCT k) AS matching_knowledge
                WHERE matching_knowledge > 0
                MERGE (p)-[r:TEACHES_KNOWLEDGE {matching_knowledge: matching_knowledge}]->(c)
                RETURN COUNT(r) AS relationships_created
            """)
            count = result.single()["relationships_created"]
            return count
    
    def create_skill_course_relationships(self) -> int:
        """
        Create direct relationships between Skills and Courses.
        
        Returns number of relationships created.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Skill)
                MATCH (c:Course)
                WHERE toLower(c.title) CONTAINS toLower(s.name) 
                   OR toLower(c.description) CONTAINS toLower(s.name)
                MERGE (s)-[r:TAUGHT_IN]->(c)
                RETURN COUNT(r) AS relationships_created
            """)
            count = result.single()["relationships_created"]
            return count
    
    def create_knowledge_course_relationships(self) -> int:
        """
        Create direct relationships between Knowledge areas and Courses.
        
        Returns number of relationships created.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (k:Knowledge)
                MATCH (c:Course)
                WHERE toLower(c.title) CONTAINS toLower(k.name) 
                   OR toLower(c.description) CONTAINS toLower(k.name)
                MERGE (k)-[r:COVERED_IN]->(c)
                RETURN COUNT(r) AS relationships_created
            """)
            count = result.single()["relationships_created"]
            return count
    
    def create_career_pathway_with_courses(self, from_profile: str, to_profile: str) -> Dict:
        """
        Create a recommended course pathway for career transition.
        
        Returns information about the pathway and recommended courses.
        """
        with self.driver.session() as session:
            # Find skill gap
            skill_gap = session.run("""
                MATCH (target:Profile {title: $to_profile})-[:HAS_SKILL]->(s:Skill)
                WHERE NOT EXISTS {
                    MATCH (current:Profile {title: $from_profile})-[:HAS_SKILL]->(s)
                }
                RETURN COLLECT(s.name) AS missing_skills
            """, from_profile=from_profile, to_profile=to_profile).single()
            
            # Find courses covering those skills
            courses = session.run("""
                MATCH (target:Profile {title: $to_profile})-[:HAS_SKILL]->(s:Skill)
                WHERE NOT EXISTS {
                    MATCH (current:Profile {title: $from_profile})-[:HAS_SKILL]->(s)
                }
                MATCH (s)-[:TAUGHT_IN]->(c:Course)
                RETURN DISTINCT c.title AS course_title, COLLECT(DISTINCT s.name) AS skills_covered
                LIMIT 20
            """, from_profile=from_profile, to_profile=to_profile)
            
            return {
                'from_profile': from_profile,
                'to_profile': to_profile,
                'missing_skills': skill_gap['missing_skills'],
                'recommended_courses': [dict(record) for record in courses]
            }
    
    def get_integration_statistics(self) -> Dict:
        """Get statistics about the integrated graph."""
        with self.driver.session() as session:
            stats = {}
            
            # Profile-Course relationships
            profile_course = session.run("""
                MATCH (p:Profile)-[r:RELEVANT_COURSE]->(c:Course)
                RETURN COUNT(r) AS count
            """).single()
            stats['profile_course_relationships'] = profile_course['count']
            
            # Skill-Course relationships
            skill_course = session.run("""
                MATCH (s:Skill)-[r:TAUGHT_IN]->(c:Course)
                RETURN COUNT(r) AS count
            """).single()
            stats['skill_course_relationships'] = skill_course['count']
            
            # Knowledge-Course relationships
            knowledge_course = session.run("""
                MATCH (k:Knowledge)-[r:COVERED_IN]->(c:Course)
                RETURN COUNT(r) AS count
            """).single()
            stats['knowledge_course_relationships'] = knowledge_course['count']
            
            return stats


def main():
    parser = argparse.ArgumentParser(
        description="Integrate ENISA profiles graph with course embeddings graph."
    )
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687",
                       help="Neo4j database URI.")
    parser.add_argument("--user", "-u", type=str, default="neo4j",
                       help="Neo4j username.")
    parser.add_argument("--password", "-p", type=str, required=True,
                       help="Neo4j password.")
    parser.add_argument("--skip-skill-matching", action="store_true",
                       help="Skip creating profile-course relationships by skill.")
    parser.add_argument("--skip-knowledge-matching", action="store_true",
                       help="Skip creating profile-course relationships by knowledge.")
    parser.add_argument("--career-pathway", nargs=2, metavar=("FROM", "TO"),
                       help="Generate career pathway with courses from one profile to another.")
    
    args = parser.parse_args()
    
    print(f"Connecting to Neo4j at {args.uri}...", file=sys.stderr)
    integrator = GraphIntegrator(args.uri, args.user, args.password)
    
    try:
        # Check if both graphs exist
        print("Checking for existing graphs...", file=sys.stderr)
        graph_status = integrator.check_graphs_exist()
        
        print(f"ENISA profiles graph: {'✓ Found' if graph_status['enisa_graph'] else '✗ Not found'} "
              f"({graph_status['profile_count']} profiles)", file=sys.stderr)
        print(f"Course embeddings graph: {'✓ Found' if graph_status['course_graph'] else '✗ Not found'} "
              f"({graph_status['course_count']} courses)", file=sys.stderr)
        
        if not graph_status['enisa_graph']:
            print("\nError: ENISA profiles graph not found. Run create_neo4j_enisa_graph.py first.",
                  file=sys.stderr)
            sys.exit(1)
        
        if not graph_status['course_graph']:
            print("\nError: Course embeddings graph not found. Run create_neo4j_graph.py first.",
                  file=sys.stderr)
            sys.exit(1)
        
        print("\nIntegrating graphs...", file=sys.stderr)
        
        # Create relationships
        if not args.skip_skill_matching:
            print("Creating profile-course relationships by skill...", file=sys.stderr)
            count = integrator.create_profile_course_relationships_by_skill()
            print(f"  Created {count} profile-course relationships", file=sys.stderr)
            
            print("Creating skill-course relationships...", file=sys.stderr)
            count = integrator.create_skill_course_relationships()
            print(f"  Created {count} skill-course relationships", file=sys.stderr)
        
        if not args.skip_knowledge_matching:
            print("Creating profile-course relationships by knowledge...", file=sys.stderr)
            count = integrator.create_profile_course_relationships_by_knowledge()
            print(f"  Created {count} profile-knowledge-course relationships", file=sys.stderr)
            
            print("Creating knowledge-course relationships...", file=sys.stderr)
            count = integrator.create_knowledge_course_relationships()
            print(f"  Created {count} knowledge-course relationships", file=sys.stderr)
        
        # Generate career pathway if requested
        if args.career_pathway:
            from_profile, to_profile = args.career_pathway
            print(f"\nGenerating career pathway: {from_profile} → {to_profile}", file=sys.stderr)
            pathway = integrator.create_career_pathway_with_courses(from_profile, to_profile)
            
            print(f"\nCareer Pathway Analysis")
            print("=" * 80)
            print(f"From: {pathway['from_profile']}")
            print(f"To: {pathway['to_profile']}")
            print(f"\nMissing Skills ({len(pathway['missing_skills'])} total):")
            for skill in pathway['missing_skills']:
                print(f"  - {skill}")
            
            print(f"\nRecommended Courses ({len(pathway['recommended_courses'])} total):")
            for i, course in enumerate(pathway['recommended_courses'], 1):
                print(f"\n{i}. {course['course_title']}")
                print(f"   Skills covered: {', '.join(course['skills_covered'])}")
        
        # Print statistics
        print("\nIntegration Statistics:", file=sys.stderr)
        stats = integrator.get_integration_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}", file=sys.stderr)
        
        print("\n✓ Integration complete!", file=sys.stderr)
        print("\nExample queries to try:", file=sys.stderr)
        print("  1. Find courses for a profile:")
        print("     MATCH (p:Profile {title: 'Penetration Tester'})-[:RELEVANT_COURSE]->(c:Course)")
        print("     RETURN c.title LIMIT 10")
        print("\n  2. Find courses teaching a specific skill:")
        print("     MATCH (s:Skill {name: 'Conduct ethical hacking'})-[:TAUGHT_IN]->(c:Course)")
        print("     RETURN c.title")
        print("\n  3. Career pathway with courses:")
        print("     Use --career-pathway option to generate detailed pathways")
        
    finally:
        integrator.close()


if __name__ == "__main__":
    main()
