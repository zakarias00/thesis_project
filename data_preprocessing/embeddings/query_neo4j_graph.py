"""
Example script for querying the Neo4j course graph.

This script demonstrates common queries you can perform on the course graph.
"""

from neo4j import GraphDatabase
import argparse
from typing import List, Dict


class CourseGraphQuery:
    """Helper class for querying the course knowledge graph."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize connection to Neo4j."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        """Close the database connection."""
        if self.driver:
            self.driver.close()
    
    def get_all_courses(self, limit: int = 10) -> List[Dict]:
        """Get all courses in the database."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Course)
                RETURN c.course_id as id, c.title as title
                ORDER BY c.course_id
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]
    
    def find_similar_courses(self, course_title: str, limit: int = 5) -> List[Dict]:
        """Find courses similar to a given course."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Course {title: $title})-[r:SIMILAR_TO]->(similar:Course)
                RETURN similar.title as title, 
                       similar.description as description,
                       r.similarity as similarity
                ORDER BY r.similarity DESC
                LIMIT $limit
            """, title=course_title, limit=limit)
            return [dict(record) for record in result]
    
    def find_courses_by_skill(self, skill_name: str) -> List[Dict]:
        """Find all courses that teach a specific skill."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Course)-[:HAS_SKILL]->(s:Skill)
                WHERE toLower(s.name) = toLower($skill)
                RETURN c.title as title, c.description as description
            """, skill=skill_name)
            return [dict(record) for record in result]
    
    def get_course_skills(self, course_title: str) -> List[str]:
        """Get all skills for a given course."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Course {title: $title})-[:HAS_SKILL]->(s:Skill)
                RETURN s.name as skill
                ORDER BY s.name
            """, title=course_title)
            return [record["skill"] for record in result]
    
    def find_courses_with_shared_skills(self, course_title: str, min_shared: int = 2) -> List[Dict]:
        """Find courses that share skills with a given course."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c1:Course {title: $title})-[:HAS_SKILL]->(s:Skill)<-[:HAS_SKILL]-(c2:Course)
                WHERE c1 <> c2
                WITH c2, collect(DISTINCT s.name) as shared_skills
                WHERE size(shared_skills) >= $min_shared
                RETURN c2.title as title, shared_skills, size(shared_skills) as count
                ORDER BY count DESC
            """, title=course_title, min_shared=min_shared)
            return [dict(record) for record in result]
    
    def get_most_common_skills(self, limit: int = 10) -> List[Dict]:
        """Get the most commonly taught skills."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (s:Skill)<-[:HAS_SKILL]-(c:Course)
                RETURN s.name as skill, count(c) as course_count
                ORDER BY course_count DESC
                LIMIT $limit
            """, limit=limit)
            return [dict(record) for record in result]
    
    def recommend_courses(self, course_title: str, limit: int = 5) -> List[Dict]:
        """Recommend courses based on similarity and shared skills."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (source:Course {title: $title})
                MATCH (source)-[sim:SIMILAR_TO]->(target:Course)
                OPTIONAL MATCH (source)-[:HAS_SKILL]->(skill:Skill)<-[:HAS_SKILL]-(target)
                WITH target, sim.similarity as similarity, count(DISTINCT skill) as shared_skills
                RETURN target.title as title,
                       target.description as description,
                       similarity,
                       shared_skills,
                       (similarity * 0.7 + (shared_skills * 0.1)) as score
                ORDER BY score DESC
                LIMIT $limit
            """, title=course_title, limit=limit)
            return [dict(record) for record in result]
    
    def find_learning_paths(self, start_course: str, max_depth: int = 3, limit: int = 5) -> List[Dict]:
        """Find learning paths starting from a course."""
        with self.driver.session() as session:
            result = session.run(f"""
                MATCH path = (start:Course {{title: $start}})-[:SIMILAR_TO*1..{max_depth}]->(end:Course)
                WHERE start <> end AND ALL(r in relationships(path) WHERE r.similarity > 0.5)
                WITH path, 
                     [node in nodes(path) | node.title] as course_titles,
                     reduce(sim = 1.0, rel in relationships(path) | sim * rel.similarity) as path_strength
                RETURN course_titles, length(path) as steps, path_strength
                ORDER BY path_strength DESC
                LIMIT $limit
            """, start=start_course, limit=limit)
            return [dict(record) for record in result]
    
    def get_database_stats(self) -> Dict:
        """Get statistics about the graph database."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Course)
                OPTIONAL MATCH (s:Skill)
                OPTIONAL MATCH ()-[r:HAS_SKILL]->()
                OPTIONAL MATCH ()-[sim:SIMILAR_TO]->()
                RETURN count(DISTINCT c) as course_count,
                       count(DISTINCT s) as skill_count,
                       count(DISTINCT r) as has_skill_count,
                       count(DISTINCT sim) as similarity_count
            """)
            return dict(result.single())


def main():
    parser = argparse.ArgumentParser(description="Query the Neo4j course knowledge graph.")
    parser.add_argument("--uri", type=str, default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--user", type=str, default="neo4j", help="Neo4j username")
    parser.add_argument("--password", "-p", type=str, required=True, help="Neo4j password")
    parser.add_argument("--course", type=str, help="Course title for queries")
    
    args = parser.parse_args()
    
    # Initialize query interface
    print("Connecting to Neo4j...\n")
    query = CourseGraphQuery(args.uri, args.user, args.password)
    
    try:
        # Get database statistics
        print("=" * 60)
        print("DATABASE STATISTICS")
        print("=" * 60)
        stats = query.get_database_stats()
        print(f"Total Courses: {stats['course_count']}")
        print(f"Total Skills: {stats['skill_count']}")
        print(f"Course-Skill Relationships: {stats['has_skill_count']}")
        print(f"Similarity Relationships: {stats['similarity_count']}")
        print()
        
        # Show most common skills
        print("=" * 60)
        print("TOP 10 MOST COMMON SKILLS")
        print("=" * 60)
        common_skills = query.get_most_common_skills(10)
        for idx, item in enumerate(common_skills, 1):
            print(f"{idx}. {item['skill']}: {item['course_count']} courses")
        print()
        
        # If a course is specified, show detailed information
        if args.course:
            course_title = args.course
            
            print("=" * 60)
            print(f"COURSE: {course_title}")
            print("=" * 60)
            
            # Get skills for this course
            print("\nSkills taught:")
            skills = query.get_course_skills(course_title)
            if skills:
                for skill in skills:
                    print(f"  - {skill}")
            else:
                print("  No skills found")
            
            # Find similar courses
            print("\nSimilar courses:")
            similar = query.find_similar_courses(course_title, 5)
            if similar:
                for idx, item in enumerate(similar, 1):
                    print(f"  {idx}. {item['title']} (similarity: {item['similarity']:.3f})")
            else:
                print("  No similar courses found")
            
            # Find courses with shared skills
            print("\nCourses with shared skills:")
            shared = query.find_courses_with_shared_skills(course_title, 2)
            if shared:
                for idx, item in enumerate(shared[:5], 1):
                    print(f"  {idx}. {item['title']} ({item['count']} shared skills)")
            else:
                print("  No courses with shared skills found")
            
            # Get recommendations
            print("\nRecommended courses:")
            recommendations = query.recommend_courses(course_title, 5)
            if recommendations:
                for idx, item in enumerate(recommendations, 1):
                    print(f"  {idx}. {item['title']}")
                    print(f"      Similarity: {item['similarity']:.3f}, Shared skills: {item['shared_skills']}")
            else:
                print("  No recommendations found")
            
            # Find learning paths
            print("\nPossible learning paths:")
            paths = query.find_learning_paths(course_title, max_depth=2, limit=3)
            if paths:
                for idx, item in enumerate(paths, 1):
                    print(f"  {idx}. {' → '.join(item['course_titles'])}")
                    print(f"      Steps: {item['steps']}, Strength: {item['path_strength']:.3f}")
            else:
                print("  No learning paths found")
        else:
            # Just show sample courses
            print("=" * 60)
            print("SAMPLE COURSES (use --course to explore specific course)")
            print("=" * 60)
            courses = query.get_all_courses(10)
            for idx, course in enumerate(courses, 1):
                print(f"{idx}. {course['title']}")
    
    finally:
        query.close()
        print("\nConnection closed.")


if __name__ == "__main__":
    main()
