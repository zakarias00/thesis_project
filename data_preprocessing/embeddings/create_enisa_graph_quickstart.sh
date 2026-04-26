#!/bin/bash

# Quickstart script to create ENISA cybersecurity profiles Neo4j graph
# Usage: ./create_enisa_graph_quickstart.sh [password]

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}ENISA Cybersecurity Profiles Neo4j Graph Creation${NC}"
echo "=================================================="

# Check if password is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Neo4j password required${NC}"
    echo "Usage: $0 <neo4j_password>"
    echo ""
    echo "Example:"
    echo "  $0 mypassword"
    exit 1
fi

NEO4J_PASSWORD="$1"
NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
DATASET="../enisa_skill_set.csv"

# Check if dataset exists
if [ ! -f "$DATASET" ]; then
    echo -e "${RED}Error: ENISA dataset not found at $DATASET${NC}"
    exit 1
fi

echo -e "${GREEN}Configuration:${NC}"
echo "  Neo4j URI: $NEO4J_URI"
echo "  Neo4j User: $NEO4J_USER"
echo "  Dataset: $DATASET"
echo ""

# Check if Neo4j is running
echo -e "${BLUE}Checking Neo4j connection...${NC}"
if ! command -v cypher-shell &> /dev/null; then
    echo -e "${RED}Warning: cypher-shell not found. Cannot verify Neo4j connection.${NC}"
    echo "Make sure Neo4j is running before proceeding."
else
    if cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" -a "$NEO4J_URI" "RETURN 1" &> /dev/null; then
        echo -e "${GREEN}✓ Neo4j is running and accessible${NC}"
    else
        echo -e "${RED}✗ Cannot connect to Neo4j${NC}"
        echo "Please make sure Neo4j is running and credentials are correct."
        exit 1
    fi
fi

# Run the graph creation script
echo ""
echo -e "${BLUE}Creating ENISA graph...${NC}"
python create_neo4j_enisa_graph.py \
    --dataset "$DATASET" \
    --uri "$NEO4J_URI" \
    --user "$NEO4J_USER" \
    --password "$NEO4J_PASSWORD" \
    --clear \
    --batch-size 100 \
    --skill-similarity-threshold 2 \
    --knowledge-similarity-threshold 2

echo ""
echo -e "${GREEN}✓ Graph creation complete!${NC}"
echo ""
echo "You can now query the graph using Neo4j Browser at http://localhost:7474"
echo ""
echo "Example queries:"
echo "  1. View all profiles:"
echo "     MATCH (p:Profile) RETURN p LIMIT 25"
echo ""
echo "  2. Find profiles that share skills:"
echo "     MATCH (p1:Profile)-[r:SHARES_SKILLS_WITH]->(p2:Profile)"
echo "     RETURN p1.title, p2.title, r.count ORDER BY r.count DESC"
echo ""
echo "  3. Find the most common skills:"
echo "     MATCH (s:Skill)<-[:HAS_SKILL]-(p:Profile)"
echo "     RETURN s.name, COUNT(p) AS profile_count"
echo "     ORDER BY profile_count DESC LIMIT 10"
echo ""
echo "  4. View a specific profile with all relationships:"
echo "     MATCH (p:Profile {title: 'Penetration Tester'})-[r]-(n)"
echo "     RETURN p, r, n"
