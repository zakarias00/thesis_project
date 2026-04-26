#!/bin/bash
# Quick start script for creating a Neo4j graph from course embeddings

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}===========================================================${NC}"
echo -e "${BLUE}   Neo4j Course Graph Creator - Quick Start${NC}"
echo -e "${BLUE}===========================================================${NC}"
echo ""

# Check if running from correct directory
if [ ! -f "create_embeddings.py" ]; then
    echo -e "${RED}Error: This script must be run from the embeddings directory${NC}"
?    echo -e "${YELLOW}cd data_preprocessing/embeddings${NC}"
    exit 1
fi

# Step 1: Check prerequisites
echo -e "${GREEN}Step 1: Checking prerequisites...${NC}"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 is not installed. Please install Python 3.8+${NC}"
    exit 1
fi
echo "  ✓ Python3 found"

# Check pip packages
echo "  Checking required packages..."
python3 -c "import pandas, numpy, sentence_transformers, neo4j, sklearn" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}  Some packages are missing. Installing...${NC}"
    pip install pandas numpy sentence-transformers neo4j scikit-learn tqdm
else
    echo "  ✓ All required packages installed"
fi

# Step 2: Check for dataset
echo ""
echo -e "${GREEN}Step 2: Checking for courses dataset...${NC}"

DATASET_PATH="../courses_dataset.csv"
if [ ! -f "$DATASET_PATH" ]; then
    # Try alternative location
    DATASET_PATH="../../eda/courses_dataset.csv"
    if [ ! -f "$DATASET_PATH" ]; then
        echo -e "${RED}Error: courses_dataset.csv not found${NC}"
        echo "Please ensure the dataset is in the correct location"
        exit 1
    fi
fi
echo "  ✓ Dataset found: $DATASET_PATH"

# Step 3: Create embeddings (if not already created)
echo ""
echo -e "${GREEN}Step 3: Checking for embeddings...${NC}"

if [ ! -f "course_embeddings_embeddings.npy" ] || [ ! -f "course_embeddings_metadata.csv" ]; then
    echo "  Embeddings not found. Creating embeddings..."
    echo ""
    python3 create_embeddings.py \
        --input "$DATASET_PATH" \
        --output-prefix course_embeddings \
        --mode row \
        --normalize \
        --batch-size 64
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to create embeddings${NC}"
        exit 1
    fi
    echo ""
    echo "  ✓ Embeddings created successfully"
else
    echo "  ✓ Embeddings already exist"
fi

# Step 4: Neo4j connection details
echo ""
echo -e "${GREEN}Step 4: Neo4j Configuration${NC}"
echo ""
echo "Please provide your Neo4j connection details:"
echo -e "${YELLOW}(Default: bolt://localhost:7687, user: neo4j)${NC}"
echo ""

# Read Neo4j URI
read -p "Neo4j URI [bolt://localhost:7687]: " NEO4J_URI
NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}

# Read Neo4j username
read -p "Neo4j username [neo4j]: " NEO4J_USER
NEO4J_USER=${NEO4J_USER:-neo4j}

# Read Neo4j password (hidden)
read -s -p "Neo4j password: " NEO4J_PASSWORD
echo ""

if [ -z "$NEO4J_PASSWORD" ]; then
    echo -e "${RED}Error: Password cannot be empty${NC}"
    exit 1
fi

# Read other parameters
echo ""
read -p "Clear existing data? [y/N]: " CLEAR_DB
read -p "Top K similar courses [5]: " TOP_K
TOP_K=${TOP_K:-5}
read -p "Similarity threshold [0.5]: " THRESHOLD
THRESHOLD=${THRESHOLD:-0.5}

# Step 5: Create Neo4j graph
echo ""
echo -e "${GREEN}Step 5: Creating Neo4j graph...${NC}"
echo ""

# Build command
CMD="python3 create_neo4j_graph.py \
    --embeddings course_embeddings_embeddings.npy \
    --metadata course_embeddings_metadata.csv \
    --dataset $DATASET_PATH \
    --uri $NEO4J_URI \
    --user $NEO4J_USER \
    --password $NEO4J_PASSWORD \
    --top-k $TOP_K \
    --similarity-threshold $THRESHOLD"

if [[ $CLEAR_DB =~ ^[Yy]$ ]]; then
    CMD="$CMD --clear"
fi

# Execute
eval $CMD

if [ $? -ne 0 ]; then
    echo ""
    echo -e "${RED}Error: Failed to create Neo4j graph${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "  1. Ensure Neo4j is running (check http://localhost:7474)"
    echo "  2. Verify credentials are correct"
    echo "  3. Check that port 7687 is accessible"
    exit 1
fi

echo ""
echo -e "${GREEN}===========================================================${NC}"
echo -e "${GREEN}   Graph creation complete!${NC}"
echo -e "${GREEN}===========================================================${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Open Neo4j Browser: http://localhost:7474"
echo "  2. Run example queries (see README_NEO4J.md)"
echo "  3. Query programmatically:"
echo -e "     ${YELLOW}python3 query_neo4j_graph.py --password YOUR_PASSWORD${NC}"
echo ""
echo -e "${BLUE}Example query in Neo4j Browser:${NC}"
echo -e "${YELLOW}MATCH (c:Course)-[r:SIMILAR_TO]->(similar:Course)"
echo "RETURN c.title, similar.title, r.similarity"
echo -e "ORDER BY r.similarity DESC LIMIT 10${NC}"
echo ""
