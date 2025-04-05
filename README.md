# Property Graphs

## Overview
This repository contains a hands-on **lab on Property Graphs** using **Neo4j**. The lab covers **graph modeling, querying, data loading, evolution, and analysis using Cypher and graph algorithms.**

## Setup Instructions
1. Install **Neo4j** and set up a database instance (refer to official Neo4j documentation for installation guides). The username should be named 'neo4j' and the password should be '12345678'. If other username and password is chosen, the file .env should be modified. 
2. Install the needed libraries in Python: pip install -r requirements.txt
3. Locate the neo4j 'import' folder and copy the full path to the .env file on the variable 'IMPORT_PATH'

## Execution
### **Part A: Graph Modeling & Data Loading**
- **A.1 Modeling:** Design located in the folder assets.
- **A.2 Instantiating/Loading:** python A2_GomesOlajuyigbe.py [Make sure that the created files can be located by neo4j, otherwise the loading will not work. By default neo4j uses their own 'import' folder to store files so make sure to add this path on the .env file]
- **A.3 Graph Evolution:** python A3_GomesOlajuyigbe.py [Make sure that the created files can be located by neo4j, otherwise the loading will not work. By default neo4j uses their own 'import' folder to store files so make sure to add this path on the .env file]

### **Part B: Querying Graph Data**
python B_GomesOlajuyigbe.py

### **Part C: Reviewer Recommender System**
python C_GomesOlajuyigbe.py

### **Part D: Graph Algorithms**
python D_GomesOlajuyigbe.py

