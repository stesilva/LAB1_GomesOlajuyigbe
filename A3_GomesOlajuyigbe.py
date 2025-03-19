from dotenv import load_dotenv
import os
import logging
from scripts import evolve_data, evolve_graph

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

if __name__ == "__main__":
    #Load the needed credentials to connect to neo4j
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    logger.info("--------------------- GENERATING SYNTHETIC DATA FOR NEEDED FIELDS TO EVOLVE GRAPH ---------------------")
    evolve_data.synthetic_data()

    logger.info("--------------------- CONNECTING AND LOADING DATA TO NEO4J TO EVOLVE GRAPH ---------------------")
    evolve_graph.connect_load_neo4j(uri,user,password)