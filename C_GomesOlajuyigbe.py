from dotenv import load_dotenv
import os
import logging
from scripts import recommender

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

    logger.info("--------------------- EXECUTION OF RECOMMENDER ---------------------")
    recommender.connect_run_neo4j(uri,user,password)