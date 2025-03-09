from dotenv import load_dotenv
import os
import logging

from scripts.connector_neo4j import ConnectorNeo4j
from scripts import connector_API

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
    api_key = os.getenv("API_KEY")
    search_fields = os.getenv("FIELDS").split(',')

    logger.info("--------------------- RETRIVING DATA FROM SEMANTIC SCHOLAR ---------------------")
    connector_API.retrive_data_API(api_key,search_fields)


    # logger.info("--------------------- CONNECTING TO NEO4J ---------------------")
    # connector = ConnectorNeo4j(uri, user, password)
    # connector.connect()

    # connector.close()