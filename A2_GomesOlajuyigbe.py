from dotenv import load_dotenv
import os
import logging
from scripts import connector_API, preprocess_API_data, synthetic_data, extract_data, load_data

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

    logger.info("--------------------- PREPROCESSING AND TRANSFORMING DATA FROM SEMANTIC SCHOLAR ---------------------")
    preprocess_API_data.preprocess_data()

    logger.info("--------------------- GENERATING SYNTHETIC DATA FOR NEEDED FIELDS ---------------------")
    synthetic_data.synthetic_data()

    logger.info("--------------------- EXTRACTING DATA TO NEO4J FORMAT ---------------------")
    extract_data.extract_data()

    logger.info("--------------------- CONNECTING AND LOADING DATA TO NEO4J ---------------------")
    load_data.connect_load_neo4j(uri,user,password)