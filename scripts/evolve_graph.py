from scripts.connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

#Functions to create nodes and edges for each CSV file containing the generated data
#Created according to desing version 2 (assets/graphDesignVersion2.png)

def load_node_institution(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///institutions.csv" AS row
            CREATE (institution:Institution {
            institutionID: trim(row.institutionID), 
            name: trim(row.name), 
            type: trim(row.type)})"""
    )     
    print('Created node for institution.')

def load_edge_author_affiliated_institution(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///author_institution_relations.csv' AS row
            MATCH (author:Author {authorID: row.authorID})
            MATCH (institution:Institution {institutionID: row.institutionID})
            MERGE (author)-[c:AFFILIATED_TO]->(institution)"""
    )      
    print('Created edge for the relationship AFFILIATED_TO')

def update_edge_paper_reviewed_author(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM "file:///reviewer_paper_properties.csv" AS row
            MATCH (paper:Paper {paperDOI: row.paperDOI}), (author:Author {authorID: row.authorID})
            MATCH (paper)-[r:REVIEWED_BY]->(author)
            SET r.review = row.review, r.decision = row.suggested_decision"""
    )      
    print('Updated edge for the relationship REVIEWED_BY')

def connect_load_neo4j(uri,user,password):
    connector = ConnectorNeo4j(uri, user, password)
    connector.connect()
    session = connector.create_session()

    logger.info("Creating and loading nodes and edges ...")

    session.execute_write(load_node_institution)
    session.execute_write(load_edge_author_affiliated_institution)
    session.execute_write(update_edge_paper_reviewed_author)

    print('Creation and loading completed with successes.')

    connector.close()