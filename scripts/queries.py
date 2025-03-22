from scripts.connector_neo4j import ConnectorNeo4j
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

def print_results(records, summary):
    time = summary.result_available_after
    print(f"{len(records)} ROWS RETURNED IN {time} ms.")

#Find the top 3 most cited papers of each conference/workshop
#Use the property 'citationCounts' of node 'Paper' to simplify query
def run_query1(session):
    result = session.run(
        """MATCH (p:Paper)-[:PRESENTED_IN]->(cw:ConferenceWorkshop)
            WITH p,cw
            ORDER BY p.citationCount DESC
            RETURN cw.name AS conferenceWorkshopName, collect(p.title + ' (' + p.citationCount + ')') [0..3] AS top3CitedPaper"""
    )     
    
    print('----------- Results for query 1 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)

#Count the citation for each paper presented in a conference or workshop 
def run_query1_v2(session):
    result = session.run(
        """MATCH (p:Paper)-[c:CITES]->(citedPaper:Paper)-[:PRESENTED_IN]->(cw:ConferenceWorkshop)
        WITH cw, citedPaper, count(p.paperDOI) as citationCount
        ORDER BY citationCount DESC
        RETURN cw.name AS conferenceWorkshopName, collect(citedPaper.title + ' (' + citationCount + ')') [0..3] AS top3CitedPaper"""
    )     
    print('----------- Results for query 1 - Version 2 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)


def run_query2(session):
    result = session.run(
        """"""
    )     
    
    print('----------- Results for query 2 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

def run_query3(session):
    result = session.run(
        """"""
    )     
    
    print('----------- Results for query 3 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

def run_query4(session):
    result = session.run(
        """"""
    )     
    
    print('----------- Results for query 4 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    


def connect_run_neo4j(uri,user,password):
    connector = ConnectorNeo4j(uri, user, password)
    connector.connect()
    session = connector.create_session()
    
    logger.info("Running Queries ...")

    session.execute_read(run_query1)
    #session.execute_read(run_query1_v2)
    #session.execute_read(run_query2)
    #session.execute_read(run_query3)
    #session.execute_read(run_query4)


    print('----------- All queries finished -----------')

    connector.close()