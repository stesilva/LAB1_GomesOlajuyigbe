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
        RETURN cw.name AS conferenceWorkshopName, collect(citedPaper.title + ' (' + citationCount + ')') [0..3] AS top3CitedPapers"""
    )     
    print('----------- Results for query 1 - Version 2 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)

# For each conference/workshop find its community: i.e., those authors that have published papers on that conference/workshop in, at least, 4 different editions.
def run_query2(session):
    result = session.run(
        """MATCH (cw:ConferenceWorkshop)<-[published:PRESENTED_IN]-(paper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH cw.name AS conferenceWorkshopName, author.name as author, COUNT(DISTINCT published.edition) as distinct_editions
        WHERE distinct_editions > 3
        RETURN conferenceWorkshopName, collect(author) as community"""
    )     
    
    print('----------- Results for query 2 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    


#Find the impact factor of the journals in your graph
#Impact factor (year1) = Citations(year1) / Publications(year1-1) + Publications(year1-2)
#For this it was considered that to count towards a citation, the citedPaper has to be published 1 or 2 year before the paper that made the citation
#For example, if a paper published in 2025 cites another one from 2024, this citation will count towards the IF 2025 of the cited paper.
#If it cites a paper from 2006 this citation will not count towards the IF 2007 of the cited paper.

def run_query3(session):
    result = session.run(
        """MATCH (p:Paper)-[:CITES]->(citedPaper:Paper)-[pi:PUBLISHED_IN]->(j:Journal)
        MATCH (p)-[citingPub:PUBLISHED_IN|PRESENTED_IN]->()
        WITH j.journalID AS journalID,j.name AS journalName,citingPub.year AS year,pi.year AS publicationYear
        WHERE publicationYear IN [year - 1, year - 2]
        WITH journalID,journalName, year, COUNT(*) AS journalCitations

        MATCH (p:Paper)-[pi:PUBLISHED_IN]->(j:Journal {journalID: journalID})
        WHERE pi.year IN [year - 1, year - 2]
        WITH journalID,journalName,year,journalCitations, COUNT(p) AS journalPublications
        WHERE journalPublications > 0

        RETURN journalID,journalName,year,(journalCitations/journalPublications) AS impactFactor
        ORDER BY year DESC, impactFactor DESC, journalName ASC """
    )     
    
    print('----------- Results for query 3 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

# Find the h-index of the authors in your graph.
def run_query4(session):
    result = session.run(
        """MATCH (paper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH author.name as authorName, paper.citationCount as citation
        ORDER BY authorName, citation DESC
        WITH authorName, collect(citation) as citations
        WITH authorName, citations, range(0, size(citations)-1) as indices
        WITH authorName, max([i in indices WHERE i < size(citations) AND citations[i] >= i+1 | i+1]) as hindex
        RETURN authorName, size(hindex) as hindex
        ORDER BY hindex DESC"""
    )     
    
    print('----------- Results for query 4 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

# Find the h-index of the authors in your graph.
def run_query4_v2(session):
    result = session.run(
        """MATCH (paper:Paper) -[:CITES]-> (citedpaper:Paper)
        MATCH (citedpaper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH author.name as authorName, citedpaper.paperDOI as paperdoi, COUNT(paper) as citation
        ORDER BY authorName, citation DESC
        WITH authorName, collect(citation) as citations
        WITH authorName, citations,
            range(0, size(citations)-1) as indices
        WITH authorName, 
            max([i in indices WHERE i < size(citations) AND citations[i] >= i+1 | i+1]) as hindex
        RETURN authorName, size(hindex) as hindex
        ORDER BY hindex DESC"""
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
    session.execute_read(run_query3)
    #session.execute_read(run_query4)


    print('----------- All queries finished -----------')

    connector.close()