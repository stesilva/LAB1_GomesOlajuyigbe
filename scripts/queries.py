from scripts.connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
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

#For each conference/workshop find its community: i.e., those authors that have published papers on that conference/workshop in, at least, 4 different editions.
def run_query2(session):
    result = session.run(
        """MATCH (cw:ConferenceWorkshop)<-[presented:PRESENTED_IN]-(paper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH cw.name AS conferenceWorkshopName, author, COUNT(DISTINCT presented.edition) as distinct_editions
        WHERE distinct_editions > 3
        RETURN conferenceWorkshopName, collect(author.name) as community"""
    )     
    
    print('----------- Results for query 2 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    


#Find the impact factor of the journals in your graph
#Impact factor (year1) = Citations(year1) / Publications(year1-1) + Publications(year1-2)
#For this it was considered that to count towards a citation, the citedPaper has to be published 1 or 2 year before the paper that made the citation
#For example, if a paper published in 2021 cites another one from 2020, this citation will count towards the IF 2021 of the cited paper.
#If it cites a paper from 2018 this citation will not count towards the IF 2018 of the cited paper.

#Fixed a year to better design the query and to better see the results
def run_query3(session):
    result = session.run(
        """MATCH (p:Paper)-[pi:PUBLISHED_IN]->(j:Journal)
        WHERE pi.year IN [2020, 2019]
        WITH j, collect(p.paperDOI) as papersIDs
        WITH j, papersIDs, size(papersIDs) as journalPublications
        
        UNWIND papersIDs as papersID
        MATCH (citingPaper:Paper)-[:CITES]->(citedPaper:Paper {paperDOI:papersID})
        MATCH (citingPaper)-[citingPub:PUBLISHED_IN|PRESENTED_IN]->()
        WHERE citingPub.year IN [2021]
        WITH j, journalPublications,COUNT(citingPaper.paperDOI) AS journalCitations

        RETURN j.journalID as journalID,j.name as journalName, 2021 as impactYear, journalCitations, journalPublications, (toFloat(journalCitations)/journalPublications) AS impactFactor
        ORDER BY impactFactor DESC, journalName ASC"""
    )     
    
    print('----------- Results for query 3 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

#Find the h-index of the authors in your graph.
def run_query4(session):
    result = session.run(
        """MATCH (paper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH author, paper.citationCount as citation
        ORDER BY author.authorID, citation DESC
        WITH author, collect(citation) as citations
        WITH author, citations, range(0, size(citations)-1) as indices
        WITH author, max([i in indices WHERE i < size(citations) AND citations[i] >= i+1 | i+1]) as hindex
        RETURN author.authorID as authorID, author.name as authorName, size(hindex) as hindex
        ORDER BY hindex DESC"""
    )     
    
    print('----------- Results for query 4 -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)    

#Find the h-index of the authors in your graph without the 'citationCount' property
def run_query4_v2(session):
    result = session.run(
        """MATCH (paper:Paper) -[:CITES]-> (citedpaper:Paper)
        MATCH (citedpaper:Paper)-[:AUTHORED_BY]->(author:Author)
        WITH author, citedpaper.paperDOI as paperdoi, COUNT(paper) as citation
        ORDER BY author.authorID, citation DESC
        WITH author, collect(citation) as citations
        WITH author, citations, range(0, size(citations)-1) as indices
        WITH author, max([i in indices WHERE i < size(citations) AND citations[i] >= i+1 | i+1]) as hindex
        RETURN author.authorID as authorID, author.name as authorName, size(hindex) as hindex
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
    session.execute_read(run_query2)
    session.execute_read(run_query3)
    session.execute_read(run_query4)


    print('----------- All queries finished -----------')

    connector.close()