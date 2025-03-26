from scripts.connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

#Functions to create nodes and edges for each corresponding query
#Created according to desing version 3 (assets/graphDesignVersion3.png)

#Define a node for the 'Technology' community and then link the keywords to that given node
def run_query1(session):
    session.run(
        """MATCH (c:Community {name: 'Technology'})
            DETACH DELETE c"""
    )     

    session.run(
        """CREATE (c:Community {name: 'Technology'})"""
    )   

    print('----------- \'Technology Community\' node created! -------------')

    result = session.run(
        """MATCH (k:Keyword)
            WHERE k.keyword IN ['Machine Learning', 'Big Data', 'Natural Language Processing', \
            'Artificial Intelligence','Graph', 'Computer Science', 'Cloud', 'Data Mining', 'Computer Vision', 'Cybersecurity']
            MATCH (c:Community)
            MERGE (k)-[:DEFINES]->(c)"""
    )     
    
    print('----------- Relationship with keywords created! -------------')


#Process journals and conferences/workshops separately
#For each paper, see if it is present on the list of papers that have the keywords that defines a community. If yes, mark it as 1
# A journal is related to a community if 90% of its papers are market with 1
def run_query2(session):

    session.run(
        """MATCH ()-[rt:RELATED_TO]->()
            DETACH DELETE rt"""
    )  
        
    session.run(
        """MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)-[:DEFINES]->(c:Community {name:'Technology'})
            WITH c, collect(p.paperDOI) AS papersID
            MATCH (j:Journal)<-[:PUBLISHED_IN]-(p2:Paper)
            WITH j, c, CASE WHEN p2.paperDOI IN papersID THEN 1 ELSE 0 END AS journalPaperCommunity
            WITH j, c, (toFloat(sum(journalPaperCommunity)) / count(*)) AS percentangePapersCommunityJournal
            WHERE percentangePapersCommunityJournal >= 0.90
            MERGE (j)-[:RELATED_TO]->(c)""")

    session.run( """MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)-[:DEFINES]->(c:Community {name:'Technology'})
            WITH c, collect(p.paperDOI) AS papersID
            MATCH (cw:ConferenceWorkshop)<-[PRESENTED_IN]-(p3:Paper)
            WITH cw, c, CASE WHEN p3.paperDOI in papersID THEN 1 ELSE 0 END as cwPaperCommunity
            WITH cw, c, (toFloat(sum(cwPaperCommunity)) / count(*)) as percentangePapersCommunityConferenceWorkshop
            WHERE percentangePapersCommunityConferenceWorkshop >= 0.90
            MERGE (cw)-[:RELATED_TO]->(c)"""
    )     
    
    print('----------- Relationship between Journals/Conference/Workshops and Communities created!-------------') 


#Define the top 100 papers of the community. To be considered a top paper, that paper has to be cited by another that also belongs to the community.
#Papers with the most number of citations are selected
def run_query3(session):

    session.run(
        """MATCH ()-[top100:TOP_100]->()
            DETACH DELETE top100"""
    )  
        
    session.run(
        """MATCH (citingPaper:Paper)-[]->(:Journal|ConferenceWorkshop)-[r:RELATED_TO]->(c:Community {name: 'Technology'})
            MATCH (citedPaper:Paper)-[]->(:Journal|ConferenceWorkshop)-[r:RELATED_TO]->(c:Community {name: 'Technology'})
            MATCH (citingPaper)-[:CITES]->(citedPaper)
            WITH c, citedPaper, count(DISTINCT citingPaper) as citations
            ORDER BY citations DESC
            LIMIT 100
            MERGE (citedPaper)-[:TOP_100]->(c)"""
    )     
    
    print('----------- TOP 100 papers for each community defined! -------------')

#Define the author by simple checking the papers that are marked as top 100
def run_query4(session):
    session.run(
        """MATCH ()-[r:GURU|POSSIBLE_REVIEWER]->()
            DETACH DELETE r"""
    )  

    session.run(
        """MATCH (a:Author)<-[:AUTHORED_BY]-(p:Paper)-[:TOP_100]->(c:Community {name: 'Technology'})
            WITH a, c, count(DISTINCT p) as quantityTop100Papers
            MERGE (a)-[:POSSIBLE_REVIEWER]->(c)
            WITH a, c, quantityTop100Papers
            WHERE quantityTop100Papers > 1
            MERGE (a)-[:GURU]->(c)"""
    )     
    
    print('----------- Possible reviewer and gurus for each community created! -------------')


def connect_run_neo4j(uri,user,password):
    connector = ConnectorNeo4j(uri, user, password)
    connector.connect()
    session = connector.create_session()
    
    logger.info("Running Queries for Recommender ...")

    session.execute_write(run_query1)
    session.execute_write(run_query2)
    session.execute_write(run_query3)
    session.execute_write(run_query4)


    print('----------- All queries finished -----------')

    connector.close()