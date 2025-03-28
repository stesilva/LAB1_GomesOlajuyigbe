from scripts.connector_neo4j import ConnectorNeo4j
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def print_results(records, summary):
    time = summary.result_available_after
    print(f"{len(records)} ROWS RETURNED IN {time} ms.")

# Use Pagerank algorithm to check most influential papers based on the number of papers that cited it
def run_query1(session):
    result = session.run(
        """CALL gds.graph.project(
                'paperCites',
                'Paper',
                'CITES'
            );

            CALL gds.pageRank.write.estimate(
            'paperCites', 
                {
                    writeProperty: 'pageRank'
                }
            );

            CALL gds.pageRank.stream('paperCites')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).paperDOI AS paperId, score as influence
            ORDER BY influence DESC
            LIMIT 10;"""
    )       
    
    print('----------- Results for PageRank Algorithm on identifying the most important papers -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)

# Use NodeSimilarity algorithm to identify the simlar authors that collaborate together on papers
# topK is set to one to limit the number of scores computed per node. This limits the result to the single most similar author pair for each node
# degreeCutoff is set to 2 to ignore nodes with only 1 AUTHORED_BY relationship and only considers authors with 2 or more papers
# we filtered the results to only include similar authors with a similarity less than 1 to  filter out trivial cases  

def run_query2(session):
    result = session.run(
        """CALL gds.graph.project(
                'similarAuthors',
                ['Author', 'Paper'],
                {
                    AUTHORED_BY: 
                    {
                        orientation:'REVERSE'
                    }
                }
            );

            CALL gds.nodeSimilarity.write.estimate(
                'similarAuthors', 
                {
                    writeRelationshipType: 'SIMILAR',
                    writeProperty: 'score'
                }
            );

            CALL gds.nodeSimilarity.stream(
                'similarAuthors', 
                {
                    topK:1,
                    degreeCutoff:2
                }
            )
            YIELD node1, node2, similarity
            WHERE similarity < 1
            RETURN gds.util.asNode(node1).name AS Author1,
              gds.util.asNode(node2).name AS Author2, similarity
            ORDER BY similarity DESC"""
    )       
    
    print('----------- Results for Similarity Algorithm to identify similar authors -------------')
    records = list(result)
    summary = result.consume()
    print_results(records, summary)

def connect_run_neo4j(uri,user,password):
    connector = ConnectorNeo4j(uri, user, password)
    connector.connect()
    session = connector.create_session()
    
    logger.info("Running Queries for Graph Algorithms ...")

    session.execute_write(run_query1)
    session.execute_write(run_query2)


    print('----------- All queries finished -----------')

    connector.close()