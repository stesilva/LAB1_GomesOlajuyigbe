from neo4j import GraphDatabase


class ConnectorNeo4j:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._auth = (user,password)
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=self._auth)

        try:
            self._driver.verify_connectivity()
            print("Connection successful!")
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")


    def close(self):
        if self._driver is not None:
            self._driver.close()