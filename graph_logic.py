''' Resources:
Mark Needham - Building a recommendation engine with Python and Neo4j : https://www.youtube.com/watch?v=VGCCVNlZmRI
Nigel Small | A Pythonic Tour of Neo4j and the Cypher Query Language: https://www.youtube.com/watch?v=Ma6lVy6x3Mg
Data Science and Recommendations - Nicole White: https://www.youtube.com/watch?v=60E2WV4iwIg'''
# :play https://guides.neo4j.com/pydata
# https://neo4j.com/docs/developer-manual/current/cypher/clauses/load-csv/
# http://py2neo.org/2.0/intro.html
# https://github.com/vishal11582285/GitHubNeo4jGraph.git
from py2neo import Graph as g,Path
import pandas as pd

from basefunctions import DATABASE_NAME
from basefunctions import openMySQlConnection, getResultsFromQueryAll,getResultsFromQueryFew,closeMySQLConnection
from basefunctions import createCSVFromResults,PROJECTS_COL_LIST,PROJECT_CSV_NAME,USERS_COL_LIST,USERS_CSV_NAME
from basefunctions import form_queries,read_csv_and_clean,PROJECT_OWNER_CSV_NAME

conn=openMySQlConnection(DATABASE_NAME)
# form_queries()
# read_csv_and_clean()

# users = pd.read_csv(USERS_CSV_NAME + '.csv')
# print(users.shape)
# projects = pd.read_csv(PROJECT_CSV_NAME + '.csv')
# print(projects.shape)

# graph = g()
# query = """
# MATCH (group:Group)-[:HAS_TOPIC]->(topic)
# WHERE group.name CONTAINS "Python"
# RETURN group.name, COLLECT(topic.name) AS topics
# """

# create_nodes()
# create_rels()
print('All Queries pushed to Graph !')
closeMySQLConnection(conn)