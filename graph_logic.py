''' Resources:
Mark Needham - Building a recommendation engine with Python and Neo4j : https://www.youtube.com/watch?v=VGCCVNlZmRI
Nigel Small | A Pythonic Tour of Neo4j and the Cypher Query Language: https://www.youtube.com/watch?v=Ma6lVy6x3Mg
Data Science and Recommendations - Nicole White: https://www.youtube.com/watch?v=60E2WV4iwIg'''
# :play https://guides.neo4j.com/pydata
# https://neo4j.com/docs/developer-manual/current/cypher/clauses/load-csv/
# http://py2neo.org/2.0/intro.html

from py2neo import Graph as g,Path
import pandas as pd

from basefunctions import DATABASE_NAME
# from neo4j.v1 import GraphDatabase
from basefunctions import openMySQlConnection, getResultsFromQueryAll,getResultsFromQueryFew,closeMySQLConnection
from basefunctions import createCSVFromResults,PROJECTS_COL_LIST,PROJECT_CSV_NAME,USERS_COL_LIST,USERS_CSV_NAME
from basefunctions import form_queries,read_csv_and_clean,PROJECT_OWNER_CSV_NAME

import pandas as pd

def create_nodes():
    query = """
    LOAD CSV WITH HEADERS
    FROM "file:///GitHubProjectsGraph.csv"
    AS row
    CREATE (:Project {project_description:row.project_description, project_id:row.project_id,
    URL:row.URL,project_language:row.project_language,owner_id:row.owner_id,
    project_created_at: toInteger(row.project_created_at),project_name: row.project_name})
    """
    result = graph.run(query)

    # query="CREATE INDEX ON :Project(project_name)"
    # result =graph.run(query)

    query = """
    CREATE CONSTRAINT ON (p:Project)
    ASSERT p.project_name IS UNIQUE
    """
    result = graph.run(query)

    query = """
    USING PERIODIC COMMIT
    LOAD CSV WITH HEADERS
    FROM "file:///GitHubUsersGraph.csv"
    AS row
    CREATE (:User {user_name:row.user_name, user_id:row.user_id,
    user_email:row.user_email,user_login:row.login,user_created_on: toInteger(row.user_created_on),user_company: row.user_company,
    user_type:row.user_type,user_location:row.user_location})
    """
    result = graph.run(query)

    # query="CREATE INDEX ON :User(user_id)"
    # result =graph.run(query)

    query = """
    CREATE CONSTRAINT ON (p:User)
    ASSERT p.user_id IS UNIQUE
    """
    result = graph.run(query)

def create_rels():
    read_df_rels = pd.read_csv(PROJECT_OWNER_CSV_NAME + '.csv')
    a = 0

    print('Forming Links.....', end='\n')

    for pname, uid, lang in zip(read_df_rels['project_name'], read_df_rels['owner_id'],
                                read_df_rels['project_language']):
        # print(type(pname),type(uid),type(lang))
        a += 1
        dict = {}
        dict['pname'] = str(pname)
        dict['uid'] = str(uid)
        dict['lang'] = str(lang)
        query = """
        MATCH (a:Project),(b:User)
        WHERE a.project_name = {pname} AND b.user_id = {uid}
        CREATE (b)-[r:WORKS_ON]->(a)
        SET r.language={lang}"""
        result = graph.run(query, dict)

    print('Created {} Links !'.format(a), end='\n')


# conn=openMySQlConnection(DATABASE_NAME)
# form_queries()
# read_csv_and_clean()

users = pd.read_csv(USERS_CSV_NAME + '.csv')
print(users.shape)
projects = pd.read_csv(PROJECT_CSV_NAME + '.csv')
print(projects.shape)

graph = g()
query = """
MATCH (group:Group)-[:HAS_TOPIC]->(topic)
WHERE group.name CONTAINS "Python" 
RETURN group.name, COLLECT(topic.name) AS topics
"""

print('Initializing Graph.....')

# create_nodes()

# create_rels()
print('All Queries pushed to Graph !')
# closeMySQLConnection(conn)