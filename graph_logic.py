''' Resources:
Mark Needham - Building a recommendation engine with Python and Neo4j : https://www.youtube.com/watch?v=VGCCVNlZmRI
Nigel Small | A Pythonic Tour of Neo4j and the Cypher Query Language: https://www.youtube.com/watch?v=Ma6lVy6x3Mg
Data Science and Recommendations - Nicole White: https://www.youtube.com/watch?v=60E2WV4iwIg'''
# :play https://guides.neo4j.com/pydata
# https://neo4j.com/docs/developer-manual/current/cypher/clauses/load-csv/
# http://py2neo.org/2.0/intro.html
# https://github.com/vishal11582285/GitHubNeo4jGraph.git
# mysql -h <host> -u <username> -p < create_mysql.sql
# Reco:
# https://www.kernix.com/blog/an-efficient-recommender-system-based-on-graph-database_p9

from py2neo import Graph as g,Path
from collections import OrderedDict
from basefunctions import DATABASE_NAME
from basefunctions import openMySQlConnection, getResultsFromQueryAll,getResultsFromQueryFew,closeMySQLConnection,create_dict_summary
from basefunctions import createCSVFromResults,PROJECTS_COL_LIST,PROJECT_CSV_NAME,USERS_COL_LIST,USERS_CSV_NAME
from basefunctions import form_queries,read_csv_and_clean,create_nodes,create_rels
import pandas as pd

conn=openMySQlConnection(DATABASE_NAME)
print('Begin....',end='\n')
# form_queries()
# read_csv_and_clean()
#
# users = pd.read_csv(USERS_CSV_NAME + '.csv')
# print(users.shape)
# projects = pd.read_csv(PROJECT_CSV_NAME + '.csv')
# print(projects.shape)

# okay
graph = g()
# query = """
# MATCH (group:Group)-[:HAS_TOPIC]->(topic)
# WHERE group.name CONTAINS "Python"
# RETURN group.name, COLLECT(topic.name) AS topics
# """

# create_nodes()
# create_rels()
# create_dict_summary()

query = """ MATCH (p:Project)-[r:PROJECT_PROJECT]->() DELETE r """
graph.run(query)


'''
Creating Project Project Relationships.
Relationship is estabished if 2 projects have atleast one common developer.
Weights on relationship is the number of common developers between them.
'''
query = """ MATCH (p:Project) RETURN p LIMIT 10"""
result = list(graph.run(query))
# project_developers = OrderedDict()
for i in result:
    query = """ MATCH (p1:Project)<-[:WORKS_ON]-(d:User)-[:WORKS_ON]->(p2:Project) WHERE p1.project_name={pr_name} RETURN p1.project_name,d.user_login,p2.project_name LIMIT 10"""
    # result_ = list(graph.run(query))
    result_ = list(graph.run(query, {'pr_name': i['p']['project_name']}))
    if len(result_)>0:
        # print(list(result_),end='\n')
        for j in result_:
            dict = {}
            dict['pr_name1'] = str(j['p1.project_name'])
            dict['pr_name2'] = str(j['p2.project_name'])
            # print('{} {}'.format(dict['pr_name1'],dict['pr_name2']),end='\n')
            # dict['lang'] = str(lang)
            query = """
                    MATCH (a:Project),(b:Project)
                    WHERE a.project_name = {pr_name1} AND b.project_name = {pr_name2}
                    MERGE (a)-[:PROJECT_PROJECT]->(b)
                    MERGE (b)-[:PROJECT_PROJECT]->(a)
                    """
            graph.run(query, dict)
            # print()
            query = """
                    MATCH (a:Project) <-[r1:WORKS_ON]-(u1:User)
                    WHERE a.project_name = {pr_name1}
                    RETURN u1.user_login
                    """
            a=list(graph.run(query, dict))
            a=list(map(lambda x:x['u1.user_login'],list(a)))
            # print(a)
            query = """
                    MATCH (a:Project) <-[r1:WORKS_ON]-(u1:User)
                    WHERE a.project_name = {pr_name2}
                    RETURN u1.user_login
                    """
            b = list(graph.run(query, dict))
            b = list(map(lambda x: x['u1.user_login'], list(b)))
            dict['weight']=len(list(set(a) & set(b)))
            # print('{} {} {}'.format(dict['pr_name1'],dict['pr_name2'],weight),end='\n')
            query = """
                    MATCH (a:Project) <-[r1:PROJECT_PROJECT]-(b:Project)
                    MATCH (b:Project) <-[r2:PROJECT_PROJECT]-(a:Project)
                    WHERE a.project_name={pr_name1} AND b.project_name={pr_name2}
                    SET r2.number_of_developers={weight}
                    SET r1.number_of_developers={weight}
                    """
            graph.run(query, dict)

'''
Creating Developer Developer Relationships.
Relationship is estabished if 2 developer are working on atleast one common project.
Weights on relationship is the number of common projects between them.
'''
query = """ MATCH (p:User) RETURN p LIMIT 10"""
result = list(graph.run(query))
# project_developers = OrderedDict()
for i in result:
    query = """ MATCH (u1:User)-[:WORKS_ON]->(p:Project)<-[:WORKS_ON]-(u2:User) WHERE u1.user_id={u_id} RETURN u1.user_id,p.project_name,u2.user_id"""
    # result_ = list(graph.run(query))
    result_ = list(graph.run(query, {'u_id': i['p']['user_id']}))
    if len(result_)>0:
        # print(list(result_),end='\n')
        for j in result_:
            dict = {}
            dict['u_id1'] = str(j['u1.user_id'])
            dict['u_id2'] = str(j['u2.user_id'])
            # print('{} {}'.format(dict['pr_name1'],dict['pr_name2']),end='\n')
            # dict['lang'] = str(lang)
            query = """
                    MATCH (a:User),(b:User)
                    WHERE a.user_id = {u_id1} AND b.user_id = {u_id2}
                    MERGE (a)-[:DEVELOPER_DEVELOPER]->(b)
                    MERGE (b)-[:DEVELOPER_DEVELOPER]->(a)
                    """
            graph.run(query, dict)
            # print()
            query = """
                    MATCH (a:User) -[r1:WORKS_ON]->(p1:Project)
                    WHERE a.user_id = {u_id1}
                    RETURN p1.project_name
                    """
            a=list(graph.run(query, dict))
            a=list(map(lambda x:x['p1.project_name'],list(a)))
            # print(a)
            query = """
                    MATCH (a:User) -[r1:WORKS_ON]->(p1:Project)
                    WHERE a.user_id = {u_id2}
                    RETURN p1.project_name
                    """
            b = list(graph.run(query, dict))
            b = list(map(lambda x: x['p1.project_name'], list(b)))
            dict['weight']=len(list(set(a) & set(b)))
            # print('{} {} {}'.format(dict['pr_name1'],dict['pr_name2'],weight),end='\n')
            query = """
                    MATCH (a:User) <-[r1:DEVELOPER_DEVELOPER]-(b:User)
                    MATCH (b:User) <-[r2:DEVELOPER_DEVELOPER]-(a:User)
                    WHERE a.user_id={u_id1} AND b.user_id={u_id2}
                    SET r2.number_of_projects={weight}
                    SET r1.number_of_projects={weight}
                    """
            graph.run(query, dict)


print('All Queries pushed to Graph !',end='\n')
print('Written File !')
closeMySQLConnection(conn)