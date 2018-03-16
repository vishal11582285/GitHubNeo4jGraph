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
from basefunctions import openMySQlConnection, getResultsFromQueryAll,getResultsFromQueryFew,closeMySQLConnection
from basefunctions import createCSVFromResults,PROJECTS_COL_LIST,PROJECT_CSV_NAME,USERS_COL_LIST,USERS_CSV_NAME
from basefunctions import form_queries,read_csv_and_clean,create_nodes,create_rels
import pandas as pd

conn=openMySQlConnection(DATABASE_NAME)
form_queries()
read_csv_and_clean()
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

create_nodes()
create_rels()

query = """ MATCH (p:Project) RETURN p """
result = list(graph.run(query))

project_developers=OrderedDict()
for i in result:
    query = """ MATCH (p:Project)<-[:WORKS_ON]-() WHERE p.project_name={pr_name} RETURN COUNT(p) """
    result_ = list(graph.run(query,{'pr_name' : i['p']['project_name']}))
    project_developers[i['p']['project_name']]=result_[0]['COUNT(p)']
# a=sorted(a)
project_developers=OrderedDict(sorted(project_developers.items(), key=lambda item: item[1],reverse=True))
# for i in project_developers.items():
#     print('project:{}  developers:{}'.format(i[0],i[1]),end='\n')

query = """ MATCH (p:User) RETURN p LIMIT 100"""
result = list(graph.run(query))
#
developer_projects=OrderedDict()
for i in result[0:100]:
    query = """ MATCH (p:User)-[:WORKS_ON]->() WHERE p.user_name={u_name} RETURN COUNT(p) """
    result_ = list(graph.run(query,{'u_name' : i['p']['user_name']}))
    developer_projects[i['p']['user_name']]=result_[0]['COUNT(p)']

# a=sorted(a)
developer_projects=OrderedDict(sorted(developer_projects.items(), key=lambda item: item[1],reverse=True))
# for i in developer_projects.items():
#     print('developer:{}  projects:{}'.format(i[0],i[1]),end='\n')

with open('abc.txt','w') as r:
    r.write('Saved Developers and their number of projects:')
    for i in developer_projects.items():
        r.write(str(i))
    r.write('Saved Projects and their number of developers:')
    for i in project_developers.items():
        r.write(str(i))

print('All Queries pushed to Graph !',end='\n')
print('Written File !')
closeMySQLConnection(conn)