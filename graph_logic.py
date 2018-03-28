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

from py2neo import Graph as g

from basefunctions import DATABASE_NAME
from basefunctions import form_queries, read_csv_and_clean
from basefunctions import openMySQlConnection, closeMySQLConnection,create_nodes,create_rels,create_dict_summary,read_dictionary_from_graph_results

#
# users = pd.read_csv(USERS_CSV_NAME + '.csv')
# print(users.shape)
# projects = pd.read_csv(PROJECT_CSV_NAME + '.csv')
# print(projects.shape)
# query = """
# MATCH (group:Group)-[:HAS_TOPIC]->(topic)
# WHERE group.name CONTAINS "Python"
# RETURN group.name, COLLECT(topic.name) AS topics
# """

conn=openMySQlConnection(DATABASE_NAME)

print('Begin....',end='\n')

'Form SQL queries and Create CSV files for table data. (MySQL)'
form_queries()

'Prune Missing Data and blank values from CSV files.'
# read_csv_and_clean()

'Initialize Neo4j Graph driver.'
graph = g()

'Create Nodes In Graph. Projects and Users.'
# create_nodes()

'Create Relationships In Graph. User-Project "WORKS_ON" and User-User "FOLLOWS" ' \
'Also From Collboration Graph. Cite Paper. Project-Project and User-User. (Imp)' \
'Also create relation ship between the Actor who reated the Pull request for a project and the owner of that Project'
create_rels()

'Create Project-User and User-Project summaries eg. How many developers on working on each project. '
# create_dict_summary()

print('All Queries pushed to Graph !',end='\n')
print('Written File !')


# #Recommemdation logic
# query = """
#         MATCH (a:User),(b:User),(c:Project)
#         MATCH (a)-[:FOLLOWS]->(b)
#         MATCH (b)-[:WORKS_ON]->(c)
#         WITH a.user_login AS UserLoginName,COLLECT(DISTINCT b.user_login) AS AllOtherUsers,COLLECT(DISTINCT c.project_name) AS ProjectNames
#         RETURN UserLoginName,AllOtherUsers,ProjectNames
#         """
#
# result = list(graph.run(query))
#
# read_dictionary_from_graph_results(result)
#
#
#
# # CALL algo.pageRank.stream('Project','PROJECT_PROJECT',
# #   {iterations:20, dampingFactor:0.85, concurrency:4})
# # YIELD node, score
# # RETURN node.project_name,node.project_language,score order by score DESC
#
# 'CLose the open MySQL connection'
closeMySQLConnection(conn)