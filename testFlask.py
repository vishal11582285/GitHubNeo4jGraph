from flask import Flask

app=Flask(__name__)

@app.route('/')
def hello_w():
    return ('Lets see what this does')


@app.route('/user/<username>')
def show_user_profile(username):
    # show the user profile for that user
    return 'User %s' % username

@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'Post %d' % post_id

################
# for i in result:
#     query = """ MATCH (u1:User)-[:WORKS_ON]->(p:Project)<-[:WORKS_ON]-(u2:User) WHERE u1.user_id={u_id} RETURN u1.user_id,p.project_name,u2.user_id"""
#     # result_ = list(graph.run(query))
#     result_ = list(graph.run(query, {'u_id': i['p']['user_id']}))
#     if len(result_)>0:
#         # print(list(result_),end='\n')
#         for j in result_:
#             dict = {}
#             dict['u_id1'] = str(j['u1.user_id'])
#             dict['u_id2'] = str(j['u2.user_id'])
#             # print('{} {}'.format(dict['pr_name1'],dict['pr_name2']),end='\n')
#             # dict['lang'] = str(lang)
#             query = """
#                     MATCH (a:User),(b:User)
#                     WHERE a.user_id = {u_id1} AND b.user_id = {u_id2}
#                     MERGE (a)-[:DEVELOPER_DEVELOPER]->(b)
#                     MERGE (b)-[:DEVELOPER_DEVELOPER]->(a)
#                     """
#             graph.run(query, dict)
#             # print()
#             query = """
#                     MATCH (a:User) -[r1:WORKS_ON]->(p1:Project)
#                     WHERE a.user_id = {u_id1}
#                     RETURN p1.project_name
#                     """
#             a=list(graph.run(query, dict))
#             a=list(map(lambda x:x['p1.project_name'],list(a)))
#             # print(a)
#             query = """
#                     MATCH (a:User) -[r1:WORKS_ON]->(p1:Project)
#                     WHERE a.user_id = {u_id2}
#                     RETURN p1.project_name
#                     """
#             b = list(graph.run(query, dict))
#             b = list(map(lambda x: x['p1.project_name'], list(b)))
#             dict['weight']=len(list(set(a) & set(b)))
#             # print('{} {} {}'.format(dict['pr_name1'],dict['pr_name2'],weight),end='\n')
#             query = """
#                     MATCH (a:User) <-[r1:DEVELOPER_DEVELOPER]-(b:User)
#                     MATCH (b:User) <-[r2:DEVELOPER_DEVELOPER]-(a:User)
#                     WHERE a.user_id={u_id1} AND b.user_id={u_id2}
#                     SET r2.number_of_projects={weight}
#                     SET r1.number_of_projects={weight}
#                     """
#             graph.run(query, dict)