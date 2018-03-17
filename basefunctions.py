import os
from collections import OrderedDict
from shutil import copyfile

import pandas as pd
import pymysql as p
from py2neo import Graph as g

DATABASE_NAME="githubSQL"
cursor=p.cursors
PROJECTS_COL_LIST=['project_id','project_name','owner_id','project_description','project_language','URL','project_created_at']
PROJECT_CSV_NAME='GitHubProjects'
USERS_COL_LIST=['user_id','login','user_name','user_company','user_location','user_email','user_type','user_created_on']
USERS_CSV_NAME='GitHubUsers'
PROJECT_OWNER_CSV_NAME='ProjectOwnerRel'
PROJECTS_OWNER_COL_LIST=['project_id','project_name','owner_id','project_language']
FOLLOWERS_CSV_NAME='Followers'
FOLLOWERS_COL_LIST=['user_id','follower_id','created_at']

DESTINATION_DIRECTORY='C:/Users/visha/AppData/Roaming/Neo4j Desktop/Application/neo4jDatabases/database-63968ab1-e409-4ae4-9eaf-ac1388de3cfb/installation-3.3.4/import/'
# BASE_PATH='/'
# id,owner_id,name,description,language,created_at

graph=g()

def create_nodes():
    print('Initializing Graph.....')
    query = """
    LOAD CSV WITH HEADERS
    FROM "file:///GitHubProjectsGraph.csv"
    AS row
    CREATE (:Project {project_description:row.project_description, project_id:row.project_id,
    URL:row.URL,project_language:row.project_language,owner_id:row.owner_id,
    project_created_at: toInteger(row.project_created_at),project_name: row.project_name})
    """

    graph.run(query)

    query = """
    CREATE CONSTRAINT ON (p:Project)
    ASSERT p.project_name IS UNIQUE
    """

    graph.run(query)

    query = """
    USING PERIODIC COMMIT
    LOAD CSV WITH HEADERS
    FROM "file:///GitHubUsersGraph.csv"
    AS row
    CREATE (:User {user_name:row.user_name, user_id:row.user_id,
    user_email:row.user_email,user_login:row.login,user_created_on: toInteger(row.user_created_on),user_company: row.user_company,
    user_type:row.user_type,user_location:row.user_location})
    """
    graph.run(query)

    query = """
    CREATE CONSTRAINT ON (p:User)
    ASSERT p.user_id IS UNIQUE
    """
    graph.run(query)
    print('Nodes Formed !')

def proj_proj_dev_dev_rels():
    query = """ MATCH (p:Project)-[r:PROJECT_PROJECT]->() DELETE r """
    graph.run(query)
    query = """ MATCH (p:User)-[r:DEVELOPER_DEVELOPER]->() DELETE r """
    graph.run(query)

    '''
    Creating Project Project Relationships.
    Relationship is estabished if 2 projects have atleast one common developer.
    Weights on relationship is the number of common developers between them.
    '''
    query = """ MATCH (p:Project) RETURN p """
    result = list(graph.run(query))
    # project_developers = OrderedDict()
    visited_nodes = list()
    proj = {}
    for i in result:
        query = """ MATCH (p1:Project)<-[:WORKS_ON]-(d:User)-[:WORKS_ON]->(p2:Project) WHERE p1.project_name={pr_name} RETURN p1.project_name,d.user_login,p2.project_name"""
        # result_ = list(graph.run(query))
        cur_proj_name = i['p']['project_name']
        list_ = [cur_proj_name]
        result_ = list(graph.run(query, {'pr_name': cur_proj_name}))
        # print(i,end='\n')
        # print(result_,end='\n')
        list_ = list_ + list(map(lambda x: x['p2.project_name'], result_))
        # print(list_, end='\n')

        while len(list_) > 0 and cur_proj_name not in visited_nodes:
            cur_proj_name = list_[0]
            visited_nodes.append(str(cur_proj_name))
            print(len(list(visited_nodes)), end='\n')
            list_.pop(list_.index(cur_proj_name))
            for j in list_:
                dict = {}
                dict['pr_name1'] = cur_proj_name
                dict['pr_name2'] = j
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
                if dict['pr_name1'] not in list(proj.keys()):
                    a = list(graph.run(query, dict))
                    a = list(map(lambda x: x['u1.user_login'], list(a)))
                    proj[dict['pr_name1']] = a
                # print(a)
                query = """
                        MATCH (a:Project) <-[r1:WORKS_ON]-(u1:User)
                        WHERE a.project_name = {pr_name2}
                        RETURN u1.user_login
                        """
                if dict['pr_name2'] not in list(proj.keys()):
                    b = list(graph.run(query, dict))
                    b = list(map(lambda x: x['u1.user_login'], list(b)))
                    proj[dict['pr_name2']] = b

                # b = list(graph.run(query, dict)) if
                # b = list(map(lambda x: x['u1.user_login'], list(b)))
                # proj[dict['pr_name2']] = b

                dict['weight'] = len(list(set(proj[dict['pr_name1']]) & set(proj[dict['pr_name2']])))

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
    visited_nodes = list()
    developers = {}
    query = """ MATCH (p:User) RETURN p """
    result = list(graph.run(query))
    # project_developers = OrderedDict()

    ################

    for i in result:
        query = """ MATCH (u1:User)-[:WORKS_ON]->(p:Project)<-[:WORKS_ON]-(u2:User) WHERE u1.user_id={u_id} RETURN u1.user_id,p.project_name,u2.user_id"""
        # result_ = list(graph.run(query))
        cur_proj_name = i['p']['user_id']
        list_ = [cur_proj_name]
        result_ = list(graph.run(query, {'u_id': i['p']['user_id']}))
        # print(i,end='\n')
        # print(result_,end='\n')
        list_ = list_ + list(map(lambda x: x['u2.user_id'], result_))
        # print(list_, end='\n')

        while len(list_) > 0 and cur_proj_name not in visited_nodes:
            cur_proj_name = list_[0]
            visited_nodes.append(str(cur_proj_name))
            print(len(list(visited_nodes)), end='\n')
            list_.pop(list_.index(cur_proj_name))
            for j in list_:
                dict = {}
                dict['u_id1'] = cur_proj_name
                dict['u_id2'] = j
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
                if dict['u_id1'] not in list(developers.keys()):
                    a = list(graph.run(query, dict))
                    a = list(map(lambda x: x['p1.project_name'], list(a)))
                    developers[dict['u_id1']] = a
                # print(a)
                query = """
                        MATCH (a:User) -[r1:WORKS_ON]->(p1:Project)
                        WHERE a.user_id = {u_id2}
                        RETURN p1.project_name
                        """
                if dict['u_id2'] not in list(developers.keys()):
                    b = list(graph.run(query, dict))
                    b = list(map(lambda x: x['p1.project_name'], list(b)))
                    developers[dict['u_id2']] = b

                # b = list(graph.run(query, dict)) if
                # b = list(map(lambda x: x['u1.user_login'], list(b)))
                # proj[dict['pr_name2']] = b

                dict['weight'] = len(list(set(developers[dict['u_id1']]) & set(developers[dict['u_id2']])))

                # print('{} {} {}'.format(dict['pr_name1'],dict['pr_name2'],weight),end='\n')
                query = """
                        MATCH (a:User) <-[r1:DEVELOPER_DEVELOPER]-(b:User)
                        MATCH (b:User) <-[r2:DEVELOPER_DEVELOPER]-(a:User)
                        WHERE a.user_id={u_id1} AND b.user_id={u_id2}
                        SET r2.number_of_projects={weight}
                        SET r1.number_of_projects={weight}
                        """
                graph.run(query, dict)

def create_dict_summary():
    query = """ MATCH (p:Project) RETURN p LIMIT 1000"""
    result = list(graph.run(query))
    project_developers = OrderedDict()
    for i in result:
        query = """ MATCH (p:Project)<-[:WORKS_ON]-() WHERE p.project_name={pr_name} RETURN COUNT(p) """
        result_ = list(graph.run(query, {'pr_name': i['p']['project_name']}))
        project_developers[i['p']['project_name']] = result_[0]['COUNT(p)']
    # a=sorted(a)
    project_developers = OrderedDict(sorted(project_developers.items(), key=lambda item: item[1], reverse=True))
    # for i in project_developers.items():
    #     print('project:{}  developers:{}'.format(i[0],i[1]),end='\n')

    query = """ MATCH (p:User) RETURN p LIMIT 1000"""
    result = list(graph.run(query))
    #
    developer_projects = OrderedDict()
    for i in result[0:100]:
        query = """ MATCH (p:User)-[:WORKS_ON]->() WHERE p.user_name={u_name} RETURN COUNT(p) """
        result_ = list(graph.run(query, {'u_name': i['p']['user_name']}))
        developer_projects[i['p']['user_name']] = result_[0]['COUNT(p)']

    # a=sorted(a)
    developer_projects = OrderedDict(sorted(developer_projects.items(), key=lambda item: item[1], reverse=True))
    # for i in developer_projects.items():
    #     print('developer:{}  projects:{}'.format(i[0],i[1]),end='\n')

    with open('abc.txt', 'w') as r:
        r.write('Saved Developers and their number of projects:')
        for i in developer_projects.items():
            r.write(str(i))
        r.write('Saved Projects and their number of developers:')
        for i in project_developers.items():
            r.write(str(i))

def works_on_rels():
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
    return a

def follows_rels():
    read_df_rels = pd.read_csv(FOLLOWERS_CSV_NAME + '.csv')
    a = 0
    print('Forming Links.....', end='\n')
    for uid, follower, cr in zip(read_df_rels['user_id'], read_df_rels['follower_id'], read_df_rels['created_at']):
        # print(type(pname),type(uid),type(lang))
        a += 1
        dict = {}
        dict['date'] = str(cr)
        dict['uid'] = str(uid)
        dict['fol'] = str(follower)
        query = """
                MATCH (a:User),(b:User)
                WHERE a.user_id = {uid} AND b.user_id = {fol}
                CREATE (b)-[r:FOLLOWS]->(a)
                SET r.date={date}"""
        result = graph.run(query, dict)
    return a

def create_rels():
    print('Creating Project-Worker Links...')
    a=works_on_rels()
    print('Created {} Links !'.format(a), end='\n')

    print('Creating User-Follower Links...')
    a=follows_rels()
    print('Created {} Links !'.format(a), end='\n')

    print('Creating Project-Project and Developer-Developer Collboration Links...')
    proj_proj_dev_dev_rels()

    print('Graph Ready !')

def read_csv_and_clean():
    users = pd.read_csv(USERS_CSV_NAME + '.csv')
    # print(list(users.shape))
    # print(users.apply(lambda x: sum(x.isnull())))
    print(users.shape)
    users['user_company'].fillna('UNKNOWN', inplace=True)
    users['user_location'].fillna('UNKNOWN', inplace=True)
    users['user_name'].fillna('JOHN DOE', inplace=True)
    users['user_email'].fillna('UNKNOWN', inplace=True)
    print(users.shape)
    # print(list(users.shape))
    # print(users.apply(lambda x: sum(x.isna())))

    projects = pd.read_csv(PROJECT_CSV_NAME + '.csv')
    projects = pd.DataFrame(projects[projects['project_description'].notnull()])
    # print(list(projects.shape))
    # print(list(projects.shape))
    # print(projects.apply(lambda x: sum(x.isna())))

    # projects['user_company'].fillna('UNKNOWN', inplace=True)
    # projects['user_location'].fillna('UNKNOWN', inplace=True)
    # projects['user_name'].fillna('JOHN DOE', inplace=True)
    # projects['user_email'].fillna('UNKNOWN', inplace=True)
    # print(list(projects.shape))

    # print(projects['project_description'].isnull())

    # print(projects.apply(lambda x: sum(x.isna())))
    users['user_name']=list(map(lambda x: str(x).replace('"', ''),users['user_name']))
    # users = pd.DataFrame(users)
    print(users.shape)
    users.to_csv(USERS_CSV_NAME+'Graph' + '.csv')
    projects.to_csv(PROJECT_CSV_NAME + 'Graph'+'.csv')
    print('Files Written ! {} {} '.format(USERS_CSV_NAME,PROJECT_CSV_NAME))
    # print(os.listdir())
    files=list(filter(lambda x:str(x).__contains__('Graph.csv'),os.listdir()))
    # print(list(files))
    # print(DESTINATION_DIRECTORY + src
    for i in list(files):
        copyfile(i, DESTINATION_DIRECTORY+i)
    # copyfile(PROJECT_CSV_NAME+'.csv', DESTINATION_DIRECTORY+PROJECT_CSV_NAME+'.csv')

def form_queries():
    query = "show tables"
    results = getResultsFromQueryAll(query)

    query = "show fields from users"
    results = getResultsFromQueryAll(query)

    query = "select id ,name,owner_id,description,language,url,created_at as p1 " \
            "from projects where name is not NULL and language IS NOT NULL and url is not null group by name"
    results = getResultsFromQueryAll(query)
    createCSVFromResults(results, PROJECT_CSV_NAME, PROJECTS_COL_LIST)

    query = "select id ,name,owner_id,language from projects " \
            "where name is not NULL and description is NOT NULL and language IS NOT NULL and url is not null"
    results = getResultsFromQueryAll(query)
    createCSVFromResults(results, PROJECT_OWNER_CSV_NAME, PROJECTS_OWNER_COL_LIST)

    query = "select id,login,name,company,location,email,type,created_at from users " \
            "where login is not NULL and email IS NOT NULL and name is not null"
    results = getResultsFromQueryAll(query)
    createCSVFromResults(results, USERS_CSV_NAME, USERS_COL_LIST)

    query = "select user_id,follower_id,created_at from followers"
    results = getResultsFromQueryAll(query)
    createCSVFromResults(results, FOLLOWERS_CSV_NAME, FOLLOWERS_COL_LIST)

def createCSVFromResults(results,name_,col_list):
    # abc=pd.DataFrame(col_list,columns=)
    abc=pd.DataFrame(results,columns=col_list)
    abc.to_csv(str(name_)+'.csv')
    print('CSV CREATED : {}'.format(str(name_)+'.csv'))

def refineReturnList(cursor_list):
    results = list(cursor_list)
    results = list(map(lambda x: list(x), results))
    return results

def printThis(results):
    print('Printing Results:', end='\n')
    for result in results:
        print(result,end='\n')

def getResultsFromQueryAll(query,p=0):
    cursor.execute(query)
    results=refineReturnList(cursor.fetchall())
    printThis(results) if(p==1) else 0
    return results

def getResultsFromQueryFew(query,howMany,p=0):
    cursor.execute(query)
    results = refineReturnList(cursor.fetchmany(howMany))
    printThis(results) if(p==1) else 0
    return results

def openMySQlConnection(database_name):
    # Open database connection with MySQL database
    conn = p.connect("localhost", "root", "root",database_name )
    # prepare a cursor object using cursor() method
    global cursor
    cursor = conn.cursor()
    cursor.execute("use "+str(database_name))
    return conn

def closeMySQLConnection(db):
    db.close()