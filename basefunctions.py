import pymysql as p
import numpy as np
import pandas as pd
from shutil import copyfile
import os
DATABASE_NAME="githubSQL"
cursor=p.cursors
PROJECTS_COL_LIST=['project_id','project_name','owner_id','project_description','project_language','URL','project_created_at']
PROJECT_CSV_NAME='GitHubProjects'
USERS_COL_LIST=['user_id','login','user_name','user_company','user_location','user_email','user_type','user_created_on']
USERS_CSV_NAME='GitHubUsers'
PROJECT_OWNER_CSV_NAME='ProjectOwnerRel'
PROJECTS_OWNER_COL_LIST=['project_id','project_name','owner_id','project_language']
DESTINATION_DIRECTORY='C:/Users/User/AppData/Roaming/Neo4j Desktop/Application/neo4jDatabases/database-81e0ffd7-3dc8-426d-a54a-dbb7d75778a3/installation-3.3.2/import/'
# BASE_PATH='/'
# id,owner_id,name,description,language,created_at

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
            "from projects where name is not NULL and description is NOT NULL and language IS NOT NULL and url is not null group by name"
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