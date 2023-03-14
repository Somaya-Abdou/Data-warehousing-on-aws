import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """  this fun. copys data from s3 bucket to staging tables which are defined in in sql_queries file """ 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ this fun. inserts data from staging tables to fact and dimension tables which are defined in sql_queries file """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """  loading cluster parameters using config """  
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DB_NAME                = config.get('CLUSTER','DB_NAME')
    DB_USER                = config.get('CLUSTER','DB_USER')
    DB_PASSWORD            = config.get('CLUSTER','DB_PASSWORD')
    DB_PORT                = config.get('CLUSTER','DB_PORT')
    AWS_CLUSTER_IDENTIFIER = config.get('CLUSTER','AWS_CLUSTER_IDENTIFIER')
    DB_ENDPOINT            = config.get('CLUSTER','DB_ENDPOINT')
    I_Am_Role_Name         = config.get('IAM_ROLE','I_Am_Role_Name')
    RoleArn                = config.get('IAM_ROLE','RoleArn')
    LOG_DATA               = config.get('S3','LOG_DATA')
    LOG_JSONPATH           = config.get('S3','LOG_JSONPATH')
    SONG_DATA              = config.get('S3','SONG_DATA')
    SONG_JSONPATH          = config.get('S3','SONG_JSONPATH')
   
    """  connecting to the cluster """ 
    conn = psycopg2.connect(host=DB_ENDPOINT, dbname=DB_NAME, user= DB_USER, password=DB_PASSWORD, port=DB_PORT) 
                                
    cur = conn.cursor()                            
                        
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()