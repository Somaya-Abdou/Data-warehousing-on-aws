import configparser
import psycopg2
import pandas as pd
import os
import json
import boto3
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """  this fun. drops all tables if they exist which are written in in sql_queries file """ 
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """  this fun. creates all tables if they exist which are written in in sql_queries file """ 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """  Taking all values in dwh file using config function """ 
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
    
    """ connecting to the cluster using psycopg2 library """ 
    conn = psycopg2.connect(host=DB_ENDPOINT, dbname=DB_NAME, user= DB_USER, password=DB_PASSWORD, port=DB_PORT)
                                          
    cur = conn.cursor()
   
  
    drop_tables(cur, conn)
    create_tables(cur, conn)
                                
    conn.commit()                  
    conn.close()


if __name__ == "__main__":
    main()