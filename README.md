# the purpose of this database :

1-It is made to simplify quering the data 

2-speed the analysis process 

3-Using data warehousing provided the project with large :

      data storage
      
      much faster performance 
      
      secure space to work thanks to the AWS secret access key.
      
 With the choice of working on the cloud instead on premisis gave us :
 
      more scalability
      
      cheaper costs comparing with buying new servers and hardware storage.
 
# the design :

To move the data from Udacity's s3 bucket to our database, 2 staging tables were created to move the data to them

To move the data to our db I made 1 fact table and 4 dimension tables in the sql_queries.py.

# How to run the python scripts :

1-create a user on aws website

2-create a cluster and attach required roles to it

3-Run create_tables.py in the terminal

4-Run etl.py in the terminal
