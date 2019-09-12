import pandas as pd
from sqlalchemy import create_engine 


## load credentials
#json load
import os
import json

CONFIG_PATH = '../configs'
DATA_PATH = '../data'

#loads config
with open(os.path.join(CONFIG_PATH,'filename.json'),'r') as file_:

    mysql_creds = json.load(file_)


#all methods here:

# function to load dataset
def load_dataset(filepath):
    """
    
        read the file with the help of Pandas and return that file given the filepath on config as a parameter
        
        args:
            filepath: str filepath
            
            
        return:
            data: pd.DataFrame
            
        example:
        >> import pandas as pd
        >> load_dataset(filepath=path/to/file.csv)
        pandas.core.frame.DataFrame
            
    """
    data_csv = pd.read_csv(filepath,chunksize=None, header = 0)
    return data_csv

# function to create an engine
def create_engine_function(sqldb_route,username,password,host,database):
    """
        creates sqlalchemy engine given a set of config parameters
        
        args:
            sqldb_route: Fixed string defines the type of server route
            user: string credential username
            password: str credentials password
            host: string host url
            database: string database name
            
        return:
            sqlalchemy_engine
            
            
    example:
    >> create_engine_function(sqldb_route=dict_config['mysql_db_config']['sqldb_route'],
              user=dict_config['mysql_db_config']['username'],
              password=dict_config['mysql_db_config']['password'],
              host=dict_config['mysql_db_config']['host'],
              database=dict_config['mysql_db_config']['database'])
    
    <function __main__.create_engine(dict_config)>
            
    """
    engine_url = '{0}://{1}:{2}@{3}/{4}'.format(sqldb_route,username,password,host,database)
    engine = create_engine(engine_url)
    return engine

# function to write data into database
def write_to_do(df,table_name, engine, check, index_column):
    """
      
     Write records stored in a DataFrame to a mySQL database.
        
        args:
            df: pd.DataFrame 
            table_name : string
            engine: sqlalchemy.engine.Engine
            check: str {‘fail’, ‘replace’, ‘append’}, default ‘fail’
            index_column: bool, default True
            
        raises:
            ValueError
              When the table already exists and if_exists is ‘fail’ (the default).
            
        example:
        
            write_to_do(df=df,table_name = dict_config['db_import']['table_name'],
                        engine = engine,
                        check = dict_config['db_import']['if_exists'],
                        index_column = dict_config['db_import']['index_column'],
                       )
            <function __main__.write_to_do(df, table_name, engine, check, index_column)>
    
    
    """
    with engine.connect() as conn, conn.begin():
        df.to_sql(name = table_name, con = conn, if_exists = check,index=index_column, chunksize=100)

def database_writter():
    print("Running main")
    ## create engine
    engine=create_engine_function(sqldb_route=mysql_creds['mysql_db_config']['sqldb_route'],
                  username=mysql_creds['mysql_db_config']['username'],
                  password=mysql_creds['mysql_db_config']['password'],
                  host=mysql_creds['mysql_db_config']['host'],
                  database=mysql_creds['mysql_db_config']['database'])

    ##load data
    df = load_dataset(filepath=os.path.join(DATA_PATH,str(mysql_creds['data_frame']['df'])))

    ##write to sql db
    write_to_do(df= df,
            table_name = mysql_creds['db_import']['table_name'],
                engine = engine,
                check = mysql_creds['db_import']['if_exists'],
                index_column = mysql_creds['db_import']['index_column'],
               )
        
if __name__ == "__main__":

    print("I am here")
    
    database_writter()
    
    print("All done")
