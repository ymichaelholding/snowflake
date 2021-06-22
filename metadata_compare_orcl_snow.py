import cx_Oracle
import snowflake.connector
from lib.db_connections import DBconnections
import pandas as pd
import time
#import fastapi
import logging

def setup_oracle():
     db= DBconnections('ORACLE')
     connections = db.oracle_set_connections()
     cursor = connections.cursor()
     return cursor,connections

def setup_snowflake():
     conn = snowflake.connector.connect(
          user="*****",
          password="*****",
          account="*****",
          warehouse="****",
          role='*****',
          database="****",
          schema="****"
     )
     snowflake_cursor=conn.cursor()
     return snowflake_cursor,conn

start_time = time.time()
cursor,connections=setup_oracle()
snowflake_cursor,conn=setup_snowflake()
end_time = time.time()
print(f"The execution time of connection establihsed is: {end_time-start_time} in sec")

def get_src_meta_queries(table_name):
   source_sql_query='select table_name,column_name,data_Type,data_length,data_precision,data_Scale,' \
                 'data_default from USER_tAB_COLUMNS WHERE TABLE_NAME= ' + table_name
   print(source_sql_query)
   return source_sql_query

def get_tar_meta_queries(table_name):
   target_sql_query='select table_name,column_name,data_type,character_maximum_length,numeric_precision,numeric_scale '\
                  ' FROM INFORMATION_sCHEMA.COLUMNS ' \
                  ' WHERE TABLE_NAME= ' + table_name
   print(target_sql_query)
   return target_sql_query

def get_results(query,db_conn):
  df =  pd.read_sql(query,db_conn)
  return df

start_time = time.time()
source_sql_query=get_src_meta_queries("'EMPLOYEES'")
target_sql_query=get_tar_meta_queries("'EMPLOYEES'")
end_time = time.time()
print(f"The execution time of parsing query is: {end_time-start_time} in sec")

start_time = time.time()
oracle_df = get_results(source_sql_query,connections)
end_time = time.time()
print(f"The execution time of oracle query result set is: {end_time-start_time} in sec")
start_time = time.time()
snowflake_df=get_results(target_sql_query,conn)
end_time = time.time()
print(f"The execution time of snowflake query result set is: {end_time-start_time} in sec")

#close the cursor in snowflake
conn.close()
#close the cursor in oracle
connections.close()

def comma_seperate_values(rows):
    str_concat=''
    for i in rows:
        str_concat = str_concat + ',' + str(i)
    return str_concat[1:]

def final_dict_append(key,value):
    res_dict = {}
    res_dict[key]=value
    return res_dict


def get_missing_columns(source_df,target_df):
     missing_df =  pd.merge(source_df, target_df, on="COLUMN_NAME", how="outer", 
                   right_index=True, 
                   left_index=True).query("TABLE_NAME_y.isnull()",engine='python')["COLUMN_NAME"]

     column_list = comma_seperate_values(missing_df)
     return column_list

def get_all_validations():
    column_list=get_missing_columns(oracle_df,snowflake_df)
    final_dict=final_dict_append("'Missing columns'",column_list)
    return final_dict

def test_results(oracle_df,snowflake_df):
   #print(oracle_df.head(5))
   #print(snowflake_df.head(5))
   #missing_df =  pd.merge(oracle_df, snowflake_df, on="COLUMN_NAME", how="outer", right_index=True, left_index=True)
   #print(missing_df)
   print(get_missing_columns(oracle_df,snowflake_df))
   test = get_all_validations()
   for keys,values in test.items():
       print(keys + ':' + values)
   return 0

def main():
     test_results(oracle_df,snowflake_df)

#app = fastApI()

#app.get('/')

if __name__ == "__main__":
     main()

