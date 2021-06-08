import cx_Oracle
import snowflake.connector
from lib.db_connections import DBconnections

def setup_oracle():
     db= DBconnections('ORACLE')
     connections = db.oracle_set_connections()
     cursor = connections.cursor()
     return cursor,connections

def setup_snowflake():
     conn = snowflake.connector.connect(
          user="Michael",
          password="*******",
          account="************,
          warehouse="COMPUTE_WH",
          role='*************',
          database="ORCL",
          schema="HR"
     )
     snowflake_cursor=conn.cursor()
     return snowflake_cursor,conn

cursor,connections=setup_oracle()
snowflake_cursor,conn=setup_snowflake()

#read the sample records
result = cursor.execute("select employee_id,first_name from employees")

#check the roles in snowflakes
for rec in snowflake_cursor.execute("select current_role(), current_database(), current_schema(), current_warehouse()"):
    print(rec)

#executes the records in snowflakes
for i in result:
     snowflake_cursor.execute(  "INSERT INTO HR.employees(employee_id, first_name) VALUES " +
        " ( " + str(i[0]) + ", '" + i[1] + "')")

#commit in snowflake
conn.commit()

#close the cursor in snowflake
conn.close()
#close the cursor in oracle
connections.close()

if __name__ == "__main__":
     #should replace main functions
     print("check the exercies")

