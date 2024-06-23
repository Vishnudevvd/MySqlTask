import logging
import pandas as pd

import mysql.connector as conn

from utils import DB_NAME, FILE_PATH, CONFIG, SQL_FILE_PATH, ALTER_TABLE_PATH

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')

file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


#database creation
def create_database(cursor, database_name):
    try:
        cursor.execute(
            f"CREATE DATABASE {database_name} DEFAULT CHARACTER SET 'UTF8MB4'"
        )
    except conn.Error as err:
        logger.exception(f"Failed creating database : {err}")
        exit(1)
    else:
        logger.info(f"Database {database_name} created successfully")

# database connection
def database_connection(cursor, database_name):
    try:
        cursor.execute(f"USE {database_name}")
    except conn.Error as err:
        logger.info(f"Database {database_name} doesn't exist.")
        if err.errno == conn.errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, database_name)
            cnx.database = database_name
        else:
            logger.error(err)
    else:
        logger.info("database connection successful")

# table creation
def create_tables(cursor, sql_file_path, table_name):
    try:
        with open(sql_file_path, 'r') as p:
            sql_statements = p.read().split(';')
            for statement in sql_statements:
                if f'create table `{table_name}`'.lower() in statement.lower():
                    cursor.execute(statement.strip())
                    logger.info(f"Table {table_name} created succesfully")
    except conn.Error as err:
        logger.error(f"Create table {table_name} failed")


# check tables already exists if not runs the function to create and add uc
def check_table_exist(cursor, database_name):
    try:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name IN (%s, %s, %s);"""
        , (database_name, 'groups', 'locations', 'users' ))

        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        if 'groups' in tables:
            logger.info("Table 'groups' exists.")
        else:
            create_tables(cursor, sql_file_path=SQL_FILE_PATH, table_name= 'groups')
            prevent_duplicate(cursor, alter_table_path = ALTER_TABLE_PATH,
                               database_name = DB_NAME, table_name = 'groups')
        if 'locations' in tables:
            logger.info("Table 'locations' exists.")
        else:
            create_tables(cursor, sql_file_path=SQL_FILE_PATH, table_name= 'locations')
            prevent_duplicate(cursor, alter_table_path = ALTER_TABLE_PATH,
                               database_name = DB_NAME, table_name = 'locations')
        if 'users' in tables:
            logger.info("Table 'users' exists.")
        else:
            create_tables(cursor, sql_file_path=SQL_FILE_PATH, table_name= 'users')
            prevent_duplicate(cursor, alter_table_path = ALTER_TABLE_PATH,
                               database_name = DB_NAME, table_name = 'users')
        
    
    except err.Error as err:
        logger.info(f"Error: {err}")

def prevent_duplicate(cursor, alter_table_path, database_name, table_name):
    try:
        with open(alter_table_path, 'r') as p:
            sql_statements = p.read().split(';')
            for statement in sql_statements:
                if f'ALTER TABLE {database_name}.{table_name}'.lower() in statement.lower():
                    cursor.execute(statement.strip())
                    logger.info(f"Table {table_name} added unique constraint succesfully")
    except conn.Error as err:
        logger.error(f"alter table {table_name} to add unique constraint failed")

            
# data insert groups column            
def insert_group(cursor, dataframe):
    group_ids = []
    try:
        for index, row in dataframe.iterrows():
            insert_group_query = """
                INSERT INTO employee.groups (group_name, description, creation_date)
                VALUES (%s, %s, CURDATE())
                ON DUPLICATE KEY UPDATE groups.group_id = LAST_INSERT_ID(group_id);

            """
            group_data = (row['group_name'], row['description'])
            cursor.execute(insert_group_query, group_data)
            cursor.execute("SELECT LAST_INSERT_ID()")
            group_id = cursor.fetchone()[0]
            group_ids.append(group_id)
            cnx.commit()  # Commit the transaction
        
    except conn.Error as err:
        logger.error(f"Error inserting group: {err}")
        return None
    else:
        print(group_ids)
        if group_id is not None:
            df_group['group_id'] = group_ids
            return df_group

# insert location data to the location table
def insert_location(cursor, dataframe):
    location_ids = []
    try:
        for index, row in dataframe.iterrows():
            insert_location_query = """
                INSERT INTO employee.locations(location_name, address, city, country, group_id)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE locations.location_id = LAST_INSERT_ID(location_id);

            """
            location_data = (row['location_name'], row['address'] ,row['city'], row['country'],
                          row['group_id'])
            cursor.execute(insert_location_query, location_data)
            cursor.execute("SELECT LAST_INSERT_ID()")
            location_id = cursor.fetchone()[0]
            location_ids.append(location_id)
            cnx.commit()  # Commit the transaction
        
    except conn.Error as err:
        logger.error(f"Error inserting locations: {err}")
        return None
    else:
        print(location_ids)
        if location_ids is not None:
            df_location['location_id'] = location_ids
            return df_location

# Insert user data to user table
def insert_user(cursor, dataframe):
    try:
        for index, row in dataframe.iterrows():
            insert_location_query = """
                INSERT INTO employee.users(user_name, email, phone_number, location_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE users.user_id = LAST_INSERT_ID(user_id);

            """
            user_data = (row['user_name'], row['email'] ,row['phone_number'], row['location_id'])
            cursor.execute(insert_location_query, user_data)
            cnx.commit()  # Commit the transaction
        
    except conn.Error as err:
        logger.error(f"Error inserting users: {err}")
        return None
    else:
        print("completed uploading user details")

if __name__== '__main__':      
    # read incoming data
    df = pd.read_excel(FILE_PATH)

    # column rename
    df.rename(columns={'groupname':'group_name', 'group_description':'description',
                    'locationname':'location_name', 'location_address':'address',
                    }, inplace=True)

    # data cleaning
    df['description'] = df['description'].str.replace(r'[/\[\].,!~-]', '', regex=True).str.strip()
    df['phone_number'] = df['phone_number'].str.replace(r'[()-]', '', regex=True).str.strip()
    df['email'] = df['email'].str.lower().str.strip().str.replace(' ', '')

    # dataframe for group
    df_group = df[['group_name', 'description']]
    df_group = df_group.drop_duplicates()

    try:
        cnx = conn.connect(**CONFIG)
    except conn.Error as err:
        if err.errno == conn.errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with username or password")
        else:
            logger.info(err)
    finally:
        logger.info('Connection to mysql server is successfull')
        cursor = cnx.cursor()

        #database connection
        database_connection(cursor, database_name=DB_NAME)
        check_table_exist(cursor, database_name=DB_NAME)

        #insert group details and return dataframe with group_id
        df_group = insert_group(cursor, dataframe = df_group)

        #merge group_id with main dataframe
        df_merged = pd.merge(df, df_group, on=['group_name', 'description'], how='left')

        df_location = df_merged[['location_name','address', 'city', 'country', 'group_id']]
        

        # insert location details and return dataframe with location_id
        df_location = insert_location(cursor, dataframe = df_location)
        print(df_location)

        #merge location_id with main dataframe
        df_merged = pd.merge(df, df_location, on=['location_name', 'address', 'city', 
                                                'country'], how='left')
        
        df_user = df_merged[['user_name', 'email', 'phone_number', 'location_id']]

        # insert user details to the table
        # print(df_user['phone_number'])
        insert_user(cursor, dataframe = df_user)    

        logger.info("completed the execution")

        cnx.close()

