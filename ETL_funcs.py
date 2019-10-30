from __future__ import generators
import pyodbc
import decimal
import datetime
import csv
import re


def db_connect(conn_string):
    """
    :Args - DB Connection String
    :Returns - DB connection and cursor object for DB interaction
    """
    cnxn = pyodbc.connect(conn_string)
    cursor = cnxn.cursor()

    return cnxn, cursor


def retrieve_table_names(table_list_path):
    """
    :Args - CSV file path containing list of table names to retrieve
    :Returns - Python iterable list object of table names for skyward db, and one for target db
    """

    with open(table_list_path, 'r', newline = '') as fp:
        csv_reader = csv.reader(fp, delimiter = ',')
        skyward_table_list = [str(row[0]) for row in csv_reader]
        adm_table_list = [table[table.find('PUB.')+4:].replace('"','').replace('-','_')
                          for table in skyward_table_list]

    return skyward_table_list, adm_table_list


def clean_list_items(list_iterable):
    """
    Skyward DB uses hyphens in column names and fields. These must be removed for SQL Server

    :Args - Any iterable containing strings
    :Returns - A cleaned list object with hyphens in strings replace with underscores
    """

    clean_list = list(map(lambda x: x.replace('-', '_')
                                    .replace('\'', '')
                                    .replace('"', ''), list_iterable))

    return clean_list


def retrieve_table_columns(cursor, table_name):
    """
    :Args - DB cursor object, table name in target DB
    :Returns - Zipped list of column names with data types, list of column names
    """

    def convert_data_types(data_types):
        parsed_data_types = []
        for item in data_types:
            if item == str:
                parsed_data_types.append('NVARCHAR(100)')
            elif item == bool:
                parsed_data_types.append('BIT')
            elif item == int:
                parsed_data_types.append('INT')
            elif item == datetime.date:
                parsed_data_types.append('DATE')
            elif item == decimal.Decimal:
                parsed_data_types.append('FLOAT')
            else:
                parsed_data_types.append('NVARCHAR(100)')

        return parsed_data_types

    cursor.execute(f'SELECT TOP 1 * FROM {table_name}')
    column_names = [item[0] for item in cursor.description]
    data_types = [item[1] for item in cursor.description]
    column_list = clean_list_items(column_names)
    zipped_col_list = zip(clean_list_items(column_names), convert_data_types(data_types))

    return zipped_col_list, column_list


def create_table_drop_string(db_name, schema, table_name):
    """
    :Args - Name of database, schema name, name of table in destination DB
    :Returns - An executable sql string to drop table if it exists in destination DB
    """

    drop_string = f"""
    IF OBJECT_ID('{db_name}.{schema}.{table_name}') IS NOT NULL DROP TABLE {db_name}.{schema}.{table_name}
    """

    return drop_string


def create_table_create_string(db_name, schema, table_name, zipped_col_list):
    """
    :Args - Destination DB name, schema name, table name, list of tuples with column names and data types
    :Returns - Executable table creation string for target DB
    """

    create_string = f'CREATE TABLE {db_name}.{schema}.{table_name} (\n'
    for i, pair in enumerate(zipped_col_list):
        if i == 0:
            create_string += f'{pair[0]} {pair[1]}\n'
        else:
            create_string += f',{pair[0]} {pair[1]}\n'
    create_string += ')'

    return create_string


def create_table_insert_string(db_name, schema, table_name, col_list):
    """
    :Args - db name, schema, table name and list of columns for target DB
    :Returns - Executable insert string for target DB
    """

    insert_string = f'INSERT INTO {db_name}.{schema}.{table_name} (\n'
    for i, col in enumerate(col_list):
        if i == 0:
            insert_string += f'{col}\n'
        else:
            insert_string += f',{col}\n'
    insert_string += ') VALUES (\n'
    for i, col in enumerate(col_list):
        if i == 0:
            insert_string += '?\n'
        else:
            insert_string += ',?\n'
    insert_string += ')'

    return insert_string


def create_bulk_insert_string(db_name, schema, table_name, path):
    """
    :Args - Destination table DB name, schema and table name, along with file path to bulk insert from
    :Returns - A bulk insert string to be used in place of a traditional insert to pull from a CSV file
    """

    insert_string = f'BULK INSERT {db_name}.{schema}.{table_name}\n'
    insert_string += f"FROM '{path}'\n"
    insert_string += "WITH ( FIELDTERMINATOR='|');"

    return insert_string


def create_bypass_dict(table_name, bypass_config_path):
    """
    :Args - Name of table currently being worked with, file path to bypass config
    :Returns - Dict of table name keys with bypass column name values
    """

    with open(bypass_config_path, 'r') as fp:
        csv_reader = csv.reader(fp, delimiter = ',')
        bypass_dict = {}
        for row in csv_reader:
            bypass_dict[row[0]] = row[1:]

    return bypass_dict


def clean_params(table_name, bypass_dict, param_list):
    """
    :Args - List of parameters for insertion into SQL DB
    :Returns - Cleaned list of parameters
    """
    clean_data = []
    for tuple in param_list:
        clean_row = []
        for i, param in enumerate(tuple):
            if str(i) in bypass_dict[table_name]:
                param = None
            elif param == '':
                param = None
            elif param == True:
                param = 1
            elif param == False:
                param = 0
            elif param == '\t':
                param = None
            elif isinstance(param, str):
                param = param[:95]
            else:
                param = param
            clean_row.append(param)
        #clean_row = [None if param == '' else param for param in tuple]
        clean_data.append(clean_row)

    return clean_data


def ResultIter(cursor, arraysize):
    """
    :Args - DB Cursor object, array size for batch fetching
    :Returns - Iterable list of table data from source database
    """
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result



### TEST OF FUNCTIONS ###

if __name__ == "__main__":
    conn_string = 'Driver={Progress OpenEdge 11.7 Driver}; HOST=64.132.91.21; DB=SKYWARD; UID=SQLRo2; PWD=4riRMBKQ; PORT=45025;'
    sky_cnxn, sky_cursor = db_connect(conn_string)

    #tables = ['SKYWARD.PUB."STUDENT-EW"']
    sky_tables, adm_tables = retrieve_table_names('C:/Reports/Script Files/Skyward_DB_ETLs/entry_withdrawal_table_list.csv')
    col_tuples, col_names = retrieve_table_columns(sky_cursor, sky_tables[2])
    clean_adm_table_names = clean_list_items(adm_tables)

    sql = f'SELECT * FROM {sky_tables[2]}'
    sky_cursor.execute(sql)
    data = [row for row in sky_cursor.fetchall()]
    clean_data = clean_params(data)
    print(clean_data)


    #col_create_tuple, col_names = retrieve_table_columns(sky_cursor, clean_tables)
    drop_string = create_table_drop_string('rpa', 'sky', clean_adm_table_names[0])
    create_string = create_table_create_string('rpa', 'sky', clean_adm_table_names[0], col_tuples)
    insert_string = create_table_insert_string('rpa', 'sky', clean_adm_table_names[0], col_names)

    bypass_dict = create_bypass_dict('STUDENT', 'C:/Reports/Script Files/Skyward_DB_ETLs/column_bypass_config.csv')
    print(bypass_dict['STUDENT'])
