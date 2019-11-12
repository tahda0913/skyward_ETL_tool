import configparser
import csv
import os

import sys
from sys import argv

import ETL_funcs
from ETL_funcs import clean_list_items
from ETL_funcs import ResultIter


config = configparser.ConfigParser()
config.read('C:/Reports/Script Files/Skyward_DB_ETLs/config_files/config.ini')

skyward_driver = config['Skyward_DB']['Driver']
skyward_hostname = config['Skyward_DB']['HOST']
skyward_db_name = config['Skyward_DB']['DB']
skyward_uid = config['Skyward_DB']['UID']
skyward_pwd = config['Skyward_DB']['PWD']
skyward_port = config['Skyward_DB']['PORT']
skyward_col_enc = config['Skyward_DB']['ColumnEncryption']

adm_driver = config['ADM_DB']['Driver']
adm_server = config['ADM_DB']['Server']
adm_db_name = config['ADM_DB']['Database']
adm_uid = config['ADM_DB']['Uid']
adm_pwd = config['ADM_DB']['Pwd']

skyward_conn_string = f"""
                      Driver={skyward_driver};
                      HOST={skyward_hostname};
                      DB={skyward_db_name};
                      UID={skyward_uid};
                      PWD={skyward_pwd};
                      PORT={skyward_port};
                      """

adm_conn_string = f"""
                  driver={adm_driver};
                  server={adm_server};
                  database={adm_db_name};
                  Uid={adm_uid};
                  Pwd={adm_pwd};
                  """

if argv[1] == 'student_ew':
    table_list_path = config['Table_Lists']['Entry_Withdrawal']
elif argv[1] == 'courses':
    table_list_path = config['Table_Lists']['Course_Schedule']
elif argv[1] == 'demographics':
    table_list_path = config['Table_Lists']['Demographics']
else:
    print('Error: Table List Variable not recognized')
    sys.exit()

bypass_config_path = 'C:/Reports/Script Files/Skyward_DB_ETLs/config_files/column_bypass_config.csv'

if __name__ == '__main__':
    sky_cnxn, sky_cursor = ETL_funcs.db_connect(skyward_conn_string)
    adm_cnxn, adm_cursor = ETL_funcs.db_connect(adm_conn_string)

    adm_cursor.fast_executemany = True

    sky_tables, adm_tables = ETL_funcs.retrieve_table_names(table_list_path)
    clean_adm_table_names = clean_list_items(adm_tables)

    for i, table in enumerate(sky_tables):
        col_tuples, col_names = ETL_funcs.retrieve_table_columns(sky_cursor, table)
        adm_table_name = clean_adm_table_names[i]
        bypass_dict = ETL_funcs.create_bypass_dict(adm_table_name, bypass_config_path)

        drop_string = ETL_funcs.create_table_drop_string('rpa',
                                                         'sky',
                                                         adm_table_name)

        create_string = ETL_funcs.create_table_create_string('rpa',
                                                             'sky',
                                                             adm_table_name,
                                                             col_tuples)

        insert_string = ETL_funcs.create_bulk_insert_string('rpa',
                                                            'sky',
                                                            adm_table_name,
                                                            'C:/Reports/Script Files/Skyward_DB_ETLs/temp_batch.csv')

        adm_cursor.execute(drop_string)
        adm_cursor.execute(create_string)

        sql_fetch = f'SELECT * FROM {table}'
        row_count_fetch = f'SELECT COUNT(*) FROM {table}'

        sky_cursor.execute(row_count_fetch)
        row_count = sky_cursor.fetchall()

        sky_cursor.execute(sql_fetch)
        batch_count = 1

        while True:
            results = sky_cursor.fetchmany(10000)
            clean_results = ETL_funcs.clean_params(adm_table_name, bypass_dict, results)
            with open('C:/Reports/Script Files/Skyward_DB_ETLs/temp_batch.csv', 'w', newline = '') as fp:
                csv_writer = csv.writer(fp, delimiter = '|')
                csv_writer.writerows(clean_results)
            if not results:
                print('End of Results')
                os.remove('C:/Reports/Script Files/Skyward_DB_ETLs/temp_batch.csv')
                break
            print(f'Inserting batch {batch_count} into table rpa.sky.{adm_table_name}')
            print(f'Table rows {(batch_count * 10000) - 9999} - {batch_count * 10000} out of {row_count[0][0]}')
            adm_cursor.execute(insert_string)
            batch_count += 1

        adm_cnxn.commit()
