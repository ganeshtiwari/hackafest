import sys
import time
import os
from datetime import datetime
from datetime import timedelta
from importlib import import_module
import configparser
sys.path.append('..')
from helpers.db import DB
from process.processfile import process_file

parser = configparser.ConfigParser()

# TODO: Fetch data from queue assigned to me (only one file)
# TODO: Insert into the file_process_log
# TODO: Begin processing file
NAME = 'worker01'


def fetch_from_file_queue(db):
    """
    :param db: Connection to aip.db
    :return: list of a tuple of single file
    """
    statement = f'''
        select * from file_queue where worker = '{NAME}'
            order by priority asc limit 1;
    '''
    print(statement)
    try:
        output = db.execute(statement).fetchall()
        return output
    except Exception as e:
        print(e)


def delete_from_file_queue(db, dm_fileid):
    """
    Deletes the record from file_queue
    :param db: Connection to aip.db
    :param dm_fileid:  Unique file identifier in file_queue
    """
    delete_statement = f'''
        delete from file_queue
        where dm_fileid = {dm_fileid}
    '''
    try:
        db.execute(delete_statement)
    except Exception as e:
        print(e)


def insert_to_process_log(db, file):
    """
    :param db: Connection to aip.db
    :param file: file record from file_queue
    """
    file = list(file)
    file.append('')
    file.append('')
    file = tuple(file)
    insert_statement = f'''
        insert into file_process_log 
        values {file}
    '''
    try:
        db.execute(insert_statement)
    except Exception as e:
        print(e)


def update_file_process_log(db, field_name, field_value, where_condition):
    """
    :param db: Connection to aip.db
    :param field_name: table field name
    :param field_value: field value
    :param where_condition: update condition
    """
    update_statement = f'''
        update file_process_log set 
            {field_name} = '{field_value}'
        where {where_condition}
    '''
    try:
        db.execute(update_statement)
    except Exception as e:
        print(e)


def main():
    """
    Continuously run worker.
    Get single file assigned to this worker and send it to processing.
    """
    while True:
        db = DB('..\\aip.db')
        file = fetch_from_file_queue(db)
        if file:
            file = file[0]
            dm_fileid = file[0]
            client_id = file[1]
            print('Inserting to file_process_log')
            insert_to_process_log(db, file)
            print('Deleting from file queue')
            delete_from_file_queue(db, dm_fileid)
            print('Starting file processing')
            start_time = datetime.now().strftime('%Y:%m:%d %H:%M:%S')
            print(start_time)
            update_condition = f'dm_fileid = {dm_fileid}'
            update_file_process_log(db, 'start_time', start_time, update_condition)
            client_config_path = f'../config/client{client_id}.ini'
            if os.path.exists(client_config_path):
                parser.read(client_config_path)
                try:
                    client_process_script = parser.get('ACTIONS', 'process_file')
                    client_process_script = import_module(f'process.{client_process_script}')
                    client_process_script.process_file(db, dm_fileid)
                except Exception as e:
                    file.append('')
                    print(e)
            else:
                process_file(db, dm_fileid)
            end_time = (datetime.now() + timedelta(hours=2)).strftime('%Y:%m:%d :%H:%M:%S')
            update_file_process_log(db, 'end_time', end_time, update_condition)
        else:
            print(f'{NAME} could not fetch file from file_queue')

        print(f'{NAME} time to sleep for one minute')
        db.close_connection()
        time.sleep(60)
        print(f'{NAME} wake up time')


if __name__ == '__main__':
    main()