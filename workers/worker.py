import sys
from datetime import datetime
from datetime import timedelta

sys.path.append('..')
from helpers.db import DB

# TODO: Fetch data from queue assigned to me
# TODO: Insert into the file_process_log
# TODO: Begin processing file
NAME = 'worker01'


def fetch_from_file_queue(db):
    statement = f'''
        select * from file_queue where worker = '{NAME}'
            order by priority desc limit 1;
    '''
    print(statement)
    try:
        output = db.execute(statement).fetchall()
        print(output)
        if output:
            start_time = datetime.now()
            output = list(output[0])
            # print(output)
            dm_fileid = output[0]
            output.append(start_time.strftime('%Y:%m:%d %M:%H:%S'))
            output.append('')
            insert_statement = f'''
                insert into file_process_log values {tuple(output)}
            '''
            db.execute(insert_statement)
            print('Inserted to process_file_log')
            print('Deleting same from file_queue')
            delete_statement = f'''
                delete from file_queue where dm_fileid = {dm_fileid}
            '''
            db.execute(delete_statement)
        #  process_file(dm_fileid) # TODO: Make a process that process the file using the dmfileid and update endtime in processlog
    except Exception as e:
        print(e)


def delete_frm_file_queue(dm_fileid):
    pass


fetch_from_file_queue(DB('..\\aip.db'))
