import sys
sys.path.append('..')
from datetime import datetime, timedelta
import time
from helpers.db import DB


def get_files(db, name):
    # db = DB('pipeline')
    output = None
    query = '''
        select * from pipeline where worker = f{name} order by priority desc limit 1; 
    '''
    try:
        output = db.execute(query)
    except Exception as e:
        print(e)
    
    return output


def process_files(db, files):
    starttime = datetime.now()
    s_time = starttime.strftime('%Y:%m:%d %H:%M:%S')
    insert_log = '''
        insert into process_log values(
            fileid, 
            clientid, 
            filename, 
            source, 
            worker, 
            f{s_time}, 
            endtime
        );
    '''
    try: 
        db.execute(insert_log)
    except Exception as e:
        print(e)
    endtime = starttime + timedelta(hours=2)
    endtime = endtime.strftime('%Y:%m:%d %H:%M:%S')
    update_log = '''
        update process_log set 
        endtime = f{endtime}
        where fileid = fileid
    '''
    db.execute(update_log)



def main():
    db = DB('pipeline')
    name = 'worker01'
    statement = '''
        create table pipeline(
            fileid int, 
            clientid int, 
            filename char(255), 
            source char(50), 
            worker char(20), 
            starttime char(20), 
            endtime char(20),
        );
    '''
    db.execute(statement)
    statement = '''
        insert into pipeline values (
            123, 
            23, 
            'abc.txt', 
            'ftp', 
            f{name}, 
            '12:12:12 1:1:1', 
            '13:12:12 1:1:1'
        );
    '''
    while True:
        db = DB('pipeline')
        files = get_files(db, name) 
        print(files)

if __name__ == '__main__':
    db = DB('data_source')
    try:
        output = db.execute('''
            select * from data_source;
        ''')
        print(output)
    except Exception as e:
        print(e)

    # main()



