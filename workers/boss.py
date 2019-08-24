import sys
import time

sys.path.append('..')
from helpers.db import DB

workers = ['worker01', 'worker02', 'worker03', 'worker04']


def get_data_from_source(db):
    statement = '''
        select * from file_source a 
        where not exists (
            select 1 from file_process_log b 
            where a.dm_fileid = b.dm_fileid
        );
    '''
    try:
        output = db.execute(statement)
        if output:
            return output.fetchall()
    except Exception as e:
        print(e)


def dispatch_to_queue(db, file):
    statement = f"insert into file_queue values {file}"
    try:
        db.execute(statement)
    except Exception as e:
        print(e)
    else:
        print(f'{file} dispatched to queue')


def run_boss(db, worker_index):
    files = get_data_from_source(db)
    for file in files:
        file = list(file)
        file.append('')
        file.append(workers[worker_index])
        dispatch_to_queue(db, tuple(file))
        worker_index = (worker_index + 1) % len(workers)
    return worker_index


def main():
    current_worker = 0
    while True:
        db = DB('..\\aip.db')
        current_worker = run_boss(db, current_worker)
        print("Sleeping for 1 min")
        db.close_connection()
        time.sleep(60)
        print('Waking...')


if __name__ == '__main__':
    main()
