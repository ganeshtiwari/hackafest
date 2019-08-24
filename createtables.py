from helpers.db import DB

db = DB('aip.db')

# Drop block
try:
    statement = '''
        drop table file_source;
    '''
    db.execute(statement)
    print('file_source dropped')
except Exception as e:
    print(e)

try:
    statement = '''
        drop table file_queue;
    '''
    db.execute(statement)
    print('file_queue dropped')
except Exception as e:
    print(e)

try:
    statement = '''
        drop table file_process_log;
    '''
    db.execute(statement)
    print('file_process_log dropped')
except Exception as e:
    print(e)

# Create block
try:
    statement = '''
        create table file_source (
            dm_fileid int primary key not null, 
            client_id int,
            filename char(255), 
            source char(50)
        );
    '''
    db.execute(statement)
    print('Created table file_source')
except Exception as e:
    print(e)

try:
    statement = '''
            create table file_queue (
                dm_fileid int primary key not null, 
                client_id int,
                filename char(255), 
                source char(50), 
                priority int, 
                worker char(20)
            );
        '''
    db.execute(statement)
    print('created table file_queue')
except Exception as e:
    print(e)


try:
    statement = '''
            create table file_process_log (
                dm_fileid int primary key not null, 
                client_id int,
                filename char(255), 
                source char(50), 
                priority int, 
                worker char(20), 
                start_time date, 
                end_time date
            );
        '''
    db.execute(statement)
    print('created table file_process_log')
except Exception as e:
    print(e)

db.close_connection()
