import sqlite3
from helpers.db import DB

db = DB('test.db')

# create table
try:
    output = db.execute('''
        create table data_source(
                dm_fileid int, 
                file_name char(255), 
                file_source char(50)
            );
    ''')
except Exception as e:
    print(e)

# Insert data
db.execute('''
    insert into data_source values(
        1234, 
        "test.txt", 
        "ftp"
    );
''')

db.close_connection()

# Retrieve data

db = DB('test.db')

statement = '''
    select * from data_source;
'''

output = db.execute(statement)
db.close_connection()

print(output)
