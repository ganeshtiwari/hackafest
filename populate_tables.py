from helpers.db import DB
db = DB('aip.db')
try:
    statement = '''
        insert into file_source 
            select 1 as dm_fileid, 100 as client_id, 'file1.txt' as filename, 'ftp' as source
        union all
            select 2 as col1, 100 as col2, 'file2.txt' as col3, 'ftp' as col4
        union all 
            select 3 as col1, 100 as col2, 'file3.txt' as col3, 'ftp' as col4
        union all
            select 4 as col1, 200 as col2, 'file4.txt' as col3, 'ftp' as col4 
        union all 
            select 5 as col1, 200 as col2, 'file5.txt' as col3, 'ftp' as col4 
        union all 
            select 6 as col1, 200 as col2, 'file6.txt' as col3, 'ftp' as col4 
        union all 
            select 7 as col1, 200 as col2, 'file7.txt' as col4, 'ftp' as col5 
        union all 
            select 8 as col1, 300 as col2, 'file8.txt' as col3, 'ftp' as col4 
        union all 
            select 9 as col1, 300 as col2, 'file9.txt' as col3, 'ftp' as col4 
        union all 
            select 10 as col1, 300 as col2, 'file10.txt' as col3, 'ftp' col4 
        union all 
            select 11 as col1, 300 as col2, 'file11.txt' as col3, 'ftp' as col4 
        union all 
            select 12 as col1, 300 as col2, 'file12.txt' as col3, 'ftp' as col4
        union all 
            select 13 as col1, 300 as col2, 'file13.txt' as col3, 'ftp' as col4 
        union all 
            select 14 as col1, 300 as col2, 'file14.txt' as col3, 'ftp' as col4 
        union all 
            select 16 as col1, 400 as col2, 'file16.txt' as col3, 'ftp' as col4
        union all 
            select 17 as col1, 500 as col2, 'file17.txt' as col3, 'ftp' as col4;
    '''
    db.execute(statement)
    print('Populated table file_source')
except Exception as e:
    print(e)
db.close_connection()
