import sqlite3 as sl

from tqdm import tqdm

#define function
def delete_test_all(database):
    con = sl.connect(database)
    for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
        #skip sqlite_sequence
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            #get a list of all scopes
            valid=con.execute("SELECT * FROM {} WHERE Scope LIKE ?;".format(row[0]), ('%valid%',))
            test=con.execute("SELECT * FROM {} WHERE Scope LIKE ?;".format(row[0]), ('%test%',))
            #if scopes are not empty, delete table
            if valid and test:
                print('Deleting table {}'.format(row[0]))
                con.execute('DROP TABLE {}'.format(row[0]))
            con.commit()