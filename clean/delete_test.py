import sqlite3 as sl

from tqdm import *


def delete_test(database):
    con = sl.connect(database)
    for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            #print(row[0])
            
            #number of element print()
            one=(con.execute('SELECT COUNT(*) FROM {}'.format(row[0])).fetchall())
            con.execute("DELETE FROM {} WHERE Scope LIKE ?;".format(row[0]), ('%validation%',))
            con.execute("DELETE FROM {} WHERE Scope LIKE ?;".format(row[0]), ('%test%',))
            two=(con.execute('SELECT COUNT(*) FROM {}'.format(row[0])).fetchall())
            if one[0][0] != two[0][0]:
                print('Deleted {} rows from {}'.format(one[0][0]-two[0][0], row[0]))
            if two[0][0] == 0:
                print('Deleting table {}'.format(row[0]))
                con.execute('DROP TABLE {}'.format(row[0]))
            con.commit()
