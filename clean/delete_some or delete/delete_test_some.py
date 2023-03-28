import sqlite3 as sl

from tqdm import *


def delete_test_some(database):
    con = sl.connect(database)
    for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            print(row[0])
            
            #number of element print()
            one=(con.execute('SELECT COUNT(*) FROM {}'.format(row[0])).fetchall())
            validation=(con.execute('SELECT COUNT(*) FROM {} WHERE Scope LIKE ?;'.format(row[0]),('%validation%',)).fetchall())
            test=(con.execute('SELECT COUNT(*) FROM {} WHERE Scope LIKE ?;'.format(row[0]),('%test%',)).fetchall())
            none=(con.execute('SELECT COUNT(*) FROM {} WHERE Scope LIKE ?;'.format(row[0]),('%none%',)).fetchall())
            none=[x[0] for x in none]
            one=[x[0] for x in one]
            validation=[x[0] for x in validation]
            test=[x[0] for x in test]
            if one[0]==(validation[0]+test[0]+none[0]):
                print('Deleting table {}'.format(row[0]))
                con.execute('DROP TABLE {}'.format(row[0]))
            con.commit()
delete_test_some('/home/piotr/Desktop/LDCS-dark-data-qualitative-analysis/datasets/all/Lund_GRID_delete_all.db')