import sqlite3 as sl

from tqdm import *


def delete_test(database):
    con = sl.connect(database)
    for row in tqdm(con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()):
        if row[0] == 'sqlite_sequence':
            pass
        else: 
            #print(row[0])
            scopes=con.execute('SELECT DISTINCT scope FROM {};'.format(row[0])).fetchall()
            scopes=[x[0] for x in scopes]
            scopes=[x.replace(" ","") for x in scopes]
            for scope in scopes:
                if scope == None:
                    pass
                else:   
                    if 'validation' in scope or 'test' in scope:
                        if len(scopes)==1:
                            #delete that table
                            print("deleting table: {}".format(row[0]))
                            with con:
                                con.execute("DROP TABLE {};".format(row[0]))
                        else:
                            only_good_scopes=([x for x in scopes if 'validation' not in x and 'test' not in x])
                            only_bad_scopes=([x for x in scopes if 'validation' in x or 'test' in x])
                            if len(only_good_scopes)==0:
                                #delete that table
                                print("deleting table: {}".format(row[0]))
                                with con:
                                    con.execute("DROP TABLE {};".format(row[0]))    
                            else:
                                print("deleting scopes from: {}".format(row[0]))
                                for bad_scope in only_bad_scopes:
                                    with con:
                                        con.execute("DELETE FROM {} WHERE scope='{}';".format(row[0],bad_scope))

                                    #"""