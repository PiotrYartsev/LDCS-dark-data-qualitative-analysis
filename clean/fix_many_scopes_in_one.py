from logging import raiseExceptions
from tokenize import Number

from tqdm import *

from subprocess import PIPE, Popen

from zlib import adler32
import sqlite3 as sl

from subprocess import PIPE, Popen




def fix_many_scopes_in_one(dataset):
    con = sl.connect(dataset, check_same_thread=False)

    #Retrive all tables from the database
    all_batches=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    all_batches=[a[0] for a in all_batches]
    for row in all_batches:
        #If thea table is the table of tables, skip it
        if row == 'sqlite_sequence':
            pass
        else:
            scopes=con.execute("Select Scope from {} where Scope is not 'None';".format(row)).fetchall()
            scopes=list(set(scopes))
            scopes=[a[0] for a in scopes]
            if len(scopes)>1:
                print("          "+row)
                print("          "+"More than one scope")
                print("          "+"Scopes are: "+str(scopes)+"\n")
                for scope in scopes:
                    name=row+"_"+scope
                    print("                    "+"Making new table for scope: "+scope)
                    con.execute("""
                            CREATE TABLE {} (
                                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                file TEXT,
                                BatchID TEXT,
                                ComputingElement TEXT,
                                DataLocation TEXT,
                                Scope TEXT,
                                FileCreationTime INTEGER,
                                IsRecon TEXT,
                                JobSubmissionTime INTEGER, 
                                file_number INTEGER,
                                duplicate INTEGER

                            );
                        """.format(name))
                    con.execute("INSERT INTO {} SELECT * FROM {} WHERE Scope = '{}';".format(name,row,scope))
                    con.execute("DELETE FROM {} WHERE Scope = '{}';".format(row,scope))
                    con.commit()
            sheck_if_anything_is_left=con.execute("Select Scope from {} where Scope is not 'None';".format(row)).fetchall()
            if len(sheck_if_anything_is_left)==0:
                print("          "+row)
                print("          "+"All scopes been removed")
                print("          "+"Removing table\n")
                con.execute("DROP TABLE {};".format(row))
fix_many_scopes_in_one('Lund_GRID_all.db')