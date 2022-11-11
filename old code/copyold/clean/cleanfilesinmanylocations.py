from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from sqlalchemy import column

def fix_problem_location_and_dups(dataset):
    name=dataset
    con = sl.connect('{}'.format(name))

    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            print(row[0])
            all_info={}
            files=con.execute("SELECT file FROM {}".format(row[0])).fetchall()
            files2=list(files)
            files2=[n[0] for n in files2]
            files3=list(set(files2))
            if len(files3) != len(files2):
                        filenmes = []
                        files=con.execute("SELECT * FROM {}".format(row[0])).fetchall()
                        #print(files)
                        for file in files:
                            filename=file[1]
                            if filename not in all_info:
                                all_info[filename]=[]
                                all_info[filename].append((file))
                            else:
                                all_info[filename].append((file))
            #print(all_info)
            too_throw=[]
            too_fix=[]
            locations=[]
            for key in all_info:
                if(len(all_info[key]))!=1:
                    for i in range(len(all_info[key])):
                        if i==0:
                            pass
                        else:
                            too_throw.append(all_info[key][i][0])
                else:
                    location=all_info[key][0][4]
                    if location==None:
                        pass
                    else:
                        if "," in location:
                            too_fix.append(all_info[key][0][0])
                            locations.append(location)
                        if location[-1]=="/":
                            too_throw.append(all_info[key][0][0])
            if len(too_throw)!=0:
                print("Throwing away {} files".format(len(too_throw)))
                print(len(con.execute("SELECT * FROM {}".format(row[0])).fetchall()))
                for i in too_throw:
                    con.execute("DELETE FROM {} WHERE id = {}".format(row[0],i))
                    con.commit()

            if len(too_fix)!=0:
                print("Fixing {} files".format(len(too_fix)))

                for i in range(len(too_fix)):
                    id=too_fix[i]
                    location=locations[i]
                    location=location.split(",")
                    for o in location:
                        if o[-1]=="/":
                            location.remove(o)
                    location=location[0]
                    
                    con.execute("UPDATE {} SET DataLocation = '{}' WHERE id = {}".format(row[0],location,i))
                    con.commit()



