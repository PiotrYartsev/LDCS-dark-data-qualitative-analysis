from audioop import add
import os
import time
from datetime import datetime

import sqlite3 as sl
def add_time(database):
    con = sl.connect(database, check_same_thread=False)



    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            print("          "+row[0])
            
            time2=con.execute("Select JobSubmissionTime from %s" % row[0]).fetchall()[0][0]
            try:
                float(time2)
            except:
                
                
                add_file_number=[]


                
                data = con.execute("SELECT JobSubmissionTime FROM {}".format(row[0])).fetchall()
                creation_time_place_list=[]
                if len(data)<1:
                    pass
                else:
                    for rows in data:
                        if rows[0]==None:
                            pass
                        else:
                            rowss=rows[0]
                            while rowss[0]==" ":
                                #print("          "+rows[0][0])
                                rowss=rowss[1:]
                            
                            try:
                                times1=time.mktime(datetime.strptime(rowss, " %Y-%m-%d %H:%M:%S").timetuple())
                            except:
                                times1=time.mktime(datetime.strptime(rowss, "%Y-%m-%d %H:%M:%S").timetuple())
                            creation_time_place_list.append(times1)
                    with con:
                        con.execute("""
                        ALTER TABLE {}
                            DROP COLUMN JobSubmissionTime;""".format(row[0]))

                    with con:
                        con.execute("""
                        ALTER TABLE {}
                            ADD COLUMN JobSubmissionTime;""".format(row[0]))
                        
                
                

                
                




                with con:
                    
                    for n in range(len(creation_time_place_list)):
                        
                        sql="update {} set JobSubmissionTime=({}) where id={};".format(row[0],creation_time_place_list[n],n+1)
                        con.execute(sql)
