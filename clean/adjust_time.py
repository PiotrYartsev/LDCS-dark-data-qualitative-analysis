
import sqlite3 as sl
import time
from datetime import datetime
from tqdm import tqdm

#define the function
def add_time(database):
    con = sl.connect(database)
    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        if row[0] == 'sqlite_sequence':
            pass
        else:
            print("          "+row[0])
            
            JobSubmissionTime=con.execute("Select JobSubmissionTime from %s" % row[0]).fetchall()
            #sheck if it is already a number, then it is already converted, so skip
            try:
                float(JobSubmissionTime[0][0])
            except:
                
                creation_time_place_list=[]

                #convert time to seconds since epoch
                for rows in tqdm(JobSubmissionTime):
                    if rows[0]==None:
                        pass
                    else:
                        #remove space in front of time, which sometimes is there due to inconsitent formatting
                        rowss=rows[0]
                        while rowss[0]==" ":
                            #print("          "+rows[0][0])
                            rowss=rowss[1:]
                        #Convert to unix time stamp by using time.mktime in the format Year-Month-Day Hour:Minute:Second
                        try:
                            times1=time.mktime(datetime.strptime(rowss, " %Y-%m-%d %H:%M:%S").timetuple())
                        except:
                            times1=time.mktime(datetime.strptime(rowss, "%Y-%m-%d %H:%M:%S").timetuple())
                        creation_time_place_list.append(times1)
                #delete old column and values and add the new values. Faster than updating the values
                with con:
                    con.execute("""
                    ALTER TABLE {}
                        DROP COLUMN JobSubmissionTime;""".format(row[0]))

                with con:
                    con.execute("""
                    ALTER TABLE {}
                        ADD COLUMN JobSubmissionTime;""".format(row[0]))
                #update the values
                with con:
                    for n in range(len(creation_time_place_list)): 
                        sql="update {} set JobSubmissionTime=({}) where id={};".format(row[0],creation_time_place_list[n],n+1)
                        con.execute(sql)
