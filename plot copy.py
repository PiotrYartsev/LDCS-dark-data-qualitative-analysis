from ast import Break
from itertools import count
from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl

from sqlalchemy import column
position_ult=[]
#column="FileCreationTime"

time_no_problem_ult_ult=[]

creation_time_place_list_lund_ult=[]
creation_time_place_number_lund_list_ult=[]
creation_time_place_list_slac_ult=[]
creation_time_place_number_slac_list_ult=[]

creation_time_place_list_uscb_ult=[]
creation_time_place_number_uscb_list_ult=[]

time_1_list_ult=[]
time_2_list_ult=[]
postion_duplicate_1_ult=[]
postion_duplicate_2_ult=[]

position_regular_ult=[]

con = sl.connect('C:\\Users\\MSI PC\\Desktop\\gitproj\\LDCS-dark-data-qualitative-analysis\\duplicate_data.db')
for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        print(row[0])
        max_file_number = con.execute("""
        SELECT MAX(file_number) FROM {};""".format(row[0])).fetchone()[0]
        min_file_number = con.execute("""
        SELECT MIN(file_number) FROM {};""".format(row[0])).fetchone()[0]
        #print(max_file_number)
        position=list(range(min_file_number,max_file_number+1))
        time_no_problem=[]
        time_1_list=[]
        time_2_list=[]
        postion_duplicate_1=[]
        postion_duplicate_2=[]
        position_regular=[]
        def get_data(column):
            
            max_file_number2 = 5
            if max_file_number2==None:
                pass
            else:
                #print(max_file_number2)
                for n in range(1,max_file_number2+1):
                    print(n)
                    creation_time = con.execute("""
                    SELECT {},file_number,duplicate FROM {} WHERE duplicate = {};""".format(column,row[0],n), ()).fetchall()
                    print(len(creation_time))
                    print(creation_time)
                    if len(creation_time)==0:
                        pass
                    else:
                        for k in range(len(creation_time)):
                    
                            time1=creation_time[k][0]

                            
                            if n==1:
                                postion_duplicate_1.append(creation_time[k][1])
                                time_1_list.append(time1)
                            else:
                                postion_duplicate_2.append(creation_time[k][1])
                                time_2_list.append(time1)
            creation_time2 = con.execute("""
            SELECT {},file_number,duplicate FROM {} WHERE duplicate is NULL;""".format(column,row[0]), ()).fetchall()
            if len(creation_time2)==0:
                    pass
            else:
                #rint(creation_time2)
                for k in range(len(creation_time2)):
                    position_regular.append(creation_time2[k][1])
                    
                    
                    time_no_problem.append(creation_time2[k][0])
                    
            return time_1_list,time_2_list,time_no_problem,postion_duplicate_1,postion_duplicate_2,position_regular
                    
            

        def get_loacation_data(column,location):
            creation_time_place=con.execute("""
            SELECT {},file_number,ComputingElement FROM {} WHERE ComputingElement LIKE '%{}%';""".format(column,row[0],location), ()).fetchall()

            creation_time_place.sort(key=lambda x: x[1])
            creation_time_place_list=[]
            creation_time_place_number_list=[]
            #print(creation_time_place[:5])
            for n in range(len(creation_time_place)):
                creation_time_place_list.append(creation_time_place[n][0])
                
                creation_time_place_number_list.append(creation_time_place[n][1])

            return creation_time_place_list,creation_time_place_number_list

        
        column="FileCreationTime"
        time_1_list,time_2_list,time_no_problem,postion_duplicate_1,postion_duplicate_2,position_regular=get_data(column)
        #print(len(time_1_list))
        creation_time_place_list_lund, creation_time_place_number_lund_list=get_loacation_data(column,'lunarc')
        creation_time_place_list_slac, creation_time_place_number_slac_list=get_loacation_data(column,'slac')
        creation_time_place_list_uscb, creation_time_place_number_uscb_list=get_loacation_data(column,'ucsb')
        
        position_ult.extend(position)
        time_1_list_ult.extend(time_1_list)
        time_2_list_ult.extend(time_2_list)
        time_no_problem_ult_ult.extend(time_no_problem)
        postion_duplicate_1_ult.extend(postion_duplicate_1)
        postion_duplicate_2_ult.extend(postion_duplicate_2)
        position_regular_ult.extend(position_regular)
        
        try:
            creation_time_place_list_lund_ult.extend(creation_time_place_list_lund)
            creation_time_place_number_lund_list_ult.extend(creation_time_place_number_lund_list)
        except:
            pass    
        try:
            creation_time_place_list_slac_ult.extend(creation_time_place_list_slac)
            creation_time_place_number_slac_list_ult.extend(creation_time_place_number_slac_list)
        except:
            pass
        try:
            creation_time_place_list_uscb_ult.extend(creation_time_place_list_uscb)
            creation_time_place_number_uscb_list_ult.extend(creation_time_place_number_uscb_list)
        except:
            pass
        break        
print(len(creation_time_place_list_uscb_ult)+len(creation_time_place_list_slac_ult)+len(creation_time_place_list_lund_ult))



#print(len(creation_time_place_list_uscb_ult))
#print(len(creation_time_place_list_slac_ult))
#print(len(creation_time_place_list_lund_ult))

print(len(time_1_list_ult)+len(time_2_list_ult)+len(time_no_problem_ult_ult))

print(len(time_1_list))
print(1561)

print(len(time_2_list))
print(4899-1561-91)

len(time_no_problem)
print(91)
import collections
#print(time_no_problem_ult_ult[:5])

#print([item for item, count in collections.Counter(position_regular_ult).items() if count > 1])
#print(len(postion_duplicate_ult))
#print(len(position_regular_ult))
#print(len(list(set(position_regular_ult))))
time_all_1=[]
time_all_2=[]
count_all_1=[]
count_all_2=[]

times_exist_1=list(set(time_1_list_ult))

times_exist_1=sorted(times_exist_1)


times_exist_2=list(set(time_2_list_ult))

times_exist_2=sorted(times_exist_2)
for time in times_exist_1:
    count_all_1.append(time_1_list_ult.count(time))
    time_all_1.append(time)
for time in times_exist_2:
    count_all_2.append(time_2_list_ult.count(time))
    time_all_2.append(time)


plt.plot(creation_time_place_list_lund_ult,creation_time_place_number_lund_list_ult,"+",label="Created at Lund", markersize=10)
plt.plot(creation_time_place_list_slac_ult,creation_time_place_number_slac_list_ult,"*",label="Created at SLAC", markersize=10)
plt.plot(creation_time_place_list_uscb_ult,creation_time_place_number_uscb_list_ult,"s",label="Created at UCSB", markersize=10)

plt.plot(time_1_list_ult,postion_duplicate_1_ult,".",label="Early duplicate", markersize=6,color="black")
plt.plot(time_2_list_ult,postion_duplicate_2_ult,".",label="Later duplicate", markersize=6,color="red")
plt.plot(time_no_problem_ult_ult,position_regular_ult,".",label="Not a duplicate", markersize=6,color="white")
#plt.grid(linestyle='--',)
plt.title("{} of {}\n for early duplicate, late duplicate and not a duplicate file".format(column,row[0]),fontsize=20)
plt.xlabel('Time',fontsize=15)
plt.ylabel('File number',   fontsize=15)
plt.legend(loc='upper left',bbox_to_anchor=(1.05,1),fontsize=15)
#plt.ylim(0)
#plt.xlim(8500)
manager = plt.get_current_fig_manager()
manager.window.showMaximized()
plt.show()
