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
postion_duplicate_ult=[]
position_regular_ult=[]

con = sl.connect('duplicate_data_2.db')
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
        
        
        postion_duplicate=[]
        position_regular=[]
        def get_data(column):
            time_1_list=[]
            time_2_list=[]
            time_no_problem=[]
            
            for number in position:
                #print(int(number))
                creation_time = con.execute("""
                SELECT {} FROM {} WHERE file_number LIKE ?;""".format(column,row[0]), (int(number),)).fetchall()
                #print(creation_time)
                if len(creation_time)==0:
                    pass
                else:
                    if len(creation_time)>1:
                       # print(creation_time)
                        time1=creation_time[0][0]
                        time2=creation_time[1][0]
                        postion_duplicate.append(number)
                        if time1>time2:
                            time_1_list.append(time2)
                            time_2_list.append(time1)
                        else:
                            time_1_list.append(time1)
                            time_2_list.append(time2)
                    else:
                        #print(number)
                        position_regular.append(number)
                        time_no_problem.append(creation_time[0][0])
            return time_1_list,time_2_list,time_no_problem

        def get_loacation_data(column,location):
            creation_time_place=con.execute("""
            SELECT {},file_number FROM {} WHERE ComputingElement LIKE '%{}%';""".format(column,row[0],location), ()).fetchall()


            creation_time_place.sort(key=lambda x: x[1])
            creation_time_place_list=[]
            creation_time_place_number_list=[]
            #print(creation_time_place[:5])
            for n in range(len(creation_time_place)):
                creation_time_place_list.append(creation_time_place[n][0])
                
                creation_time_place_number_list.append(creation_time_place[n][1])
            return creation_time_place_list,creation_time_place_number_list

        
        column="JobSubmissionTime"
        time_1_list,time_2_list,time_no_problem=get_data(column)

        creation_time_place_list_lund, creation_time_place_number_lund_list=get_loacation_data(column,'lunarc')
        creation_time_place_list_slac, creation_time_place_number_slac_list=get_loacation_data(column,'slac')
        creation_time_place_list_uscb, creation_time_place_number_uscb_list=get_loacation_data(column,'ucsb')
        
        position_ult.extend(position)
        time_1_list_ult.extend(time_1_list)
        time_2_list_ult.extend(time_2_list)
        time_no_problem_ult_ult.extend(time_no_problem)
        postion_duplicate_ult.extend(postion_duplicate)
        position_regular_ult.extend(position_regular)
        try:
            creation_time_place_list_lund_ult.extend(creation_time_place_list_lund)
            creation_time_place_number_lund_list_ult.extend(creation_time_place_number_lund_list)
        except:
            pass    
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
        

        """
        plt.plot(creation_time_place_list_lund,creation_time_place_number_lund_list,"+",label="Created at Lund", markersize=6)
        plt.plot(creation_time_place_list_slac,creation_time_place_number_slac_list,"*",label="Created at SLAC", markersize=6)
        plt.plot(creation_time_place_list_uscb,creation_time_place_number_uscb_list,"o",label="Created at UCSB", markersize=6)

        plt.plot(time_1_list,postion_duplicate,".",label="Early duplicate", markersize=4,color="black")
        plt.plot(time_2_list,postion_duplicate,".",label="Later duplicate", markersize=4,color="red")
        plt.plot(time_no_problem,position_regular,".",label="Not a duplicate", markersize=4,color="white")
        #plt.grid(linestyle='--',)
        plt.title("{} time of {}\n for early duplicate, late duplicate and not a duplicate file".format(column,row[0]),fontsize=20)
        plt.xlabel('Time',fontsize=15)
        plt.ylabel('File number',   fontsize=15)
        plt.legend(loc='upper left',bbox_to_anchor=(1.05,1),fontsize=15)
        #plt.ylim(0)
        #plt.xlim(8500)
        manager = plt.get_current_fig_manager()
        manager.window.showMaximized()
        #plt.show()
        figure = plt.gcf()
        figure.set_size_inches(19, 10)
        if not os.path.exists("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}".format(column)):
            os.makedirs("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}".format(column))
        plt.savefig("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}\\{}.png".format(column,row[0]),bbox_inches='tight', dpi=100)
        plt.close()"""
"""
print(len(time_1_list_ult))
print(len(postion_duplicate_ult))

print(len(time_2_list_ult))
print(len(postion_duplicate_ult))

print(len(creation_time_place_list_lund_ult))
print(len(creation_time_place_number_lund_list_ult))

print(len(time_no_problem_ult_ult))
print(len(position_regular_ult))
"""


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

x = time_all_2
y = count_all_2

window = 40
average_y = []
for ind in range(len(y) - window + 1):
    average_y.append(np.mean(y[ind:ind+window]))
for ind in range(window - 1):
    average_y.insert(0, np.nan)
plt.plot(creation_time_place_list_lund_ult,creation_time_place_number_lund_list_ult,"+",label="Created at Lund", markersize=6)
plt.plot(creation_time_place_list_slac_ult,creation_time_place_number_slac_list_ult,"*",label="Created at SLAC", markersize=6)
plt.plot(creation_time_place_list_uscb_ult,creation_time_place_number_uscb_list_ult,"p",label="Created at UCSB", markersize=6)

plt.plot(times_exist_1,count_all_1,".",label="Early duplicate", markersize=8,color="black")
plt.plot(times_exist_2,count_all_2,".",label="Later duplicate", markersize=8,color="red")
plt.plot(time_no_problem_ult_ult,position_regular_ult,".",label="Not a duplicate", markersize=4,color="white")
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
"""
figure = plt.gcf()
figure.set_size_inches(19, 10)

if not os.path.exists("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}".format(column)):
    os.makedirs("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}".format(column))
plt.savefig("C:\\Users\\MSI PC\\Desktop\\project\\pictures\\{}\\{}.png".format(column,row[0]),bbox_inches='tight', dpi=100)
plt.close()"""


