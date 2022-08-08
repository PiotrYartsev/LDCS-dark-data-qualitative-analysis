from tokenize import Number
from matplotlib.axis import YAxis
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator
from datetime import datetime
import os
import sqlite3 as sl
from tqdm import *

from sqlalchemy import column

con = sl.connect('Lund_all_not_missing.db')
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
        time_3_list=[]
        time_4_list=[]
        time_5_list=[]

        postion_duplicate_1=[]
        postion_duplicate_2=[]
        postion_duplicate_3=[]
        postion_duplicate_4=[]
        postion_duplicate_5=[]

        position_regular=[]
        def get_data(column):
            
            max_file_number2 = 10
            if max_file_number2==None:
                pass
            else:
                #print(max_file_number2)
                for n in range(1,max_file_number2+1):
                    
                    creation_time = con.execute("""
                    SELECT {},file_number,duplicate FROM {} WHERE duplicate = {};""".format(column,row[0],n), ()).fetchall()
                    #print(n)
                    #print(len(creation_time))
                    #print(creation_time)
                    if len(creation_time)==0:
                        pass
                    else:
                        for k in range(len(creation_time)):
                    
                            time1=creation_time[k][0]

                            
                            if n==1:
                                postion_duplicate_1.append(creation_time[k][1])
                                time_1_list.append(time1)
                            if n==2:
                                postion_duplicate_2.append(creation_time[k][1])
                                time_2_list.append(time1)
                            if n==3:
                                postion_duplicate_3.append(creation_time[k][1])
                                time_3_list.append(time1)
                            if n==4:
                                postion_duplicate_4.append(creation_time[k][1])
                                time_4_list.append(time1)
                            if n==5:
                                postion_duplicate_5.append(creation_time[k][1])
                                time_5_list.append(time1)
                            
                            
            creation_time2 = con.execute("""
            SELECT {},file_number,duplicate FROM {} WHERE duplicate is NULL;""".format(column,row[0]), ()).fetchall()
            if len(creation_time2)==0:
                    pass
            else:
                #print(len(creation_time2))
                for k in range(len(creation_time2)):
                    position_regular.append(creation_time2[k][1])
                    
                    
                    time_no_problem.append(creation_time2[k][0])
                    
            return time_1_list,time_2_list,time_3_list,time_4_list,time_5_list, time_no_problem,postion_duplicate_1,postion_duplicate_2,postion_duplicate_3,postion_duplicate_4,postion_duplicate_5,position_regular
        
        def get_loacation_data(column,location):
            if location=="Null":
                creation_time_place=con.execute("""
            SELECT {},file_number,ComputingElement FROM {} WHERE ComputingElement is Null;""".format(column,row[0]), ()).fetchall()
                #print(len(creation_time_place))
            else:
                creation_time_place=con.execute("""
            SELECT {},file_number,ComputingElement FROM {} WHERE ComputingElement LIKE '%{}%';""".format(column,row[0],location), ()).fetchall()

            creation_time_place.sort(key=lambda x: x[1])
            creation_time_place_list=[]
            creation_time_place_number_list=[]
            #print(creation_time_place[:1])
            for n in range(len(creation_time_place)):
                creation_time_place_list.append(creation_time_place[n][0])
                
                creation_time_place_number_list.append(creation_time_place[n][1])

            return creation_time_place_list,creation_time_place_number_list
        
        
        column="FileCreationTime"
        ttime_1_list,time_2_list,time_3_list,time_4_list,time_5_list, time_no_problem,postion_duplicate_1,postion_duplicate_2,postion_duplicate_3,postion_duplicate_4,postion_duplicate_5,position_regular=get_data(column)
        #print(len(time_1_list))
        creation_time_place_list_lund, creation_time_place_number_lund_list=get_loacation_data(column,'lunarc')
        creation_time_place_list_slac, creation_time_place_number_slac_list=get_loacation_data(column,'slac')
        creation_time_place_list_uscb, creation_time_place_number_uscb_list=get_loacation_data(column,'ucsb')
        creation_time_place_list_caltech, creation_time_place_number_caltech_list=get_loacation_data(column,'caltech')
        creation_time_place_list_not_at_rucio, creation_time_place_number_not_at_rucio_list=get_loacation_data(column,'Null')


        
        print(len(time_1_list))
        if len(time_1_list)==0:
            pass
        else:
            plt.plot(creation_time_place_list_lund,creation_time_place_number_lund_list,"+",label="Created at Lund", markersize=6)
            plt.plot(creation_time_place_list_slac,creation_time_place_number_slac_list,"*",label="Created at SLAC", markersize=6)
            plt.plot(creation_time_place_list_uscb,creation_time_place_number_uscb_list,"o",label="Created at UCSB", markersize=6)
            plt.plot(creation_time_place_list_caltech,creation_time_place_number_caltech_list,"o",label="Created at CALTECH", markersize=6)
            plt.plot(creation_time_place_list_not_at_rucio,creation_time_place_number_not_at_rucio_list,"o",label="Not in Rucio", markersize=6)


            """
            for (x,y) in zip(time_1_list_ult,postion_duplicate_1_ult):
                            plt.text(x, y, str(1), color="black", fontsize=12)
            for (x,y) in zip(time_2_list_ult,postion_duplicate_2_ult):
                            plt.text(x, y, str(2), color="black", fontsize=12)
            for (x,y) in zip(time_no_problem_ult_ult,position_regular_ult):
                            plt.text(x, y, str(0), color="black", fontsize=12)
            """
            plt.plot(time_1_list,postion_duplicate_1,".",label="1st duplicate", markersize=6,color="black")
            plt.plot(time_2_list,postion_duplicate_2,".",label="2nd duplicates", markersize=6,color="red")
            plt.plot(time_3_list,postion_duplicate_3,".",label="3rd duplicates", markersize=6,color="green")
            plt.plot(time_4_list,postion_duplicate_4,".",label="4th duplicates", markersize=6,color="blue")
            plt.plot(time_5_list,postion_duplicate_5,".",label="5th duplicates", markersize=6,color="purple")

            plt.plot(time_no_problem,position_regular,".",label="Not a duplicate", markersize=6,color="white")
            plt.grid(linestyle='--',)
            plt.title("Submission time of {}\n for early duplicate, late duplicate and not a duplicate file".format(row[0]),fontsize=20)
            plt.xlabel('Time',fontsize=15)
            plt.ylabel('File number',   fontsize=15)
            plt.legend(loc='upper left',bbox_to_anchor=(1,1),fontsize=15)
            #plt.ylim(0)
            #plt.xlim(8500)
            manager = plt.get_current_fig_manager()
            manager.window.showMaximized()
            #plt.show()
            figure = plt.gcf()
            figure.set_size_inches(19, 10)
            if not os.path.exists("C:\\Users\\MSI PC\\Desktop\\project\\pictures2\\{}".format(column)):
                os.makedirs("C:\\Users\\MSI PC\\Desktop\\project\\pictures2\\{}".format(column))
            plt.savefig("C:\\Users\\MSI PC\\Desktop\\project\\pictures2\\{}\\{}.png".format(column,row[0]),bbox_inches='tight', dpi=100)
            plt.close()
            