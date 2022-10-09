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
name='SLAC_mc20_2.db'
con = sl.connect(name)
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
        time_6_list=[]
        time_7_list=[]
        time_8_list=[]
        time_9_list=[]
        time_10_list=[]
        time_11_list=[]
        time_12_list=[]
        time_13_list=[]
        time_14_list=[]
        time_15_list=[]
        time_16_list=[]
        time_17_list=[]
        time_18_list=[]
        time_19_list=[]
        time_20_list=[]
        time_21_list=[]
        time_22_list=[]
        time_23_list=[]
        time_24_list=[]
        time_25_list=[]
        time_26_list=[]
        time_27_list=[]
        time_28_list=[]
        time_29_list=[]
        time_30_list=[]
        time_31_list=[]
        time_32_list=[]
        time_33_list=[]
        time_34_list=[]
        time_35_list=[]
        time_36_list=[]
        time_37_list=[]
        time_38_list=[]
        time_39_list=[]
        time_40_list=[]
        time_41_list=[]
        time_42_list=[]
        time_43_list=[]
        time_44_list=[]
        time_45_list=[]
        time_46_list=[]
        time_47_list=[]




        postion_duplicate_1=[]
        postion_duplicate_2=[]
        postion_duplicate_3=[]
        postion_duplicate_4=[]
        postion_duplicate_5=[]
        postion_duplicate_6=[]
        postion_duplicate_7=[]
        postion_duplicate_8=[]
        postion_duplicate_9=[]
        postion_duplicate_10=[]
        postion_duplicate_11=[]
        postion_duplicate_12=[]
        postion_duplicate_13=[]
        postion_duplicate_14=[]
        postion_duplicate_15=[]
        postion_duplicate_16=[]
        postion_duplicate_17=[]
        postion_duplicate_18=[]
        postion_duplicate_19=[]
        postion_duplicate_20=[]
        postion_duplicate_21=[]
        postion_duplicate_22=[]
        postion_duplicate_23=[]
        postion_duplicate_24=[]
        postion_duplicate_25=[]
        postion_duplicate_26=[]
        postion_duplicate_27=[]
        postion_duplicate_28=[]
        postion_duplicate_29=[]
        postion_duplicate_30=[]
        postion_duplicate_31=[]
        postion_duplicate_32=[]
        postion_duplicate_33=[]
        postion_duplicate_34=[]
        postion_duplicate_35=[]
        postion_duplicate_36=[]
        postion_duplicate_37=[]
        postion_duplicate_38=[]
        postion_duplicate_39=[]
        postion_duplicate_40=[]
        postion_duplicate_41=[]
        postion_duplicate_42=[]
        postion_duplicate_43=[]
        postion_duplicate_44=[]
        postion_duplicate_45=[]
        postion_duplicate_46=[]
        postion_duplicate_47=[]

        
        


        

        position_regular=[]
        def get_data(column):
            #get max file number for each table
            max_file_number2 = con.execute('SELECT MAX(duplicate) FROM {}'.format(row[0])).fetchone()[0]
            if max_file_number2==None:
                pass
            else:
                #print(max_file_number2)
                for n in range(1,max_file_number2+1):
                    
                    creation_time = con.execute("""
                    SELECT {},file_number,duplicate FROM {} WHERE duplicate = {};""".format(column,row[0],n), ()).fetchall()

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
                            if n==6:
                                postion_duplicate_6.append(creation_time[k][1])
                                time_6_list.append(time1)
                            if n==7:
                                postion_duplicate_7.append(creation_time[k][1])
                                time_7_list.append(time1)
                            if n==8:
                                postion_duplicate_8.append(creation_time[k][1])
                                time_8_list.append(time1)
                            if n==9:
                                postion_duplicate_9.append(creation_time[k][1])
                                time_9_list.append(time1)
                            if n==10:
                                postion_duplicate_10.append(creation_time[k][1])
                                time_10_list.append(time1)
                            if n==11:
                                #print("11")
                                postion_duplicate_11.append(creation_time[k][1])
                                time_11_list.append(time1)
                            if n==12:
                                #print("12")
                                postion_duplicate_12.append(creation_time[k][1])
                                time_12_list.append(time1)
                            if n==13:
                                postion_duplicate_13.append(creation_time[k][1])
                                time_13_list.append(time1)
                            if n==14:
                                postion_duplicate_14.append(creation_time[k][1])
                                time_14_list.append(time1)
                            if n==15:
                                postion_duplicate_15.append(creation_time[k][1])
                                time_15_list.append(time1)
                            if n==16:
                                postion_duplicate_16.append(creation_time[k][1])
                                time_16_list.append(time1)
                            if n==17:
                                postion_duplicate_17.append(creation_time[k][1])
                                time_17_list.append(time1)
                            
                            if n==18:
                                postion_duplicate_18.append(creation_time[k][1])
                                time_18_list.append(time1)
                            if n==19:
                                postion_duplicate_19.append(creation_time[k][1])
                                time_19_list.append(time1)
                            if n==20:
                                postion_duplicate_20.append(creation_time[k][1])
                                time_20_list.append(time1)
                            if n==21:
                                postion_duplicate_21.append(creation_time[k][1])
                                time_21_list.append(time1)
                            if n==22:
                                postion_duplicate_22.append(creation_time[k][1])
                                time_22_list.append(time1)
                            if n==23:
                                postion_duplicate_23.append(creation_time[k][1])
                                time_23_list.append(time1)
                            if n==24:
                                postion_duplicate_24.append(creation_time[k][1])
                                time_24_list.append(time1)
                            if n==25:
                                postion_duplicate_25.append(creation_time[k][1])
                                time_25_list.append(time1)
                            if n==26:
                                postion_duplicate_26.append(creation_time[k][1])
                                time_26_list.append(time1)
                            if n==27:
                                postion_duplicate_27.append(creation_time[k][1])
                                time_27_list.append(time1)
                            if n==28:
                                postion_duplicate_28.append(creation_time[k][1])
                                time_28_list.append(time1)
                            if n==29:
                                postion_duplicate_29.append(creation_time[k][1])
                                time_29_list.append(time1)
                            if n==30:
                                postion_duplicate_30.append(creation_time[k][1])
                                time_30_list.append(time1)
                            if n==31:
                                postion_duplicate_31.append(creation_time[k][1])
                                time_31_list.append(time1)
                            if n==32:
                                postion_duplicate_32.append(creation_time[k][1])
                                time_32_list.append(time1)
                            if n==33:
                                postion_duplicate_33.append(creation_time[k][1])
                                time_33_list.append(time1)
                            if n==34:
                                postion_duplicate_34.append(creation_time[k][1])
                                time_34_list.append(time1)
                            if n==35:
                                postion_duplicate_35.append(creation_time[k][1])
                                time_35_list.append(time1)
                            if n==36:
                                postion_duplicate_36.append(creation_time[k][1])
                                time_36_list.append(time1)
                            if n==37:
                                postion_duplicate_37.append(creation_time[k][1])
                                time_37_list.append(time1)
                            if n==38:
                                postion_duplicate_38.append(creation_time[k][1])
                                time_38_list.append(time1)
                            if n==39:
                                postion_duplicate_39.append(creation_time[k][1])
                                time_39_list.append(time1)
                            if n==40:
                                postion_duplicate_40.append(creation_time[k][1])
                                time_40_list.append(time1)
                            if n==41:
                                postion_duplicate_41.append(creation_time[k][1])
                                time_41_list.append(time1)
                            if n==42:
                                postion_duplicate_42.append(creation_time[k][1])
                                time_42_list.append(time1)
                            if n==43:
                                postion_duplicate_43.append(creation_time[k][1])
                                time_43_list.append(time1)
                            if n==44:
                                postion_duplicate_44.append(creation_time[k][1])
                                time_44_list.append(time1)
                            if n==45:
                                postion_duplicate_45.append(creation_time[k][1])
                                time_45_list.append(time1)
                            if n==46:
                                postion_duplicate_46.append(creation_time[k][1])
                                time_46_list.append(time1)
                            if n==47:
                                postion_duplicate_47.append(creation_time[k][1])
                                time_47_list.append(time1)
                            

                                

                            
                            
                            
            creation_time2 = con.execute("""
            SELECT {},file_number,duplicate FROM {} WHERE duplicate is NULL;""".format(column,row[0]), ()).fetchall()
            if len(creation_time2)==0:
                    pass
            else:
                #print(len(creation_time2))
                for k in range(len(creation_time2)):
                    position_regular.append(creation_time2[k][1])
                    
                    
                    time_no_problem.append(creation_time2[k][0])
                    
            return time_1_list,time_2_list,time_3_list,time_4_list,time_5_list,time_6_list,time_7_list,time_8_list,time_9_list,time_10_list,time_11_list,time_12_list,time_13_list,time_14_list,time_15_list,time_16_list,time_no_problem,postion_duplicate_1,postion_duplicate_2,postion_duplicate_3,postion_duplicate_4,postion_duplicate_5, postion_duplicate_6,postion_duplicate_7,postion_duplicate_8,postion_duplicate_9,postion_duplicate_10,postion_duplicate_11,postion_duplicate_12,postion_duplicate_13,postion_duplicate_14,postion_duplicate_15,postion_duplicate_16,postion_duplicate_17,postion_duplicate_18,postion_duplicate_19,postion_duplicate_20,postion_duplicate_21,postion_duplicate_22,postion_duplicate_23,postion_duplicate_24,postion_duplicate_25,postion_duplicate_26,postion_duplicate_27,postion_duplicate_28,postion_duplicate_29,postion_duplicate_30,postion_duplicate_31,postion_duplicate_32,postion_duplicate_33,postion_duplicate_34,postion_duplicate_35,postion_duplicate_36,postion_duplicate_37,postion_duplicate_38,postion_duplicate_39,postion_duplicate_40,postion_duplicate_41,postion_duplicate_42,postion_duplicate_43,postion_duplicate_44,postion_duplicate_45,postion_duplicate_46,postion_duplicate_47,position_regular,time_17_list,time_18_list,time_19_list,time_20_list,time_21_list,time_22_list,time_23_list,time_24_list,time_25_list,time_26_list,time_27_list,time_28_list,time_29_list,time_30_list,time_31_list,time_32_list,time_33_list,time_34_list,time_35_list,time_36_list,time_37_list,time_38_list,time_39_list,time_40_list,time_41_list,time_42_list,time_43_list,time_44_list,time_45_list,time_46_list,time_47_list,position_regular

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
        time_1_list,time_2_list,time_3_list,time_4_list,time_5_list,time_6_list,time_7_list,time_8_list,time_9_list,time_10_list,time_11_list,time_12_list,time_13_list,time_14_list,time_15_list,time_16_list,time_no_problem,postion_duplicate_1,postion_duplicate_2,postion_duplicate_3,postion_duplicate_4,postion_duplicate_5, postion_duplicate_6,postion_duplicate_7,postion_duplicate_8,postion_duplicate_9,postion_duplicate_10,postion_duplicate_11,postion_duplicate_12,postion_duplicate_13,postion_duplicate_14,postion_duplicate_15,postion_duplicate_16,postion_duplicate_17,postion_duplicate_18,postion_duplicate_19,postion_duplicate_20,postion_duplicate_21,postion_duplicate_22,postion_duplicate_23,postion_duplicate_24,postion_duplicate_25,postion_duplicate_26,postion_duplicate_27,postion_duplicate_28,postion_duplicate_29,postion_duplicate_30,postion_duplicate_31,postion_duplicate_32,postion_duplicate_33,postion_duplicate_34,postion_duplicate_35,postion_duplicate_36,postion_duplicate_37,postion_duplicate_38,postion_duplicate_39,postion_duplicate_40,postion_duplicate_41,postion_duplicate_42,postion_duplicate_43,postion_duplicate_44,postion_duplicate_45,postion_duplicate_46,postion_duplicate_47,position_regular,time_17_list,time_18_list,time_19_list,time_20_list,time_21_list,time_22_list,time_23_list,time_24_list,time_25_list,time_26_list,time_27_list,time_28_list,time_29_list,time_30_list,time_31_list,time_32_list,time_33_list,time_34_list,time_35_list,time_36_list,time_37_list,time_38_list,time_39_list,time_40_list,time_41_list,time_42_list,time_43_list,time_44_list,time_45_list,time_46_list,time_47_list,position_regular=get_data(column)
        #print(len(time_1_list))
        creation_time_place_list_lund, creation_time_place_number_lund_list=get_loacation_data(column,'lunarc')
        creation_time_place_list_slac, creation_time_place_number_slac_list=get_loacation_data(column,'slac')
        creation_time_place_list_uscb, creation_time_place_number_uscb_list=get_loacation_data(column,'ucsb')
        creation_time_place_list_caltech, creation_time_place_number_caltech_list=get_loacation_data(column,'caltech')
        creation_time_place_list_not_at_rucio, creation_time_place_number_not_at_rucio_list=get_loacation_data(column,'Null')

        """
        print("Time no problem",len(time_no_problem))
        print("Time 1",len(time_1_list))
        print("Time 2",len(time_2_list))
        print("Time 3",len(time_3_list))
        print("Time 4",len(time_4_list))
        print("Time 5",len(time_5_list))"""


        if len(time_1_list)==0:
            pass
        else:
            plt.plot(creation_time_place_list_lund,creation_time_place_number_lund_list,"+",label="Created at Lund", markersize=20)
            plt.plot(creation_time_place_list_slac,creation_time_place_number_slac_list,"+",label="Created at SLAC", markersize=20)
            plt.plot(creation_time_place_list_uscb,creation_time_place_number_uscb_list,"+",label="Created at UCSB", markersize=20)
            plt.plot(creation_time_place_list_caltech,creation_time_place_number_caltech_list,"+",label="Created at CALTECH", markersize=20)
            plt.plot(creation_time_place_list_not_at_rucio,creation_time_place_number_not_at_rucio_list,"o",label="Not in Rucio", markersize=12)


            """
            for (x,y) in zip(time_1_list_ult,postion_duplicate_1_ult):
                            plt.text(x, y, str(1), color="black", fontsize=12)
            for (x,y) in zip(time_2_list_ult,postion_duplicate_2_ult):
                            plt.text(x, y, str(2), color="black", fontsize=12)
            for (x,y) in zip(time_no_problem_ult_ult,position_regular_ult):
                            plt.text(x, y, str(0), color="black", fontsize=12)"""
            if len(time_1_list)!=0:
                plt.plot(time_1_list,postion_duplicate_1,".",label="1st duplicate", markersize=10)
            if len(time_2_list)!=0:
                plt.plot(time_2_list,postion_duplicate_2,".",label="2nd duplicates", markersize=10)
            if len(time_3_list)!=0:
                plt.plot(time_3_list,postion_duplicate_3,".",label="3rd duplicates", markersize=10)
            if len(time_4_list)!=0:
                plt.plot(time_4_list,postion_duplicate_4,".",label="4th duplicates", markersize=10)
            if len(time_5_list)!=0:
                plt.plot(time_5_list,postion_duplicate_5,".",label="5th duplicates", markersize=10)
            if len(time_6_list)!=0:
                plt.plot(time_6_list,postion_duplicate_6,".",label="6th duplicates", markersize=10)
            if len(time_7_list)!=0:
                plt.plot(time_7_list,postion_duplicate_7,".",label="7th duplicates", markersize=10)
            if len(time_8_list)!=0:
                plt.plot(time_8_list,postion_duplicate_8,".",label="8th duplicates", markersize=10)
            if len(time_9_list)!=0:
                plt.plot(time_9_list,postion_duplicate_9,".",label="9th duplicates", markersize=10)
            if len(time_10_list)!=0:
                plt.plot(time_10_list,postion_duplicate_10,".",label="10th duplicates", markersize=10)
            #print(len(time_11_list))
            if len(time_11_list)!=0:
                plt.plot(time_11_list,postion_duplicate_11,".",label="11th duplicates", markersize=10)
            #print(len(time_12_list))
            if len(time_12_list)!=0:
                plt.plot(time_12_list,postion_duplicate_12,".",label="12th duplicates", markersize=10)
            if len(time_13_list)!=0:
                plt.plot(time_13_list,postion_duplicate_13,".",label="13th duplicates", markersize=10)
            if len(time_14_list)!=0:
                plt.plot(time_14_list,postion_duplicate_14,".",label="14th duplicates", markersize=10)
            if len(time_15_list)!=0:
                plt.plot(time_15_list,postion_duplicate_15,".",label="15th duplicates", markersize=10)
            if len(time_16_list)!=0:
                plt.plot(time_16_list,postion_duplicate_16,".",label="16th duplicates", markersize=10)
            if len(time_17_list)!=0:
                plt.plot(time_17_list,postion_duplicate_17,".",label="17th duplicates", markersize=10)
            if len(time_18_list)!=0:
                plt.plot(time_18_list,postion_duplicate_18,".",label="18th duplicates", markersize=10)
            if len(time_19_list)!=0:
                plt.plot(time_19_list,postion_duplicate_19,".",label="19th duplicates", markersize=10)
            if len(time_20_list)!=0:
                plt.plot(time_20_list,postion_duplicate_20,".",label="20th duplicates", markersize=10)
            if len(time_21_list)!=0:
                plt.plot(time_21_list,postion_duplicate_21,".",label="21th duplicates", markersize=10)
            if len(time_22_list)!=0:
                plt.plot(time_22_list,postion_duplicate_22,".",label="22th duplicates", markersize=10)
            if len(time_23_list)!=0:
                plt.plot(time_23_list,postion_duplicate_23,".",label="23th duplicates", markersize=10)
            if len(time_24_list)!=0:
                plt.plot(time_24_list,postion_duplicate_24,".",label="24th duplicates", markersize=10)
            if len(time_25_list)!=0:
                plt.plot(time_25_list,postion_duplicate_25,".",label="25th duplicates", markersize=10)
            if len(time_26_list)!=0:
                plt.plot(time_26_list,postion_duplicate_26,".",label="26th duplicates", markersize=10)
            if len(time_27_list)!=0:
                plt.plot(time_27_list,postion_duplicate_27,".",label="27th duplicates", markersize=10)
            if len(time_28_list)!=0:
                plt.plot(time_28_list,postion_duplicate_28,".",label="28th duplicates", markersize=10)
            if len(time_29_list)!=0:
                plt.plot(time_29_list,postion_duplicate_29,".",label="29th duplicates", markersize=10)
            if len(time_30_list)!=0:
                plt.plot(time_30_list,postion_duplicate_30,".",label="30th duplicates", markersize=10)
            if len(time_31_list)!=0:
                plt.plot(time_31_list,postion_duplicate_31,".",label="31th duplicates", markersize=10)
            if len(time_32_list)!=0:
                plt.plot(time_32_list,postion_duplicate_32,".",label="32th duplicates", markersize=10)
            if len(time_33_list)!=0:
                plt.plot(time_33_list,postion_duplicate_33,".",label="33th duplicates", markersize=10)
            if len(time_34_list)!=0:
                plt.plot(time_34_list,postion_duplicate_34,".",label="34th duplicates", markersize=10)
            if len(time_35_list)!=0:
                plt.plot(time_35_list,postion_duplicate_35,".",label="35th duplicates", markersize=10)
            if len(time_36_list)!=0:
                plt.plot(time_36_list,postion_duplicate_36,".",label="36th duplicates", markersize=10)
            if len(time_37_list)!=0:
                plt.plot(time_37_list,postion_duplicate_37,".",label="37th duplicates", markersize=10)
            if len(time_38_list)!=0:
                plt.plot(time_38_list,postion_duplicate_38,".",label="38th duplicates", markersize=10)
            if len(time_39_list)!=0:
                plt.plot(time_39_list,postion_duplicate_39,".",label="39th duplicates", markersize=10)
            if len(time_40_list)!=0:
                plt.plot(time_40_list,postion_duplicate_40,".",label="40th duplicates", markersize=10)
            if len(time_41_list)!=0:
                plt.plot(time_41_list,postion_duplicate_41,".",label="41th duplicates", markersize=10)
            if len(time_42_list)!=0:
                plt.plot(time_42_list,postion_duplicate_42,".",label="42th duplicates", markersize=10)
            if len(time_43_list)!=0:
                plt.plot(time_43_list,postion_duplicate_43,".",label="43th duplicates", markersize=10)
            if len(time_44_list)!=0:
                plt.plot(time_44_list,postion_duplicate_44,".",label="44th duplicates", markersize=10)
            if len(time_45_list)!=0:
                plt.plot(time_45_list,postion_duplicate_45,".",label="45th duplicates", markersize=10)
            if len(time_46_list)!=0:
                plt.plot(time_46_list,postion_duplicate_46,".",label="46th duplicates", markersize=10)
            if len(time_47_list)!=0:
                plt.plot(time_47_list,postion_duplicate_47,".",label="47th duplicates", markersize=10)




            #if len(time_no_problem)!=0:
                #plt.plot(time_no_problem,position_regular,".",label="Not a duplicate", markersize=10,color="white")

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
            name=name.replace(".db","")
            if not os.path.exists("figures/{}/{}".format(name,column)):
                os.makedirs("figures/{}/{}".format(name,column))
            plt.savefig("figures/{}/{}/{}.png".format(name,column,row[0]),bbox_inches='tight', dpi=100)
            plt.close()
            