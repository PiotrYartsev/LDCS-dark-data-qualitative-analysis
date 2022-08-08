
import os
from zlib import adler32
from datetime import datetime
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
from subprocess import PIPE, Popen
con = sl.connect('/home/pioyar/rucio-client-venv/summer_project/SLAC_mc20.db')





def runner(scope,file,max_file_number):
    p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = p.communicate()
    #print(type(L_1))
    stderr=stderr.decode("utf-8").split("\n")

    L=L_1.decode("utf-8").split("\n")  
    #if len(stderr)>1:    
    #print(stderr)
    
    if len(L)<2:
        FileCreationTime=file.split("_")[-1].replace("t","")
        FileCreationTime=FileCreationTime.replace(".roo","")
        #print(FileCreationTime)
        data.append((len(data)+1+max_file_number,file,None,None,None,scope,None,FileCreationTime)) 
        #print(len(data)+1)
    else:
        
        for n in range(len(L)):
            a=L[n]
            
            a2=a.split(":")
            #print(a2)
            if a2==['']:
                pass
            else:
                a2[1]=a2[1].replace('   ','')
                if "BatchID" in a2[0]:
                    BatchID=a2[1]
                    #print(BatchID)
                if "ComputingElement" in a2[0]:
                    ComputingElement=a2[1]
                if "DataLocation" in a2[0]:
                    DataLocation=a2[1]
                if "Scope" in a2[0]:
                    Scope=a2[1]
                if "JobSubmissionTime" in a2[0]:
                    JobSubmissionTime=str(a2[1])+':'+str(a2[2])+':'+str(a2[3])
                    #print(JobSubmissionTime)
                if "FileCreationTime" in a2[0]:
                    FileCreationTime=a2[1]

        
        data.append((len(data)+1+max_file_number,file,BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime)) 

datasets=open("/home/pioyar/rucio-client-venv/project/output/All_SLAC_GRIDFTP_time/files_missing_rucio.txt","r")
lines=datasets.readlines()

datasets_and_data={}
for line in lines:
    #print(line)
    filestuff=line.split(",")
    filename=filestuff[0]
    file_location=filestuff[1]
    dataset=file_location.split("/")[-2]
    #print(filename)
    #print(dataset)
    
    
    scope="mc20"
    file12=dataset.replace('.','')
    file12=file12.replace('_','')
    file12=file12.replace('-','')
    #print(file12)
    if file12[0].isnumeric():
        #print("True")
        file12="A"+file12
    if file12 not in datasets_and_data:
        datasets_and_data[file12]=[]
        datasets_and_data[file12].append([filename,scope,dataset])
    else:
        datasets_and_data[file12].append([filename,scope,dataset])
#print(datasets_and_data)
    


for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
    if row[0] == 'sqlite_sequence':
        pass
    else:
        if row[0] in datasets_and_data:
            max_file_number = con.execute("""
            SELECT MAX(id) FROM {};""".format(row[0])).fetchone()[0]
            
            data=[]
            print(row[0])
            #print(max_file_number)
            dataset_2=row[0]
            sql = 'INSERT INTO {} (id, file, BatchID,ComputingElement,DataLocation,Scope,JobSubmissionTime,FileCreationTime) values(?, ?, ?, ?, ?, ?, ?, ?)'.format(dataset_2)
            files=datasets_and_data[dataset_2]
            #print(files)
            
            files_1=files[:len(files)//16]
            files_2=files[len(files)//16:len(files)*2//16]
            files_3=files[len(files)*2//16:len(files)*3//16]
            files_4=files[len(files)*3//16:len(files)*4//16]
            files_5=files[len(files)*4//16:len(files)*5//16]
            files_6=files[len(files)*5//16:len(files)*6//16]
            files_7=files[len(files)*6//16:len(files)*7//16]
            files_8=files[len(files)*7//16:len(files)*8//16]
            files_9=files[len(files)*8//16:len(files)*9//16]
            files_10=files[len(files)*9//16:len(files)*10//16]
            files_11=files[len(files)*10//16:len(files)*11//16]
            files_12=files[len(files)*11//16:len(files)*12//16]
            files_13=files[len(files)*12//16:len(files)*13//16]
            files_14=files[len(files)*13//16:len(files)*14//16]
            files_15=files[len(files)*14//16:len(files)*15//16]
            files_16=files[len(files)*15//16:]
            #print(files_1)

            #print(len(files))

            #print(len(files_1)+len(files_2)+len(files_3)+len(files_4)+len(files_5)+len(files_6)+len(files_7)+len(files_8)+len(files_9)+len(files_10)+len(files_11)+len(files_12)+len(files_13)+len(files_14)+len(files_15)+len(files_16))
            
            with tqdm(total=max(len(files_1),len(files_2),len(files_3),len(files_4),len(files_5),len(files_6),len(files_7),len(files_8),len(files_9),len(files_10),len(files_11),len(files_12),len(files_13),len(files_14),len(files_15),len(files_16))) as pbar:
                
                for (a,b,c,d,e,f,g,h, k, l,m,n,o,p,w,z) in itertools.zip_longest(files_1,files_2,files_3,files_4,files_5,files_6,files_7,files_8,files_9,files_10,files_11,files_12,files_13,files_14,files_15,files_16):
                    #print(len(data))
                    #print(a)
                    if not a==None:
                        t1 = threading.Thread(target=runner, args=(a[1],a[0],max_file_number,))
                    if not b==None:
                        t2 = threading.Thread(target=runner, args=(b[1],b[0],max_file_number,))
                    if not c==None:
                        t3 = threading.Thread(target=runner, args=(c[1],c[0],max_file_number,))
                    if not d==None:
                        t4 = threading.Thread(target=runner, args=(d[1],d[0],max_file_number,))
                    if not e==None:
                        t5 = threading.Thread(target=runner, args=(e[1],e[0],max_file_number,))
                    if not f==None:
                        t6 = threading.Thread(target=runner, args=(f[1],f[0],max_file_number,))
                    if not g==None:
                        t7 = threading.Thread(target=runner, args=(g[1],g[0],max_file_number,))
                    if not h==None:
                        t8 = threading.Thread(target=runner, args=(h[1],h[0],max_file_number,))
                    if not k==None:
                        t9 = threading.Thread(target=runner, args=(k[1],k[0],max_file_number,))
                    if not l==None:
                        t10 = threading.Thread(target=runner, args=(l[1],l[0],max_file_number,))
                    if not m==None:
                        t11 = threading.Thread(target=runner, args=(m[1],m[0],max_file_number,))
                    if not n==None:
                        t12 = threading.Thread(target=runner, args=(n[1],n[0],max_file_number,))
                    if not o==None:
                        t13 = threading.Thread(target=runner, args=(o[1],o[0],max_file_number,))
                    if not p==None:
                        t14 = threading.Thread(target=runner, args=(p[1],p[0],max_file_number,))
                    if not w==None:
                        t15 = threading.Thread(target=runner, args=(w[1],w[0],max_file_number,))
                    if not z==None:
                        t16 = threading.Thread(target=runner, args=(z[1],z[0],max_file_number,))


                    if not a==None:
                        t1.start()
                    if not b==None:
                        t2.start()
                    if not c==None:
                        t3.start()
                    if not d==None:
                        t4.start()
                    if not e==None:
                        t5.start()
                    if not f==None:
                        t6.start()
                    if not g==None:
                        t7.start()
                    if not h==None:
                        t8.start()
                    if not k==None:
                        t9.start()
                    if not l==None:
                        t10.start()
                    if not m==None:
                        t11.start()
                    if not n==None:
                        t12.start()
                    if not o==None:
                        t13.start()
                    if not p==None:
                        t14.start()
                    if not w==None:
                        t15.start()
                    if not z==None:
                        t16.start()





                        
                    if not a==None:
                        t1.join()
                    if not b==None:
                        t2.join()
                    if not c==None:
                        t3.join()
                    if not d==None:
                        t4.join()
                    if not e==None:
                        t5.join()
                    if not f==None:
                        t6.join()
                    if not g==None:
                        t7.join()
                    if not h==None:
                        t8.join()
                    if not k==None:
                        t9.join()
                    if not l==None:
                        t10.join()
                    if not m==None:
                        t11.join()
                    if not n==None:
                        t12.join()
                    if not o==None:
                        t13.join()
                    if not p==None:
                        t14.join()
                    if not w==None:
                        t15.join()
                    if not z==None:
                        t16.join()

                    pbar.update(1)
            #print(sql)
            with con:
                    con.executemany(sql, data)