import statistics
from tqdm import *
import sqlite3 as sl
import threading
import itertools 
import random 
from subprocess import PIPE, Popen

#open the text file
file=open('mode1_mode2.txt','r')

#make file into text of whole file
text=file.read()
chunks=text.split(",")

thing_to_sheck={}
for part in chunks:
    parts=part.split("\n")
    thing_to_sheck[parts[0]]=parts[1:]


for key in thing_to_sheck.keys():
    thing_to_sheck[key]=[a for a in thing_to_sheck[key] if a!='']

def runner(info):
    thing=info.split(":")
    scope=thing[0]
    file=thing[1]

    p = Popen("rucio get-metadata {}:{}".format(scope,file), shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = p.communicate()
    #print(type(L_1))
    stderr=stderr.decode("utf-8").split("\n")

    L=L_1.decode("utf-8").split("\n")  
    #if len(stderr)>1:    
    #print(stderr)
    
    if len(L)<2:
        pass
    else:
        DataLocation=0
        for n in range(len(L)):
            a=L[n]
            #split at the first space
            a2=a.split(":",1)
            #print(a2)
            if a2==['']:
                pass
            else:
                if "DataLocation" in a2[0]:
                    DataLocation=a2[1]
                    DataLocation=DataLocation.replace(" ","")
                    DataLocation=DataLocation.replace("gsiftp://hep-fs.lunarc.lu.se:2811/ldcs/","/projects/hep/fs9/shared/ldmx/ldcs/gridftp/")
                    #remove everything after last slash
                    DataLocation=DataLocation[:DataLocation.rfind("/")]
                    #add the slash back
                    DataLocation=DataLocation+"/"


        if DataLocation==0:
            pass
        else:
            return DataLocation

for key in thing_to_sheck.keys():
    DataLocation=runner(key)
    print(DataLocation)
    #get a list of the files at that location DataLocation
    #Run bash cd 
    Popen("cd {}".format(DataLocation), shell=True, stdout=PIPE, stderr=PIPE)
    files_at_location=Popen("ls", shell=True, stdout=PIPE, stderr=PIPE)
    L_1, stderr = files_at_location.communicate()
    files_at_location=L_1.decode("utf-8").split("\n")
    files_at_location=L_1.decode("utf-8").split("\n")
    files_at_location=[a.replace(" ","") for a in files_at_location]
    print(files_at_location)
    for file in thing_to_sheck[key]:
        file=file.replace(" ","")
        file=file.replace("\n","")
        print(file)
        #check if the file is in the list of files at that location
        if file in files_at_location:
            print("yes")
        else:
            print("no")

    break
