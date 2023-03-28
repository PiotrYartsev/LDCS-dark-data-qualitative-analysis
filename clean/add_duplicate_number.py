import sqlite3 as sl
from tqdm import tqdm

#define the function that will be used to add the duplicate column
def add_duplicate_number(dataset):
    
    #open tha database
    con = sl.connect(dataset,isolation_level=None)
    #get the tables
    with con:
        row2s=con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall()
    #for each table
    for row2 in row2s:
        row=row2[0]
        #skip the sqlite_sequence table
        if row == 'sqlite_sequence':
            pass
        else:
            print("          "+row)
            try:
                #check if the duplicate column already exists, in which case we skip the table
                with con:
                    con.execute("Select duplicate from %s" % row).fetchall()
            except:
                #get a list of the file numbers which we found earlier
                with con:
                    any_duplicates=con.execute("Select file_number from %s" % row).fetchall()

                #get a list of the unique file numbers
                unique_file_number=list(set(any_duplicates))
                #remove the empty second variable that SQLite adds for no goddamn reason
                unique_file_number=[a[0] for a in unique_file_number]

                #create a column called duplicate
                with con:
                        con.execute("ALTER TABLE {} ADD duplicate INTEGER;".format(row))

                #if there are the same numbet of unique file numbers as file numbers, then there are no duplicates
                if len(any_duplicates)==len(list(set(any_duplicates))):
                    print("          "+"No duplicates\n\n")
                    #set all the values in the duplicate column to null
                    with con:
                        con.execute("UPDATE {} SET duplicate = NULL;".format(row))
                
                else:
                    

                    print("          "+"Duplicates")

                    
                    

                    time_1={}
                    
                    
                    #to order the duplicates by creation time, we need to get the creation time of each file
                    def get_data(file_number,row):
                        with con:
                            creation_time = con.execute("SELECT FileCreationTime, id FROM {} WHERE file_number = ?;".format(row), (int(file_number),)).fetchall()

                        if len(creation_time)<1:
                            pass
                        else:
                            #sort the id numbers by creation time
                            timelist=[a[0] for a in creation_time]
                            id=[a[1] for a in creation_time]
                            time_1[str(file_number)]=[x for _, x in sorted(zip(timelist, id))]
                    #for each unique file number, altough this is not the most efficient way to do it, it is the easiest
                    for file_number in tqdm(unique_file_number):
                        if file_number==None:
                            pass
                        else:
                            get_data(file_number,row)

                    
                    print("          "+"Update database")

                    data=[]
                    #define the sql statement that will be used to update the database
                    #for the current table, set the duplicate column to the number of the duplicate
                    sql="UPDATE {} SET duplicate = ? WHERE id = ?;".format(row)

                    with con:
                        for i in tqdm(time_1):
                            lists=time_1[i]
                            #as the list is ordered by file_creation_time, the first duplicate will be 1, the second 2, etc.
                            for n in range(len(lists)):
                                data.append((n+1,lists[n]))
                    #update the database
                    con.execute("Begin transaction;")
                    con.executemany(sql,data)
                    con.execute("Commit;")

                    print("          "+"\n")