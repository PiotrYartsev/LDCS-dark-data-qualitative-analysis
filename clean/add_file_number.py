import sqlite3 as sl

#the function add_file_number is used to add a column to the database that contains the file number
def add_file_number(database):
    con = sl.connect(database, check_same_thread=False)


    #for each table in the database
    for row in con.execute('SELECT name FROM sqlite_master WHERE type = "table" ORDER BY name').fetchall():
        #skip the sqlite_sequence table
        if row[0] == 'sqlite_sequence':
            pass
        else:
            print("          "+row[0])
            #skip the table if it already has a file_number column
            try:
                con.execute("Select file_number from %s" % row[0]).fetchall()
                pass
            except:
                
                try:
                    #list to store the file numbers
                    

                    #this is a some test dataset that does not even use our naming convention, so we need to skip it
                    if row[0]=="EaTtest":
                        with con:
                            con.execute("""
                            DROP TABLE {};""".format(row[0]))
                    else:


                        #delete all the rows that have a null file name, this is a solution to a bug that is now solved, so really this should not be needed    
                        with con:
                            con.execute("""
                            DELETE FROM {}
                                WHERE file IS NULL;""".format(row[0]))
                        #add the file_number column
                        with con:
                            con.execute("""
                            ALTER TABLE {}
                                ADD file_number INTEGER;""".format(row[0]))
                            
                        #get the file names
                        data = con.execute("SELECT file FROM {}".format(row[0]))
                        

                        #for each file name, get the file number from the name and add it to the list
                        add_file_number=[]
                        for rows in data:
                            address_to_change=rows[0].split("_")[-2].replace("run","")
                            add_file_number.append(address_to_change)

                        #add the file numbers to the database
                        with con:
                            for n in range(len(add_file_number)):
                                sql="update {} set file_number=({}) where id={};".format(row[0],add_file_number[n],n+1)
                                con.execute(sql)
                except:
                    with con:
                        con.execute("""
                        DROP TABLE {};""".format(row[0]))

