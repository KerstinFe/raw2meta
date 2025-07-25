import sqlite3
import os
from ETL_Functions import Execute_CreateSQLdbCode,  get_UserInput
import re

print("Starting Script", flush=True)

FileToWatch_Metadata,  MassSpecDirectory_ToObserve =   get_UserInput()
 
SQL_DB = Execute_CreateSQLdbCode(FileToWatch_Metadata)
 
if __name__ == "__main__":
    with sqlite3.connect(FileToWatch_Metadata) as con:
        cur = con.cursor()
        Proj_Dates_Tup = cur.execute('''SELECT ProjectID_Date FROM Metadata_Project ''').fetchall()
        con.commit()
    
        Proj_Dates = []
    
    for Date_Tup in Proj_Dates_Tup:
        Date, = Date_Tup
        Proj_Dates.append(Date)
    
    ProjMonths = [re.sub("[0-9]{2}$", "",x) for x in Proj_Dates]
    ProjMonths_unique = set(ProjMonths)
    ProjMonths_unique = list(ProjMonths_unique)
    ProjMonths_unique.sort()
    
    print(f"Months covered in DB: {ProjMonths_unique}", flush = True)
    
    for Directory in ProjMonths_unique:
        Directory_joined = os.path.join(MassSpecDirectory_ToObserve, Directory)
        print(Directory_joined, flush = True)
        
        try:
            ListRawFiles = os.listdir(Directory_joined)
        except OSError:
            continue
        
        ListRawFiles = [file for file in ListRawFiles if re.search(".raw",file)]
        ListMissingRawFiles = SQL_DB.MissingFilesFromDatabase(ListRawFiles)
        print(f"Raw Files in Directory {Directory}: {len(ListRawFiles)} files", flush = True)
        print(f"Missing from Directory {Directory}: {len(ListMissingRawFiles)} files", flush = True)
        
   
         
    
