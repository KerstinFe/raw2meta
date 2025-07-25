import sqlite3
import os
import sys
from pathlib import Path
from ETL_Functions import Execute_CreateSQLdbCode, Database_Call_MetaDataSQL, get_UserInput
from ETL_Functions import HandlingCorruptFileError,HandlingEmptyFileError 
from ETL_Functions import TempFolder,  NoFittingProjectFound,SafedAsJsonTempFile
from ETL_Functions import Logfile_corrupt,Logfile_empty,Logfile_Integrity, SplitProjectName
import re
from datetime import timedelta,date


print("Starting Script", flush=True)

if __name__ == "__main__":
    
    FileToWatch_Metadata,  MassSpecDirectory_ToObserve =   get_UserInput()
 
    Metadata_SQL = Database_Call_MetaDataSQL(FileToWatch_Metadata) 
    Metadata_SQL.Database_CreateTables()
    SQL_DB = Execute_CreateSQLdbCode(FileToWatch_Metadata)

    try:
        for Directory in os.listdir(MassSpecDirectory_ToObserve):
            Directory_joined = os.path.join(MassSpecDirectory_ToObserve, Directory)
            print(Directory_joined, flush = True)
            try:
                ListRawFiles = os.listdir(Directory_joined)
            except OSError:
                continue
            ListRawFiles = [file for file in ListRawFiles if re.search(".raw",file)]
            ListMissingRawFiles = SQL_DB.MissingFilesFromDatabase(ListRawFiles)
        
            print(f"Missing from Directory: {len(ListMissingRawFiles)} files", flush = True)
            
            for rawFileTup in ListMissingRawFiles:
                rawFile, = rawFileTup 
                
                if Path(rawFile).suffix == ".raw":
                    
                    if SQL_DB.SampleNotInDatabase(rawFile):
                        rawFile_fullpath = os.path.join(Directory_joined, rawFile)
                        rawFile_fullpath = Path(rawFile_fullpath).as_posix()
                        print(rawFile, flush = True)
                        
                        try:
                            SQL_DB.FillDatabase(rawFile_fullpath) 
                            print("DB Updated", flush =True)
                        
                        except SafedAsJsonTempFile:
                            print("Data stored in temp file", flush =True)        
                        
                        except sqlite3.IntegrityError:
                            with open(Logfile_Integrity, "a") as logfile:
                                logfile.write((rawFile_fullpath + "\n"))
                            print("File already in DB", flush =True)
              
                        except HandlingCorruptFileError:
                            with open(Logfile_corrupt, "a") as logfile:
                                logfile.write((rawFile_fullpath + "\n"))
                            print("Error while loading file, needs further inspection", flush =True)
                            
                        except HandlingEmptyFileError:
                            filesize = os.path.getsize(rawFile_fullpath)/1000
                            
                            if filesize < 15000:
                                with open(Logfile_corrupt, "a") as logfile:
                                    logfile.write((rawFile_fullpath + "\n"))
                                print("No Scans found in file and size below 15000 kb: file likely corrupt or empty.", flush =True)  
                          
                                try:
                                    SQL_DB.FillDatabase_Error(rawFile_fullpath, "CorruptFile")
                                    
                                except sqlite3.IntegrityError:
                                    print("Corrupt file already in DB", flush =True)
                                    
                            else:    
                                with open(Logfile_empty, "a") as logfile:
                                    logfile.write((rawFile_fullpath + "\n"))
                                print("No Scans found in file, but filesize above 15000 kb, needs further inspection.", flush =True)    
    except KeyboardInterrupt:
        print("Keyboard Interrupt triggered, now exiting script")
        sys.exit(0)
             
             
    DaysAgo = date.today() -timedelta(days=5)
    DaysAgo  = DaysAgo.strftime("%Y%m%d")    
    
    for file in os.listdir(TempFolder):
        
        ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(file)
                   
        print(f'Sample not in DB yet?: {SQL_DB.SampleNotInDatabase(file)}')                        
        if SQL_DB.SampleNotInDatabase(file):
            
            try:
                SQL_DB.FillDatabaseWithJson(file)
                
            except NoFittingProjectFound:
                
                if ProjectID_Date < DaysAgo:
                    try:
                        SQL_DB.FillDatabaseWithJson_KeepProject(file) 
                        
                    except sqlite3.IntegrityError:
                          with open(Logfile_Integrity, "a") as logfile:
                              logfile.write((file + "Json" + "\n"))
                          print("Sample already in DB, TEMP file deleted", flush =True)  
                          os.remove(os.path.join(TempFolder, file))  
                     
                        
            except sqlite3.IntegrityError:
                  with open(Logfile_Integrity, "a") as logfile:
                      logfile.write((file + "Json" + "\n"))
                  print("Sample already in DB, TEMP file deleted", flush =True)  
                  os.remove(os.path.join(TempFolder, file))     
                                                                           
        else:     
            os.remove(os.path.join(TempFolder, file))  
            print(f'Sample already in DB. TEMP file {file} deleted', flush = True)                               
                     
   
         
    
