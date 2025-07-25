import sqlite3
import os
from pathlib import Path
from ETL_Functions import Execute_CreateSQLdbCode,get_UserInput
from ETL_Functions import SafedAsJsonTempFile ,HandlingCorruptFileError, HandlingEmptyFileError

if __name__ == "__main__":
    
    FileToWatch_Metadata,  MassSpecDirectory_ToObserve =   get_UserInput()
    SQL_DB = Execute_CreateSQLdbCode(FileToWatch_Metadata)
    InsertAnotherFile = ''
    while InsertAnotherFile == '':
        FileToInsert = input('Enter full path to file that is supposed to be inserted as corrupt: ')
        FileToInsert =FileToInsert.replace('"', '')
        FileToInsert =FileToInsert.replace("'", "")
        rawFile_fullpath  = Path(FileToInsert).as_posix()
        
        if os.path.isfile(FileToInsert): # to prevent inserting data for other files
           
            if Path(FileToInsert).suffix == ".raw": # to prevent inserting data for other files
                
                try:
                    SQL_DB.FillDatabase(rawFile_fullpath) # I just want to try to insert the file into the database again, incase it is not actually corrupt
                    print("DB Updated, file not corrupt", flush =True)
                
                except SafedAsJsonTempFile:
                    print("Data stored in temp file, file not corrupt", flush =True)    
                    
                except sqlite3.IntegrityError:
                    print("File already in DB", flush =True)
                                                                
                except HandlingCorruptFileError:
                    try:
                        SQL_DB.FillDatabase_Error(rawFile_fullpath, "CorruptFile")
                        print("File inserted as corrupt into DB", flush =True)
                                   
                    except sqlite3.IntegrityError:
                        print("Corrupt file already in DB", flush =True)
                    
                except HandlingEmptyFileError:
                    try:
                        SQL_DB.FillDatabase_Error(rawFile_fullpath, "CorruptFile")
                        print("File inserted as corrupt into DB", flush =True)
                        
                    except sqlite3.IntegrityError:
                        print("Corrupt file already in DB", flush =True)
                   
                
            else:    
                print("Provided path ({FileToInsert}) does not belong to a raw file", flush =True)
        else:
            print("Provided path ({FileToInsert}) does not belong to a file", flush =True)
        
        InsertAnotherFile =input('Press Enter to insert another file as corrupt.')
