import os
from pathlib import Path
from ETL_Functions import Execute_CreateSQLdbCode,get_UserInput
from ETL_Functions import HandlingCorruptFileError,HandlingEmptyFileError 


if __name__ == "__main__":
    
    FileToWatch_Metadata,  MassSpecDirectory_ToObserve =   get_UserInput()
    SQL_DB = Execute_CreateSQLdbCode(FileToWatch_Metadata)
    
    InsertAnotherFile = ''
    while InsertAnotherFile == '':
    
        FileToReplace = input('Enter full path to file that is supposed to be replaced: ')
        FileToReplace =FileToReplace.replace('"', '')
        FileToReplace =FileToReplace.replace("'", "")
        rawFile_fullpath  = Path(FileToReplace).as_posix()
        
        if os.path.isfile(FileToReplace): 
           
            if Path(FileToReplace).suffix == ".raw":
                
                try:
                    SQL_DB.ReplaceErrorFile(rawFile_fullpath) 
                    print("DB Updated", flush =True)
                
                except HandlingCorruptFileError:
                     print("Error while loading file, needs further inspection", flush =True)
                    
                except HandlingEmptyFileError:
                    filesize = os.path.getsize(rawFile_fullpath)/1000
                    
                    if filesize < 15000:
                        print("No Scans found in file and size below 15000 kb: file likely corrupt or empty.", flush =True)  
                    else:
                        print("No Scans found in file but size above 15000 kb: please double check file.", flush =True)  
                        
                        ''' no insertion into DB or log files needed here as the file is already in DB'''
                        
                        
            else:    
                print("Provided path ({FileToReplace}) does not belong to a raw file")
        else:
            print("Provided path ({FileToReplace}) does not belong to a file")
            
        InsertAnotherFile =input('Press Enter to insert another file as corrupt.')
