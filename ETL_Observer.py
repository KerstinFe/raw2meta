import sqlite3
import sys
from pathlib import Path
from ETL_Functions import Execute_CreateSQLdbCode, get_UserInput, q
from ETL_Functions import HandlingCorruptFileError,HandlingEmptyFileError
from ETL_Functions import ObservingFolders, Logfile_corrupt,Logfile_empty,Logfile_Integrity,SafedAsJsonTempFile
import threading
import time
from queue import Empty
import os


print("Starting Script", flush=True)


if __name__ == "__main__":
    
    FileToWatch_Metadata,  MassSpecDirectory_ToObserve =   get_UserInput()
    print()
    print(FileToWatch_Metadata,  MassSpecDirectory_ToObserve )
    print()
    
    stop_event = threading.Event()
    ObservingFolders= ObservingFolders(MassSpecDirectory_ToObserve,FileToWatch_Metadata)
    UpdatingObservedMonths_thread = threading.Thread(target= ObservingFolders.Redefine_Directory, args=( stop_event,))
    UpdatingObservedMonths_thread.daemon = False
    UpdatingObservedMonths_thread.start()
    
    SQL_DB = Execute_CreateSQLdbCode(FileToWatch_Metadata)
    NoStopSignal = True   
    try:
        while NoStopSignal:
            try:
                file = q.get(block = False,timeout=1)
                file = Path(file).as_posix()
                
                if Path(file).suffix == ".raw":
                    time.sleep(60*5) # I am waiting 5 minutes hoping that then the file will be fully copied, because some files escape the copy check
                                                       
                    if SQL_DB.SampleNotInDatabase(file):
                        
                        print("File from Q: " + f'{file}')    
                        NotCopied = True
                        while NotCopied:
                            try:
                                with open(file, "rb") as f:
                                    f.readline()
                                    
                                NotCopied =False
                                                                  
                            except PermissionError:
                                time.sleep(30)
                               
                        try:
                            time.sleep(30) # still wait an extra 30 seconds to be sure
                            SQL_DB.FillDatabase(file) 
                            print("DB Updated", flush =True)
                        
                        except SafedAsJsonTempFile:
                            print("Data stored in temp file", flush =True)    
                            
                        except sqlite3.IntegrityError:
                            print("File already in DB", flush =True)
                            with open(Logfile_Integrity, "a") as logfile:
                                logfile.write((file + "\n"))
                                                        
                        except HandlingCorruptFileError:
                            with open(Logfile_corrupt, "a") as logfile:
                                logfile.write((file + "\n"))
                            print("Error while loading file, needs further inspection", flush =True)
                            
                        except HandlingEmptyFileError:
                            filesize = os.path.getsize(file)/1000
                            
                            if filesize < 15000:
                                with open(Logfile_corrupt, "a") as logfile:
                                    logfile.write((file + "\n"))
                                print("No Scans found in file and size below 15000 kb: file likely corrupt or empty.", flush =True)  
                             
                                try:
                                    SQL_DB.FillDatabase_Error(file, "CorruptFile")
                                    
                                except sqlite3.IntegrityError:
                                    print("Corrupt file already in DB", flush =True)
                                    
                            else:    
                                with open(Logfile_empty, "a") as logfile:
                                    logfile.write((file + "\n"))
                                print("No Scans found in file, but filesize above 15000 kb, needs further inspection.", flush =True)     
                       
            except KeyboardInterrupt:   
                print("\n Stopping threads...")
                NoStopSignal = False
                stop_event.set()
                ObservingFolders.ClosingObservations()
                print("closed Observers")
                UpdatingObservedMonths_thread.join()
                print("Main thread exiting cleanly.")
                sys.exit(0)
                            
            except Empty:
                continue
            
    except KeyboardInterrupt:
        print("\n Stopping threads...")
        NoStopSignal = False
        stop_event.set()
        ObservingFolders.ClosingObservations()
        print("closed Observers")
        UpdatingObservedMonths_thread.join()
        print("Main thread exiting cleanly.")
        sys.exit(0)
        
         
    
