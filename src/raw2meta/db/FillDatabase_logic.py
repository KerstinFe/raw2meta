import os
from typing import Union, Optional
import threading
from pathlib import Path
from datetime import timedelta, date
import time
import sqlite3
from raw2meta.helper.Exceptions import HandlingEmptyFileError, HandlingCorruptFileError, NoFittingProjectFound
from raw2meta.config.paths import Logfile_corrupt, Logfile_empty, TempFolder
from raw2meta.db.FillDatabase_Fun import Execute_CreateSQLdbCode
from raw2meta.helper.common import SplitProjectName, GetFilePath
from raw2meta.config.configuration import DaysWaiting, MinFileSize
from raw2meta.config.logger import get_configured_logger


logger = get_configured_logger(__name__)

'''
to do: check/ discuss with Fritz how to handle errors, which are immediately written into the db, which only in log files, etc.
so far: Corrupt and Empty file errors are logged, but only corrupt files with size < 15000 kb are written into the db immediately
'''


def FillDatabase_Fun(file: Union[str, Path], Metadata_DB: Union[str, Path], stop_event: Optional[threading.Event] =None) -> None:

    '''Process a file and fill the database
    Gets metadata from file, raises HandlingCorruptFileError (ArgumentOutOfRangeException) or HandlingEmptyFileError (IndexOutOfRangeException)
    handles the errors, when file is corrupt, logs it and fills the database with error information
    when file is empty it also puts it into a log file

    :param file: Path to the raw file.
    :type file: Union[str, Path]
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :param stop_event: Optional threading event to allow interrupting waits.
    :type stop_event: Optional[threading.Event], optional
    :return: None
    :rtype: None
    '''

    SQL_DB = Execute_CreateSQLdbCode(Metadata_DB) # gets the class for the functions 

    try:
        time.sleep(30) # still wait an extra 30 seconds to be sure
        SQL_DB.FillDatabase(file) # gets metadata from file, raises HandlingCorruptFileError (ArgumentOutOfRangeException) or HandlingEmptyFileError (IndexOutOfRangeException)
        logger.info("DB Updated")

                                    
    except HandlingCorruptFileError:
        with open(Logfile_corrupt, "a") as logfile:
            logfile.write((file + "\n"))
        logger.info("Error while loading file, needs further inspection")

    except HandlingEmptyFileError:
        filesize = os.path.getsize(file)/1000 # filesize returned as byte -> convertion to kb
        
        if filesize < MinFileSize:
            with open(Logfile_corrupt, "a") as logfile:
                logfile.write((file + "\n"))
            logger.info(f"No Scans found in file and size below {MinFileSize} kb: file likely corrupt or empty.")

            
            SQL_DB.FillDatabase_Error(file, "CorruptFile")

        else:
            with open(Logfile_empty, "a") as logfile:
                logfile.write((file + "\n"))
            logger.info("No Scans found in file, but filesize above 15000 kb, needs further inspection.")

def FillDatabase_old(file: Union[str, Path], Metadata_DB: Union[str, Path]) -> None:
    """Process old temp files, inserting them if project now exists or after waiting period.
    :param file: Name of the temp file.
    :type file: Union[str, Path]
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :return: None
    :rtype: None
    """
    DaysAgo = date.today() - timedelta(DaysWaiting)
    DaysAgo = DaysAgo.strftime("%Y%m%d")  

    _, _, _, ProjectID_Date = SplitProjectName(file)
    SQL_DB = Execute_CreateSQLdbCode(Metadata_DB)

    # For temp files, we need to use the TempFolder path
    TempFilePath = os.path.join(TempFolder, file)
    
    if os.path.exists(TempFilePath):
        try:
            SQL_DB.FillDatabaseWithJson(file)  # This handles temp files
            logger.info("DB Updated from temp file")   
           

        except NoFittingProjectFound:
            if ProjectID_Date < DaysAgo:
                SQL_DB.FillDatabaseWithJson_KeepProject(file)
            
            else:
                logger.info(f"Project not found yet for {file}, waiting longer")
    else:
        logger.warning(f"Temp file {TempFilePath} does not exist")                                                                     

def FillDatabase_Corrupt(file: Union[str, Path], Metadata_DB: Union[str, Path]) -> None:

    '''Process corrupt files and fill the database with error information.
    :param file: Path to the raw file.
    :type file: Union[str, Path]
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :return: None
    :rtype: None
    '''

    SQL_DB = Execute_CreateSQLdbCode(Metadata_DB)
    try: 
        SQL_DB.FillDatabase(file) 
        logger.info("DB Updated, file not corrupt")  


    except (HandlingCorruptFileError, HandlingEmptyFileError):
    
        SQL_DB.FillDatabase_Error(file, "CorruptFile")
        logger.info("File inserted as corrupt into DB")


def SampleReadyToProcess(RawfilePath: Union[str, Path], Directory_joined: Union[str, Path],
                          Metadata_DB: Union[str, Path], stop_event: Optional[threading.Event] = None) -> bool:

    ''' Check if Sample is already in database and accessible 
    :param RawfilePath: Full path to the raw file.
    :type RawfilePath: Union[str, Path]
    :param Directory_joined: Directory where the raw file is located.
    :type Directory_joined: Union[str, Path]
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :param stop_event: Optional threading event to allow interrupting waits.
    :type stop_event: Optional[threading.Event], optional
    :return: True if the sample is ready to process, False otherwise.
    :rtype: bool
    '''
    RawfilePath, Name = GetFilePath(RawfilePath, Directory_joined)

    try:
        with sqlite3.connect(Metadata_DB) as con:
            cur = con.cursor()
            RowCount = cur.execute('''SELECT EXISTS(SELECT 1 FROM Metadata_Sample 
                                            WHERE SampleName_ID LIKE (?) LIMIT 1)''', (Name,)).fetchone()
            ReturnValue, = RowCount
    except sqlite3.Error as e:
        logger.error(f"Database error checking sample {Name}: {e}")
        return False

    if ReturnValue == 0:
        logger.info(f"File from Q: {Name}")   

        # Check if file is accessible (not being copied)
        max_retries = 10
        retry_count = 0
        NotCopied = True
        while NotCopied and retry_count < max_retries:
            try:
                with open(RawfilePath, "rb") as f:
                    f.readline()
                NotCopied = False
            except PermissionError:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"File {RawfilePath} remained inaccessible after {max_retries} retries")
                    return False
                
                if stop_event.wait(timeout=60):
                        logger.info("Stop event set while waiting for file accessibility: %s", RawfilePath)
                        return False
                
            except FileNotFoundError:
                logger.error(f"File not found: {RawfilePath}")
                return False

        return True     
    else:
        return False
