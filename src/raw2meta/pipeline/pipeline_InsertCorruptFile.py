import os
from pathlib import Path
from raw2meta.db.FillDatabase_logic import  SampleReadyToProcess, FillDatabase_Corrupt
from raw2meta.components.UserInput import get_UserInput
from raw2meta.config.logger import  get_configured_logger

logger = get_configured_logger(__name__)

logger.info("Starting Script")

Metadata_DB,  MassSpecDirectory_ToObserve =   get_UserInput()

if __name__ == "__main__":
        
    InsertAnotherFile = ''

    while InsertAnotherFile == '':

        files_input = input("Enter file paths separated by commas: ")
        files = [f.strip() for f in files_input.split(",")]

        for FileToReplace in files:
            FileToReplace = Path(FileToReplace).as_posix()

            if os.path.isfile(FileToReplace) and Path(FileToReplace).suffix == ".raw": 
                Directory_joined = os.path.dirname(FileToReplace)
                if SampleReadyToProcess(FileToReplace, Directory_joined, Metadata_DB):

                    FillDatabase_Corrupt(FileToReplace, Metadata_DB)

                else:
                    logger.info("File (%s) is already in db.", FileToReplace)
                
            else:
                logger.info("Provided path (%s) does not belong to a (raw) file.", FileToReplace)

        InsertAnotherFile =input('Press Enter to insert another file as corrupt.')

     