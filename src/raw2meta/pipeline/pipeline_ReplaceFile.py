import os
from pathlib import Path
from raw2meta.components.UserInput import get_UserInput
from raw2meta.db.FillDatabase_logic import FillDatabase_Fun, SampleReadyToProcess
from raw2meta.config.logger import get_configured_logger
from raw2meta.helper.common import MakePathNice

logger = get_configured_logger(__name__)

if __name__ == "__main__":
    
    Metadata_DB,  MassSpecDirectory_ToObserve =   get_UserInput()
    logger.info("Using database: %s", os.path.abspath(Metadata_DB))
    
    InsertAnotherFile = ''

    while InsertAnotherFile == '':

        files_input = input("Enter file paths separated by commas: ")
        files = [f.strip() for f in files_input.split(",")]
        for FileToReplace in files:

            FileToReplace = MakePathNice(FileToReplace)

            if os.path.isfile(FileToReplace): 

                if Path(FileToReplace).suffix == ".raw":

                    if SampleReadyToProcess(FileToReplace, MassSpecDirectory_ToObserve, Metadata_DB):

                        FillDatabase_Fun(FileToReplace, Metadata_DB)

                    else:
                        logger.info("File (%s) is already in db.", FileToReplace)
                else:
                    logger.info("Provided path (%s) does not belong to a raw file.", FileToReplace)
            else:
                logger.info("Provided path (%s) does not belong to a file.", FileToReplace)


        InsertAnotherFile = input('Press Enter to retry inserting another file.')
