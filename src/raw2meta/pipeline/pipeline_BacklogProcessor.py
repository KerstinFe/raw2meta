import os
import sys
from raw2meta.components.UserInput import get_UserInput
from raw2meta.config.logger import get_configured_logger
from raw2meta.db.FillDatabase_logic import FillDatabase_Fun, SampleReadyToProcess, FillDatabase_old
from raw2meta.db.database_helper import MissingFilesFromDatabase
from raw2meta.config.paths import TempFolder
from raw2meta.config.logger import get_configured_logger


logger = get_configured_logger(__name__)

logger.info("Starting Script")

if __name__ == "__main__":
    
    Metadata_DB,  MassSpecDirectory_ToObserve =   get_UserInput()
  
    try:
        for Directory in os.listdir(MassSpecDirectory_ToObserve):
            if int(Directory) < 202501:
                continue
            Directory_joined = os.path.join(MassSpecDirectory_ToObserve, Directory)
                 
            ListMissingRawFiles = MissingFilesFromDatabase(Metadata_DB, Directory_joined)
            
            if not ListMissingRawFiles:
                logger.info(f"Missing from Directory: {len(ListMissingRawFiles)} files")
                continue

            logger.info(f"Missing from Directory: {len(ListMissingRawFiles)} files")

            for rawFile in ListMissingRawFiles:                                   
                if SampleReadyToProcess(rawFile, Directory_joined, Metadata_DB):
                    rawFile = os.path.join(Directory_joined, rawFile)
                    FillDatabase_Fun(rawFile, Metadata_DB)

      
    except KeyboardInterrupt:
        logger.info("Keyboard Interrupt triggered, now exiting script")
        sys.exit(0)

    for file in os.listdir(TempFolder):
        logger.info(f'Processing temp file: {file}')
        FillDatabase_old(file, Metadata_DB)

