import os
import re
from raw2meta.components.UserInput import get_UserInput
from raw2meta.config.logger import  get_configured_logger
from raw2meta.db.database_helper import MissingFilesFromDatabase, GetMonthsInDB

logger = get_configured_logger(__name__)

logger.info("Starting Script")

Metadata_DB,  MassSpecDirectory_ToObserve =   get_UserInput()
 
if __name__ == "__main__":

    ProjMonths_unique = GetMonthsInDB(Metadata_DB)

    logger.info(f"Months covered in DB: {ProjMonths_unique}")

    for Directory in ProjMonths_unique:

        Directory_joined = os.path.join(MassSpecDirectory_ToObserve, Directory)
        
        try:
            ListRawFilesOnServer =[file for file in os.listdir(Directory_joined) if re.search(".raw",file)]
            
        except OSError:
            continue
    
        ListMissingRawFiles = MissingFilesFromDatabase(Metadata_DB, Directory_joined)

        logger.info(f"Raw Files in Directory {Directory}: {len(ListRawFilesOnServer)} files")
        logger.info(f"Missing from Directory {Directory}: {len(ListMissingRawFiles)} files")


