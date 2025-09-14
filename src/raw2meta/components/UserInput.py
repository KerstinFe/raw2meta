import os
from pathlib import Path
from typing import Tuple
from raw2meta.config.configuration import  TablesMetaData
from raw2meta.db.database_helper import GetTableNames
from raw2meta.db.CreateDatabase import Database_CreateTables
from raw2meta.config.paths import DEFAULT_METADATA_DB, DEFAULT_MASS_SPEC_DIR
from raw2meta.helper.common import MakePathNice
from raw2meta.config.logger import get_configured_logger
from typing import Any, Union


logger = get_configured_logger(__name__)


def _validate_database_tables(db_path: Union[str, Path]) -> bool:
    '''Check if database has required tables.
    :param db_path: Path to the database file.
    :type db_path: Union[str, Path]
    :return: True if all required tables are present, False otherwise.
    :rtype: bool
    '''
    try:
        table_names = GetTableNames(db_path)
        return set(table_names) == set(TablesMetaData)
    except Exception as e:
        logger.error(f"Error validating database tables: {e}")
        return False


def _handle_database_creation_or_tables(db_path: Union[str, Path], exists: bool = True) -> bool:
    '''Handle database or table creation based on user input.
    :param db_path: Path to the database file.
    :type db_path: Union[str, Path]
    :param exists: Whether the database file already exists.
    :type exists: bool
    :return: True if database/tables were created, False otherwise.
    :rtype: bool
    '''
    if exists:
        create_prompt = 'The path might belong to a different database. Do you want to create new tables? Enter Yes/No: '
    else:
        create_prompt = "Database does not exist. Create New Database? Enter Yes/No: "
    
    create_choice = input(create_prompt).strip().lower()
    
    if create_choice in ("yes", "y"):
        try:
            Database_CreateTables(db_path)
            logger.info("Tables in DB created" if exists else "Database and tables created")
            return True
        except Exception as e:
            logger.error(f"Error creating database/tables: {e}")
            return False
    return False


def _validate_database_path(db_path: Union[str, Path]) -> bool:
    '''Validate database path and handle creation if needed.
    :param db_path: Path to the database file.
    :type db_path: Union[str, Path]
    :return: True if valid database path with required tables, False otherwise.
    :rtype: bool
    '''
    if os.path.isdir(db_path):
        logger.info("Provided path is a directory not a file. Please reenter filename")
        return False
        
    if not Path(db_path).suffix == ".sqlite":
        logger.info("Provided file is not a .sqlite db. Please try again.")
        return False
    
    if os.path.isfile(db_path):
        logger.info(f"SQL DB for Metadata defined as: {db_path}")
        
        if _validate_database_tables(db_path):
            logger.info("DB with tables exists")
            return True
        else:
            logger.info(f'{db_path} exists, but does not contain required tables yet.')
            return _handle_database_creation_or_tables(db_path, exists=True)
    else:
        # File doesn't exist but has .sqlite extension
        return _handle_database_creation_or_tables(db_path, exists=False)


def get_UserInput(DEFAULT_METADATA_DB: Union[str, Path] = DEFAULT_METADATA_DB, 
                  DEFAULT_MASS_SPEC_DIR: Union[str, Path] = DEFAULT_MASS_SPEC_DIR) -> Tuple[str, str]:
    ''' Function to get the location of the metadata file and the backup server location when starting the script.
    It assumes that the database is in the same folder, if that is correct the user needs to only press enter and not always enter the whole path.
    :param DEFAULT_METADATA_DB: Default path to the metadata database.
    :type DEFAULT_METADATA_DB: Union[str, Path]
    :param DEFAULT_MASS_SPEC_DIR: Default path to the mass spectrometry data directory.
    :type DEFAULT_MASS_SPEC_DIR: Union[str, Path]
    :return: Tuple containing the metadata database path and mass spec directory path.
    :rtype: Tuple[str, str]
    '''
    
    # Handle Metadata Database Input
    use_default_db = input(f'If {DEFAULT_METADATA_DB} is correct location of Metadata press enter, else enter False.')
    
    if use_default_db == '':
        Metadata_DB = DEFAULT_METADATA_DB
        logger.info(f'{Metadata_DB} is file? : {os.path.isfile(Metadata_DB)}')
        
        if os.path.isfile(Metadata_DB):
            if not _validate_database_tables(Metadata_DB):
                logger.info(f'{Metadata_DB} exists, but does not contain required tables yet.')
                if not _handle_database_creation_or_tables(Metadata_DB, exists=True):
                    use_default_db = 'False'  # Force manual input
            else:
                logger.info("DB with tables exists")
        else:
            if not _handle_database_creation_or_tables(Metadata_DB, exists=False):
                use_default_db = 'False'  # Force manual input

    # Manual database path input
    if use_default_db != '':
        Metadata_DB = ''
        
        while not _validate_database_path(Metadata_DB):
            Metadata_DB = input("Set location of SQL Database for Metadata: ")
            Metadata_DB = MakePathNice(Metadata_DB)

    # Handle Mass Spec Directory Input
    use_default_dir = input(f'If {DEFAULT_MASS_SPEC_DIR} is correct location of mass spec data press enter, else enter False.')
    
    if use_default_dir == '':  
        MassSpecDirectory_ToObserve = DEFAULT_MASS_SPEC_DIR
        if os.path.isdir(MassSpecDirectory_ToObserve):
            logger.info(f'Now Observing {MassSpecDirectory_ToObserve} for new .raw files.')
        else:
            use_default_dir = "False"
            logger.info(f'{MassSpecDirectory_ToObserve} is not a valid directory. Please try to reenter.')

    # Manual directory path input
    if use_default_dir != '':
        while True:
            MassSpecDirectory_ToObserve = input("Set location of Raw data Backup Server: ")
            MassSpecDirectory_ToObserve = MakePathNice(MassSpecDirectory_ToObserve)

            if os.path.isdir(MassSpecDirectory_ToObserve):
                logger.info(f'Now Observing {MassSpecDirectory_ToObserve} for new .raw files.')
                break
            else:
                logger.info(f'{MassSpecDirectory_ToObserve} is not a valid directory. Please try to reenter.')

    return Metadata_DB, MassSpecDirectory_ToObserve