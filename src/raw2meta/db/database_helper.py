import sqlite3
from typing import List, Tuple, Optional, Union, Any
import re
import json
import pandas as pd
import os
import dataclasses 
from raw2meta.entity.entities import SampleEntry, ProjectEntry
from raw2meta.config.paths import TempFolder
from raw2meta.config.logger import get_configured_logger
from raw2meta.helper.common import GetFilePath
import time
from pathlib import Path

logger = get_configured_logger(__name__)


def GetTableNames(Metadata_DB: Union[str, Path]) -> List[str]:
    '''Get list of table names from the database.
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :return: List of table names.
    :rtype: List[str]
    '''
    try:
        with sqlite3.connect(Metadata_DB) as con:
            cur = con.cursor()
            names = cur.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
            names = cur.fetchall()
            return [item for t in names for item in t]
    except sqlite3.Error as e:
        logger.error(f"Error getting table names: {e}")
        return []


def SaveToJson(SQLValues_Project: ProjectEntry, SQLValues_Samples: SampleEntry) -> None:
    '''This method saves the SQLValues as a json file in the TempFolder. 
    The name of the file is the ProjectID and SampleName_ID.
    If the ProjectID already exists, it appends the SampleName_ID to the filename.
    :param SQLValues_Project: ProjectEntry dataclass instance.
    :type SQLValues_Project: ProjectEntry
    :param SQLValues_Samples: SampleEntry dataclass instance.
    :type SQLValues_Samples: SampleEntry
    :return: None
    :rtype: None
    '''

    JsonDict = {"SQLValues_Project": dataclasses.asdict(SQLValues_Project),
                "SQLValues_Samples": dataclasses.asdict(SQLValues_Samples)}

    json_object = json.dumps(JsonDict, indent=2)
    FileName = (SQLValues_Project.ProjectID + ".json")

    if FileName in os.listdir(TempFolder):
        FileName = (SQLValues_Project.ProjectID + "__" + SQLValues_Samples.SampleName_ID + ".json")

    FilePathjson = os.path.join(TempFolder, FileName)
    
    try:
        with open(FilePathjson, "w") as outfile:
            outfile.write(json_object)
        logger.info(f"Saved temp file: {FileName}")
    except IOError as e:
        logger.error(f"Error saving temp file {FileName}: {e}")


def ReadJson(Tempfile: Union[str, Path]) -> Tuple[SampleEntry, ProjectEntry]:
    '''Read sample and project data from JSON temp file.
    :param Tempfile: Name of the temp JSON file.
    :type Tempfile: Union[str, Path]
    :return: Tuple of SampleEntry and ProjectEntry instances.
    :rtype: Tuple[SampleEntry, ProjectEntry]
    '''
    
    try:
        with open(os.path.join(TempFolder, Tempfile), 'r') as openfile:
            json_object = json.load(openfile)
            
            # Handle both dictionary and list formats
            samples_data = json_object["SQLValues_Samples"]
            project_data = json_object["SQLValues_Project"]
            
            # If data is a list, convert to dictionary using field names
            if isinstance(samples_data, list):
                logger.warning(f"Converting list format to dict for {Tempfile}")
                # Get field names from SampleEntry dataclass
                sample_fields = [field.name for field in dataclasses.fields(SampleEntry)]
                samples_data = dict(zip(sample_fields, samples_data))
                
            if isinstance(project_data, list):
                logger.warning(f"Converting list format to dict for project data in {Tempfile}")
                # Get field names from ProjectEntry dataclass
                project_fields = [field.name for field in dataclasses.fields(ProjectEntry)]
                project_data = dict(zip(project_fields, project_data))
            
            SQLValues_Samples = SampleEntry(**samples_data)
            SQLValues_Project = ProjectEntry(**project_data)
        
        return SQLValues_Samples, SQLValues_Project
    
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError) as e:
        logger.error(f"Error reading temp file {Tempfile}: {e}")
        # Log the actual JSON structure for debugging
        try:
            with open(os.path.join(TempFolder, Tempfile), 'r') as f:
                content = f.read()
                logger.error(f"JSON content preview: {content[:500]}...")
        except:
            logger.error("Could not read file for debugging")
        raise


class WriteEntries:

    '''This class contains methods to write entries to the database.'''

    def __init__(self, Metadata_DB: Union[str, Path]) -> None:
        '''Initialize with path to metadata database.
        :param Metadata_DB: Path to the metadata database.
        :type Metadata_DB: Union[str, Path]
        :return: None
        :rtype: None
        '''

        self.Metadata_DB = Metadata_DB 
        
        # SQL statements
        self.InsertSQL1_Project = '''INSERT INTO Metadata_Project(ProjectID,ProjectID_Date, Instrument, SoftwareVersion, 
                                                         Method, HPLC, TimeRange, FAIMSattached) 
                            VALUES(?,?,?,?,?,?,?,?);'''
         
        self.InsertSQL2_Sample = '''INSERT INTO Metadata_Sample (SampleName_ID,ProjectID,CreationDate ,Vial,InjectionVolume,
                            InitialPressure_Pump,MinPressure_Pump,MaxPressure_Pump,Std_Pressure_Pump ,AnalyzerTemp_mean,AnalyzerTemp_std) 
                            VALUES( ?,?,?,?,?,?,?,?,?,?,?);'''
 
        self.InsertCorruptSample = '''INSERT INTO Metadata_Sample (SampleName_ID,ProjectID,Error) 
                            VALUES( ?,?,?);'''
                            
        self.RegExProjectID_Query = '''SELECT ProjectID, ProjectID_Date FROM Metadata_Project 
                                                        WHERE ProjectID LIKE ?;'''
        
        self.RegExProjectID_SampleTable_Query = '''SELECT ProjectID, CreationDate FROM Metadata_Sample 
                                                        WHERE ProjectID LIKE ?
                                                        ORDER BY CreationDate DESC LIMIT 1;'''                                                

        self.Count_ProjectID_Query = '''SELECT COUNT(ProjectID) FROM Metadata_Project 
                                        WHERE ProjectID = ?;'''
        
        self.UpdateSQL2_Sample_error = '''UPDATE Metadata_Sample 
                                SET ProjectID = ?,
                                CreationDate =?,
                                Vial =?,
                                InjectionVolume =?,
                                InitialPressure_Pump =?,
                                MinPressure_Pump =?,
                                MaxPressure_Pump =?,
                                Std_Pressure_Pump =?,
                                AnalyzerTemp_mean =?,
                                AnalyzerTemp_std =?,
                                Error = "ErrorUpdated"
                                WHERE SampleName_ID LIKE ?;'''

    def execute_query_pd(self, query: str,params: Optional[Tuple[Any, ...]] = None) -> Union[int, pd.DataFrame]:
        '''Execute predefined queries and return results.
        :param query: Name of the predefined query to execute.
        :type query: str
        :param params: Parameters to bind to the query.
        :type params: Optional[Tuple[Any, ...]]
        :return: Query result as a DataFrame or a single integer value.
        :rtype: Union[int, pd.DataFrame]
        '''
        try:
            with sqlite3.connect(self.Metadata_DB) as con:
                if query == "Count_ProjectID_Query":
                    Result = pd.read_sql_query(self.Count_ProjectID_Query, con, params=params)
                    return Result.iloc[0]["COUNT(ProjectID)"]
                elif query == "RegExProjectID_Query":
                    Result = pd.read_sql_query(self.RegExProjectID_Query, con, params=params)
                    return Result
                elif query == "RegExProjectID_SampleTable_Query":
                    Result = pd.read_sql_query(self.RegExProjectID_SampleTable_Query, con, params=params)
                    return Result

                else:
                    raise ValueError(f"Unknown query: {query}")
        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.error(f"Database error executing {query}: {e}")
            raise

    def write_sample_entries(self, SQLValues_Samples: SampleEntry, SQLValues_Project: Optional[ProjectEntry] = None) -> None:
        '''Write sample and optionally project entries to database.
        :param SQLValues_Samples: SampleEntry dataclass instance.
        :type SQLValues_Samples: SampleEntry
        :param SQLValues_Project: Optional ProjectEntry dataclass instance.
        :type SQLValues_Project: Optional[ProjectEntry]
        :return: None
        :rtype: None   
        '''
        
        try:
            # Only write Project entry when it is not None
            if SQLValues_Project is not None:
                ProjectTuple = dataclasses.astuple(SQLValues_Project)
                Database_writeNewEntry(self.Metadata_DB, self.InsertSQL1_Project, ProjectTuple)

            # Write the sample entry - using Neo format as default
            SamplesTuple = dataclasses.astuple(SQLValues_Samples)
            Database_writeNewEntry(self.Metadata_DB, self.InsertSQL2_Sample, SamplesTuple)
        except Exception as e:
            logger.error(f"Error writing sample entries: {e}")
            raise

    def replace_ErrorFile(self, SQLValues_Samples_update: SampleEntry, SQLValues_Project: Optional[ProjectEntry] = None) -> None:
        '''Replace error file entry with proper metadata.
        :param SQLValues_Samples_update: SampleEntry dataclass instance with updated values.
        :type SQLValues_Samples_update: SampleEntry
        :param SQLValues_Project: Optional ProjectEntry dataclass instance.
        :type SQLValues_Project: Optional[ProjectEntry]
        :return: None
        :rtype: None
        '''
        try:
            if SQLValues_Project is not None:
                ProjectTuple = dataclasses.astuple(SQLValues_Project)
                Database_writeNewEntry(self.Metadata_DB, self.InsertSQL1_Project, ProjectTuple)

            # Reorder tuple for UPDATE statement (sample name goes last)
            SamplesTuple = dataclasses.astuple(SQLValues_Samples_update)
            SQLValues_Samples_update = SamplesTuple[1:] + (SamplesTuple[0],)

            Database_writeNewEntry(self.Metadata_DB, self.UpdateSQL2_Sample_error, SQLValues_Samples_update)
        except Exception as e:
            logger.error(f"Error replacing error file: {e}")
            raise

    def write_CorruptFile(self, SQLValues_Samples_update: Tuple[str, str, str]) -> None:
        '''Write corrupt file entry to database.
        :param SQLValues_Samples_update: Tuple containing (SampleName_ID, ProjectID, Error).
        :type SQLValues_Samples_update: Tuple[str, str, str]
        :return: None
        :rtype: None
        '''
        try:
            Database_writeNewEntry(self.Metadata_DB, self.InsertCorruptSample, SQLValues_Samples_update)
        except Exception as e:
            logger.error(f"Error writing corrupt file: {e}")
            raise


def Database_writeNewEntry(Metadata_DB: Union[str,Path], SQLStatement: str, SQLValues: Tuple) -> None:
    '''Write new entry to database with proper error handling.
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :param SQLStatement: SQL insert or update statement.
    :type SQLStatement: str
    :param SQLValues: Tuple of values to bind to the SQL statement.
    :type SQLValues: Tuple
    :return: None
    :rtype: None
    '''
    try:
        with sqlite3.connect(Metadata_DB) as con:
            cur = con.cursor()
            cur.execute(SQLStatement, SQLValues)
            con.commit()
    except sqlite3.IntegrityError:
        logger.info("File already in DB")
    except sqlite3.Error as e:
        logger.error(f"Database error writing entry: {e}")
        raise

def MissingFilesFromDatabase(Metadata_DB: Union[str, Path], Directory_joined: Union[str, Path]) -> List[str]:
    '''This is the function with which I check which samples are not in the database yet
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :param Directory_joined: Directory where the raw files are located.
    :type Directory_joined: Union[str, Path]
    :return: List of missing raw file names.
    :rtype: List[str]
    '''

    createTable = '''CREATE TABLE IF NOT EXISTS TEMP_forJoin(
    RawfileNames text 
    )'''

    InsertStatement = '''INSERT INTO TEMP_forJoin (RawfileNames) VALUES(?)'''
    
    LeftOuterJoin = ''' SELECT TEMP_forJoin.RawfileNames
    FROM TEMP_forJoin
    LEFT JOIN Metadata_Sample
    ON TEMP_forJoin.RawfileNames =Metadata_Sample.SampleName_ID
    WHERE Metadata_Sample.SampleName_ID IS NULL; '''

    logger.info(Directory_joined)

    try:
        RawfileList = os.listdir(Directory_joined)
    except OSError as e:
        logger.error(f"Error accessing directory {Directory_joined}: {e}")
        return []

    ListMissingRawFiles = []    
    RawfileList = [file for file in RawfileList if re.search(".raw", file)]
    RawfileListTuple = [(x,) for x in RawfileList]
    
    try:
        with sqlite3.connect(Metadata_DB) as con:
            cur = con.cursor()
            cur.execute(createTable)
            cur.execute("DELETE FROM TEMP_forJoin")
            con.commit()
            cur.executemany(InsertStatement, RawfileListTuple)
            con.commit()
            MissingRawFiles = con.execute(LeftOuterJoin).fetchall()
            cur.execute("DELETE FROM TEMP_forJoin")

        for rawFileTup in MissingRawFiles:
            if isinstance(rawFileTup, tuple):
                rawFile, = rawFileTup
            else:
                rawFile = rawFileTup

            if os.path.splitext(rawFile)[1] == ".raw":
               ListMissingRawFiles.append(rawFile)
               
    except sqlite3.Error as e:
        logger.error(f"Database error finding missing files: {e}")
        return []
       
    return ListMissingRawFiles

def GetMonthsInDB(Metadata_DB: Union[str, Path]) -> List[str]:
    '''Get unique months from project dates in database.
    :param Metadata_DB: Path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :return: List of unique months in 'YYYY-MM' format.
    :rtype: List[str]
    '''
    try:
        with sqlite3.connect(Metadata_DB) as con:
            cur = con.cursor()
            Proj_Dates_Tup = cur.execute('''SELECT ProjectID_Date FROM Metadata_Project ''').fetchall()
            con.commit()

        Proj_Dates = [Date_Tup[0] for Date_Tup in Proj_Dates_Tup]
        ProjMonths_unique = sorted({re.sub(r"[0-9]{2}$", "", x) for x in Proj_Dates})

        return ProjMonths_unique
    except sqlite3.Error as e:
        logger.error(f"Database error getting months: {e}")
        return []

