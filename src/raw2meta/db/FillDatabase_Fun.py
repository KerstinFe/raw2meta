import os
import re
from typing import Optional, Union
from raw2meta.helper.common import SplitProjectName, get_ProjectID_withClosestDate
from raw2meta.helper.Exceptions import NoFittingProjectFound, HandlingEmptyFileError, HandlingCorruptFileError
from raw2meta.components.GetMetadata import MetadataLists
from raw2meta.config.paths import TempFolder
from raw2meta.db.database_helper import ReadJson,  SaveToJson, WriteEntries
from raw2meta.config.logger import get_configured_logger
from System import ArgumentOutOfRangeException, IndexOutOfRangeException
from raw2meta.config.configuration import DaysWaiting
from datetime import datetime
import pandas as pd
from pathlib import Path
from raw2meta.entity.entities import SampleEntry, ProjectEntry

logger = get_configured_logger(__name__)

class Execute_CreateSQLdbCode(): 
    
    ''' These methods now insert data into the database.
    The logic is quite specific to how we aquire data. 
    '''
    
    def __init__(self,Metadata_DB: Union[str, Path]) -> None:
        """Initialize the Execute_CreateSQLdbCode class.

        :param Metadata_DB: Path to the metadata database.
        :type Metadata_DB: Union[str, Path]
        :return: None
        :rtype: None
        """
        self.WriteEntries = WriteEntries(Metadata_DB)
        self.replace_ErrorFile = self.WriteEntries.replace_ErrorFile    
        self.write_sample_entries = self.WriteEntries.write_sample_entries
        self.write_CorruptFile = self.WriteEntries.write_CorruptFile
        self.execute_query_pd = self.WriteEntries.execute_query_pd
        self.Metadata_DB = Metadata_DB
        
        # Query name constants
        self.COUNT_PROJECT_QUERY = "Count_ProjectID_Query"
        self.REGEX_PROJECT_QUERY = "RegExProjectID_Query"
        self.REGEX_PROJECT_QUERY_SAMPLE_TABLE = "RegExProjectID_SampleTable_Query"

    def _is_standard_sample(self, sample_name: str) -> bool:
        """Check if sample is a standard (Hela or other standard).
        :param sample_name: Name of the sample.
        :type sample_name: str
        :return: True if sample is a standard, False otherwise.
        :rtype: bool
        """
        return bool(re.search("HSstd", sample_name) or re.search("[Ss]tandar[dt]", sample_name))

    def _find_matching_project(self, project_id: str) -> Optional[str]:
        '''Find matching project ID, returns None if no exact match found.
        :param project_id: Project ID to search for.
        :type project_id: str
        :return: Matching project ID or None.
        :rtype: Optional[str]
        '''
        row_count = self.execute_query_pd(self.COUNT_PROJECT_QUERY, params=(project_id,))
        if row_count > 0:
            return project_id
        return None

    def FillDatabase(self,RawfilePath: Union[str, Path]) -> None:     
        '''
        To understand the logic in this function one needs to know that our files are always named with Machine_Date_Initial
        The date is not changed during the measurements. E.g. if a project starts on 01.07.2025 and ends on 05.07.2025 the date will stay 20250701.
        Therefore, I use this first partas a Project ID. 
        
        If the project does not exist, I enter first the project metadata and then the sample metadata.
        If it exists, I just enter the sample metadata into the database.
        
        However, we regularly measure Helas before and after (and sometimes during) the Q for quality control. These are marked with "HSstd".
        Additionally, when we sometimes use other project specific standards. 
        Helas will always get the actual date because they are used to check machine performance and need to be traced back.
        
        I want to match the Helas around the project we are measuring with the project ID.
        Like this we can check the performance of the machine with Hela while looking at our project QC parameters.
        
        At leat one Hela is usually measured before the project. I have to wait with inserting it into the database 
        because at that point the project ID is not known yet and I want the Project metadata to be from the actual samples and not from the Hela. 
        Therefore, when there is no fitting Project ID I am writing the Hela into a .json first so it can be matched later, when the project started.
        
        Because the dates can vary in the last two digits between project and Hela, I am using the ProjectID_regex/ ProjectID_regex_sql
        to check whether the project already exists.
        
        When a new project is created the function goes into the TEMP folder and checks whether there are files that match the project ID and can be 
        inserted into the db. 

        :param RawfilePath: Path to the raw file.
        :type RawfilePath: Union[str, Path] 
        :return: None
        :rtype: None
        
        '''
      
        try:
            MetadataOutput = MetadataLists(RawfilePath)
            SQLValues_Samples , SQLValues_Project= MetadataOutput.GetArray_SampleMetadata()
           
        except ArgumentOutOfRangeException:
            raise HandlingCorruptFileError
            
        except IndexOutOfRangeException: 
            raise HandlingEmptyFileError

        self._handle_hela_and_project_matching(SQLValues_Samples, SQLValues_Project)
        

    def FillDatabaseWithJson(self, Tempfile: Union[str, Path]) -> None:

        ''' This is to shorten the code above.
        I am reading in the .json file, and check again for the Project ID.
        This is important because some hela standards are measured without a matching project following them.
        For example if they were maintained by a different person than who measures later.
        These samples are inserted later with their own project ID. 
        
        Sometimes people are lucky and measure multiple projects in the same month on the same machine.
        The helas around these projects need to be matched to the correct one. I decided to just take the one with the closest date. 
        This might not always be ideal and correct but for now is the simplest solution. 
        If no fitting project is found an exception is raised and the .json file is kept for a couple of days (specified in params)

        :param Tempfile: Path to the temporary JSON file.
        :type Tempfile: Union[str, Path]
        :return: None
        :rtype: None
        '''
               
        _, _, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(Tempfile)

        Regex_Proj = self.execute_query_pd(self.REGEX_PROJECT_QUERY, params=(ProjectID_regex_sql,))

        if len(Regex_Proj)==0:
            logger.info(f"No fitting Project found for file {Tempfile}.")
            raise NoFittingProjectFound
            
        else:
            logger.info(f'TEMP file {Tempfile} used')
            SQLValues_Samples, SQLValues_Project = ReadJson(Tempfile)
            SQLValues_Samples.ProjectID = get_ProjectID_withClosestDate(Regex_Proj, ProjectID_Date)

            self.write_sample_entries( SQLValues_Samples, SQLValues_Project=None)

            os.remove(os.path.join(TempFolder, Tempfile))
            logger.info(f'TEMP Tempfile {Tempfile} deleted')

    def FillDatabaseWithJson_KeepProject(self, Tempfile: Union[str, Path]) -> None:  

        ''' This is mostly for Hela standards that are measured without a matching project.
        This happens for example when some is maintaining the machine without using it for their project afterwards.
        These will not fit into the logic of the functions above but I still want them to be inserted after a couple of days.
        
        :param Tempfile: Path to the temporary JSON file.
        :type Tempfile: Union[str, Path]
        :return: None
        :rtype: None
        '''        

        logger.info(f'TEMP file {Tempfile} used, keep Project')

        SQLValues_Samples, SQLValues_Project = ReadJson(Tempfile)

        self.write_sample_entries(SQLValues_Samples, SQLValues_Project)

        os.remove(os.path.join(TempFolder, Tempfile))

        logger.info(f'TEMP Tempfile {Tempfile} deleted')

    def FillDatabase_Error(self,RawfilePath: Union[str, Path], Error: str) -> None:
      
        '''Some files can be corrupt. Because I want to connect the database to a dashboard where users can follow their projects
         I want these files to be reported as corrupt so the user has the option to fix or report the problem.
         Therefore I add them only with sample and project ID and with an Error attached to them.

        :param RawfilePath: Path to the raw file.
        :type RawfilePath: Union[str, Path]
        :param Error: Description of the error encountered.
        :type Error: str
        :return: None
        :rtype: None
        '''
    
        ProjectID, _,_,_ = SplitProjectName(RawfilePath)
        SQLValues_Samples_Error = [RawfilePath, ProjectID, Error]

        matching_project = self._find_matching_project(ProjectID)
        if matching_project:
            SQLValues_Samples_Error[1] = matching_project
            
        self.write_CorruptFile(SQLValues_Samples_Error)

    def ReplaceErrorFile(self,RawfilePath: Union[str, Path]) -> None:
        
        '''Some files can be corrupt. This can be due to copying issues during backup.
        This function can be used to replace the entry of the corrupt file after it has been replaced.
        I am aiming at including it into the main logic but have not done this so far. 
        :param RawfilePath: Path to the raw file.
        :type RawfilePath: Union[str, Path]
        :return: None
        :rtype: None
        '''
         
       
        try:
            MetadataOutput = MetadataLists(RawfilePath)
            SQLValues_Samples , SQLValues_Project= MetadataOutput.GetArray_SampleMetadata()
            
        except ArgumentOutOfRangeException:
            raise HandlingCorruptFileError
            
        except IndexOutOfRangeException: 
            raise HandlingEmptyFileError
        
       
        self._handle_hela_and_project_matching_ReplaceError(SQLValues_Samples, SQLValues_Project)

### to do: return true or false if saved as json so it does not print db updated in _logic if it is saved as json
    def _handle_hela_and_project_matching(self, SQLValues_Samples: SampleEntry, SQLValues_Project: ProjectEntry) -> None:
        """Handle matching of Hela standards and project entries in the database.
        :param SQLValues_Samples: SampleEntry dataclass instance with sample metadata.
        :type SQLValues_Samples: SampleEntry
        :param SQLValues_Project: ProjectEntry dataclass instance with project metadata.
        :type SQLValues_Project: ProjectEntry   
        :return: None
        :rtype: None
        """

        _, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(SQLValues_Project.ProjectID)
        matching_project = self._find_matching_project(SQLValues_Project.ProjectID)
        if matching_project:
            # Exact match: add sample to existing project
            self.write_sample_entries(SQLValues_Samples, SQLValues_Project=None)
        else:
            if self._is_standard_sample(SQLValues_Samples.SampleName_ID):
                # Standard sample: try to find close project by regex/date
                regex_matches = self.execute_query_pd(self.REGEX_PROJECT_QUERY, params=(ProjectID_regex_sql,))
                if len(regex_matches) == 0: # no matching project found even with regex, so I keep it as json for x days (specified in params) or until project samples are measured
                    SaveToJson(SQLValues_Project, SQLValues_Samples)
                    logger.info("Data stored in temp file")
                else:
                    # returns the Project ID and Date of the last measured sample that matches the Regex
                    regex_matches_Sample = self.execute_query_pd(self.REGEX_PROJECT_QUERY_SAMPLE_TABLE, params=(ProjectID_regex_sql,))
                 
                    if len(regex_matches_Sample) != 0:
                        TimeDiff = pd.Timestamp(regex_matches_Sample["CreationDate"].iloc[0]).to_julian_date() - pd.Timestamp(SQLValues_Samples.CreationDate).to_julian_date()
                      
                         # if TimeDiff.days > DaysWaiting: # either not belonging to a project, and just a maintenance standard, or new project coming
                        if TimeDiff > DaysWaiting:
                            SaveToJson(SQLValues_Project, SQLValues_Samples)
                            logger.info(f"Data stored in temp file, the last project is longer than {DaysWaiting} days ago")

                        else:  
                            SQLValues_Samples.ProjectID = get_ProjectID_withClosestDate(regex_matches, ProjectID_Date)
                            self.write_sample_entries(SQLValues_Samples, SQLValues_Project=None)
                    else: # this should never happen!
                        logger.error("Matching Project ID in Project df but no matching Project ID in Sample df")
            else:
                # Not a standard: create new project and process pending temp files
                self.write_sample_entries(SQLValues_Samples, SQLValues_Project)
                self._process_pending_temp_files(ProjectID_regex)

    def _process_pending_temp_files(self, project_regex: str) -> None:
        """Process temp files that match the project regex.
        :param project_regex: Regex pattern to match project IDs.
        :type project_regex: str
        :return: None
        :rtype: None
        """
        for file in os.listdir(TempFolder):
            if re.search(project_regex, file):
                try:
                    self.FillDatabaseWithJson(file)
                except NoFittingProjectFound:
                    logger.info("Project not in DB yet.")

    def _handle_hela_and_project_matching_ReplaceError(self, SQLValues_Samples: SampleEntry, SQLValues_Project: ProjectEntry) -> None:
        """Handle matching of Hela standards and project entries when replacing an error file.
        :param SQLValues_Samples: SampleEntry dataclass instance with sample metadata.
        :type SQLValues_Samples: SampleEntry
        :param SQLValues_Project: ProjectEntry dataclass instance with project metadata.
        :type SQLValues_Project: ProjectEntry
        :return: None
        :rtype: None
        """

        _, _, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(SQLValues_Project.ProjectID)
        
        matching_project = self._find_matching_project(SQLValues_Project.ProjectID)
        
        if matching_project:
            # SQLValues_Samples.ProjectID = matching_project
            self.replace_ErrorFile(SQLValues_Samples, SQLValues_Project=None)

        else:
            if self._is_standard_sample(SQLValues_Samples.SampleName_ID):
                regex_matches = self.execute_query_pd(self.REGEX_PROJECT_QUERY, params=(ProjectID_regex_sql,))
                
                if len(regex_matches) == 0:
                    self.replace_ErrorFile(SQLValues_Samples, SQLValues_Project)                          
                else: 
                    '''using the time difference like this doesnt make sense here 
                        because it is not always clear when the corrupt file will be replaced 
                        and that there might already a newer project'''
                 
                    SQLValues_Samples.ProjectID = get_ProjectID_withClosestDate(regex_matches, ProjectID_Date)
                    self.replace_ErrorFile(SQLValues_Samples, SQLValues_Project=None)    
            else:
                self.replace_ErrorFile(SQLValues_Samples, SQLValues_Project)
      




