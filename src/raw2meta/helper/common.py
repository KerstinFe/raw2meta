from datetime import date
import  os
import re
from typing import Tuple, List, Union, Any, Optional, Type
from pathlib import Path
import dateutil.relativedelta
import pandas as pd
from raw2meta.RawFileReader.ImportRawFileReaderFunctions import RawFileReaderAdapter
from raw2meta.config.logger import get_configured_logger
from raw2meta.helper.Exceptions import HandlingCorruptFileError

# create a module-level logger instance
logger = get_configured_logger(__name__)

def SplitProjectName(Name: str) -> Tuple[str, str, str, str]:
    '''Split project name into components.
    :param Name: Full name of the file.
    :type Name: str
    :return: Tuple of (ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date)
    '''

    Name = os.path.splitext(Name)[0]
    Name = os.path.basename(Name)
    Names_splitted = Name.split("_")
    ProjectID = (Names_splitted[0]+"_"+Names_splitted[1]+"_"+Names_splitted[2])
    ProjectID_regex = (Names_splitted[0]+"_"+re.sub("[0-9]{2}$", "[0-9]{2}", Names_splitted[1])+"_"+Names_splitted[2])
    ProjectID_regex_sql =(Names_splitted[0]+"_"+re.sub("[0-9]{2}$", "__", Names_splitted[1])+"_"+Names_splitted[2]+ "%")
    ProjectID_Date = Names_splitted[1]
    
    return ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date

class RawFileReaderManager:
    '''Context manager that opens a RawFileReader and guarantees it is disposed.

    This manager converts lower-level/native exceptions raised by the
    vendor pythonnet wrappers into a consistent HandlingCorruptFileError so
    upstream code can handle problematic files without the whole process
    crashing.
    '''
    def __init__(self, RawfilePath: Union[str, Path]) -> None:
        """Context manager that opens a RawFileReader and guarantees it is disposed.
        This manager converts exceptions raised by the pythonnet wrappers into 
        a consistent HandlingCorruptFileError so upstream code can handle problematic 
        files without the whole process crashing.

        :param RawfilePath: Path to the raw file.
        :type RawfilePath: Union[str, Path]
        :return: None
        :rtype: None
        """
        self.RawfilePath = RawfilePath
        self.rawFile = None

    def __enter__(self) -> Any:
        """Enter the runtime context related to this object.
        :return: The opened raw file object.
        :rtype: Any
        """
        try:
            self.rawFile = RawFileReaderAdapter.FileFactory(self.RawfilePath)
            return self.rawFile
        except Exception as e:
            if self.rawFile is not None:
                try:
                    self.rawFile.Dispose()
                except Exception as cleanup_ex:
                    logger.warning(f"Cleanup failed for {self.RawfilePath}: {cleanup_ex}")
            logger.error(f"Failed to open raw file {self.RawfilePath}: {e}", exc_info=True)
            raise HandlingCorruptFileError() from e
           

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], 
                 tb: Optional[Any]) -> bool:
        """Exit the runtime context and clean up the raw file object.
        :param exc_type: The exception type.
        :type exc_type: Optional[Type[BaseException]]
        :param exc_value: The exception value.
        :type exc_value: Optional[BaseException]
        :param tb: The traceback object.
        :type tb: Optional[Any]
        :return: False to propagate exceptions, True to suppress.
        """

        if self.rawFile is not None:
            try:
                self.rawFile.Dispose()
            except Exception as disp_ex:
                logger.error(f"Error disposing raw file {self.RawfilePath}: {disp_ex}", exc_info=True)
                # don't raise here, just log
        if exc_type:
            logger.error(f"Exception while processing {self.RawfilePath}: {exc_value}", exc_info=True)
            # return False so Python re-raises original error
        return False
   

def MakePathNice(PathToMakeNice: Union[str, Path]) -> Union[str, Path]:
    '''Clean and normalize file path.
    :param PathToMakeNice: The file path to clean.
    :type PathToMakeNice: Union[str, Path]
    :return: Cleaned file path.
    :rtype: Union[str, Path]
    '''

    PathToMakeNice = PathToMakeNice.replace('"', '')
    PathToMakeNice = PathToMakeNice.replace("'", "")
    PathToMakeNice = Path(PathToMakeNice).as_posix()

    return PathToMakeNice

def get_ProjectID_withClosestDate(Regex_Proj: pd.DataFrame, ProjectID_Date: str) -> str:
    '''Find the closest project date match.
    :param Regex_Proj: DataFrame with project IDs and dates.
    :type Regex_Proj: pd.DataFrame
    :param ProjectID_Date: The project date to match.
    :type ProjectID_Date: str
    :return: The ProjectID with the closest date.
    :rtype: str
    '''
    Dist = list(Regex_Proj["ProjectID_Date"])
    Dist = [int(x)-int(ProjectID_Date)  for x in Dist]
    nearestDate = Dist.index(min(Dist))
    return Regex_Proj["ProjectID"][nearestDate]

    

def GetDirectoriesToObserve(MassSpecDirectory_ToObserve: Union[str, Path]) -> Tuple[List[str], int]:

    '''Get directories to observe for mass spectrometry data.
    :param MassSpecDirectory_ToObserve: The base directory to observe for mass spectrometry data.
    :type MassSpecDirectory_ToObserve: Union[str, Path]
    :return: None
    :rtype: None
    '''

    currentMonth_Date = date.today()
    months = [-2, -1, 0, +1]  #  last month, current month, next month
    month_strs = [
        (currentMonth_Date + dateutil.relativedelta.relativedelta(months=m)).strftime("%Y%m")
        for m in months
    ]
    Directories = [
        Path(os.path.join(MassSpecDirectory_ToObserve, m)).as_posix()
        for m in month_strs
    ]

    nextMonth = currentMonth_Date + dateutil.relativedelta.relativedelta(months=+1)
    nextMonth = nextMonth.replace(day=1)  # Adjust to the first of the next month
    timeuntilFirst = (nextMonth - currentMonth_Date).total_seconds()

    return Directories, timeuntilFirst

def GetFilePath(FileName: str, Directory_joined: Union[str, Path]) -> Tuple[str, str]:
    '''Get the full file path for a given file name.
    :param FileName: The name of the file.
    :type FileName: str
    :param Directory_joined: The base directory to join with the file name.
    :type Directory_joined: Union[str, Path]
    :return: Tuple of (Full file path, Base filename)
    :rtype: Tuple[str, str]
    '''
    BaseFilename = os.path.basename(FileName)

    if os.path.isfile(FileName) is False:
        FileName = os.path.join(Directory_joined,FileName)

        if os.path.isfile(FileName) is False:
            partDir = Path(Directory_joined).parts
            FileDate = re.sub(r"[^0-9]{6}[^0-9]{2}", r"[^0-9]{6}", BaseFilename.split("_")[1])
            FileName = os.path.join(partDir[0], FileDate, BaseFilename)

            if os.path.isfile(FileName) is False:    
                logger.error(f"File not found: {FileName}")

    return FileName, BaseFilename
