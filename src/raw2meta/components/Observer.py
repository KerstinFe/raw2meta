import sys, os
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from queue import Queue
from raw2meta.config.paths import TempFolder
from raw2meta.helper.common import GetDirectoriesToObserve, MakePathNice
from raw2meta.db.FillDatabase_logic import FillDatabase_Fun, FillDatabase_old, SampleReadyToProcess

from raw2meta.config.logger import  get_configured_logger
from raw2meta.db.database_helper import MissingFilesFromDatabase
from typing import Any, Union, List
from pathlib import Path


logger = get_configured_logger(__name__)

q = Queue()

class MyHandler(LoggingEventHandler):
    def __init__(self, q: Queue) -> None:
        """Custom event handler for monitoring file system events.

        :param q: Queue to put the paths of created files.
        :type q: Queue
        :return: None
        :rtype: None

        """
        super().__init__()
        self.q = q

    def on_created(self, event: Any) -> None:
        """Called when a file is created.
        Edits the q with the path of the created file.

        :param event: The file system event.
        :type event: Any
        :return: None
        :rtype: None
        """
        if event.event_type == "created" and not event.is_directory:
            self.q.put(event.src_path)

    def on_modified(self, event: Any) -> None:
        """Called when a file is modified.
        Nothing happens on modification.
        :param event: The file system event.
        :type event: Any
        :return: None
        :rtype: None
        """
        # Override to prevent logging every modification
        pass

    def on_deleted(self, event: Any) -> None:
        """Called when a file is deleted.
        Nothing happens on deletion.
        :param event: The file system event.
        :type event: Any
        :return: None
        :rtype: None
        """
        # Override to prevent logging every deletion
        pass

    def on_moved(self, event: Any) -> None:
        """Called when a file is moved.
        Nothing happens on move.
        :param event: The file system event.
        :type event: Any
        :return: None
        :rtype: None
        """
        # Override to prevent logging every move
        pass        

def start_watch(path_to_watch: Union[str, Path], q: Queue) -> Any:
    """Starts watching a directory for file system events.
    :param path_to_watch: The directory path to watch.
    :type path_to_watch: Union[str, Path]
    :param q: Queue to put the paths of created files.
    :type q: Queue
    :return: The observer object.
    :rtype: Any
    """
    # logger.info("Watching started")
    
    handler = MyHandler(q)
    observer = Observer()
    observer.schedule(handler, path=path_to_watch, recursive=True)
    observer.daemon = True
    observer.start()

    logger.info("Watching directory: %s", path_to_watch)
    return observer
 
class ObservingFolders():
    
    """
    On our backup server the files are sorted by month, e.g. "202501", "202502", etc.
    Because the date of the project stays the same throughout the measurements it can 
    happen that in the beginning of the month the files will still be sorted into the last month. 
    Therefore the last month will still be monitored for new files. 
    
    On the first of the month I change the folders that are being monitored.
    I let the script rerun the past two months and the now current month to check for missing files before it moves on.
    The observers are then closed, the new months to observe are set and the observers are started again.
    

    """

    def __init__(self, MassSpecDirectory_ToObserve: Union[str, Path], Metadata_DB: Union[str, Path]) -> None:
        '''Initializes the ObservingFolders class.
        :param MassSpecDirectory_ToObserve: The base directory to observe for mass spectrometry data.
        :type MassSpecDirectory_ToObserve: Union[str, Path]
        :param Metadata_DB: The path to the metadata database.
        :type Metadata_DB: Union[str, Path]
        :return: None
        :rtype: None
        '''

        self.MassSpecDirectory_ToObserve = MassSpecDirectory_ToObserve
        self.Metadata_DB = Metadata_DB
        self.DirectoryObserver1 = ""
        self.DirectoryObserver2 = ""
        self.DirectoryObserver3 = ""
        
    def Redefine_Directory( self, stop_event: Any) -> None:
        '''Redefines the directories to observe based on the current date.
        Closes existing observers and starts new ones for the updated directories.  
        :param stop_event: Event to signal stopping the observation loop.
        :type stop_event: Any
        :return: None
        :rtype: None
        '''
        
        logger.info("Running Redefine Directory")

        while not stop_event.is_set():

            logger.info("Rerunning past 2 months")
            self.RerunningTwoMonths(stop_event)
            logger.info("Finished rerunning")

            self.ClosingObservations()
            logger.info("Observers closed")

            if stop_event.is_set():
                break

            Directories, timeuntilFirst = GetDirectoriesToObserve(self.MassSpecDirectory_ToObserve)

            logger.info("Folders updated, now restarting observer")
            
            for indxM, MonthDir in enumerate(Directories[1:]):
                if os.path.isdir(MonthDir):
                    if indxM == 0:
                        self.DirectoryObserver1 = start_watch(MonthDir, q)
                    elif indxM == 1:
                        self.DirectoryObserver2 = start_watch(MonthDir, q)
                    elif indxM == 2:
                        self.DirectoryObserver3 = start_watch(MonthDir, q)


            logger.info("Observer started")
            stop_event.wait(timeout=timeuntilFirst)


    def ClosingObservations(self) -> None:
        '''Closes all active directory observers.
        :return: None
        :rtype: None
        '''

        for observer in [self.DirectoryObserver1, self.DirectoryObserver2, self.DirectoryObserver3]:
            if not isinstance(observer, str):              
                if observer.is_alive():
                    observer.stop()
                    observer.join()

  
       
    def RerunningTwoMonths(self, stop_event: Any) -> None:

       '''
        Check the past two months whether files are missing in the database and 
        whether there are temporary files that still need to be inserted. 
        :param stop_event: Event to signal stopping the observation loop.
        :type stop_event: Any
        :return: None
        :rtype: None
       '''
       Directories, _ = GetDirectoriesToObserve(self.MassSpecDirectory_ToObserve) 
       DoubleCheck = Directories[:2]  # Only the last two months       
       
       loop_stop_event1 = False

       while (not stop_event.is_set()) and (not loop_stop_event1):
            try:
                for Directory in DoubleCheck:
                    if stop_event.is_set():
                        break    
                    Directory_joined = os.path.join(self.MassSpecDirectory_ToObserve, Directory)
                 
                    ListMissingRawFiles = MissingFilesFromDatabase(self.Metadata_DB, Directory_joined)
            
                    if not ListMissingRawFiles:
                        logger.info(f"Missing from Directory: {len(ListMissingRawFiles)} files")
                        continue

                    logger.info(f"Missing from Directory: {len(ListMissingRawFiles)} files")

                    for rawFile in ListMissingRawFiles:  
                        rawFile = MakePathNice(rawFile)
                        if stop_event.is_set():
                            break                                 
                        if SampleReadyToProcess(rawFile, Directory_joined, self.Metadata_DB, stop_event=stop_event):
                            rawFile_fullpath = os.path.join(Directory_joined, rawFile)
                            FillDatabase_Fun(rawFile_fullpath, self.Metadata_DB, stop_event=stop_event)

                logger.info("Start going through temporary files")
                temp_files = os.listdir(TempFolder)
                logger.info("%d files in TEMP folder", len(temp_files))

                if not stop_event.is_set():
                    for file in temp_files:
                        if stop_event.is_set():
                            break

                        FillDatabase_old(file, self.Metadata_DB)

                loop_stop_event1 = True        

                logger.info("Temporary files done")

            except KeyboardInterrupt:
                stop_event.set()
                logger.info("Keyboard Interrupt triggered, exiting RerunningTwoMonths")
                # sys.exit(0)
                
       for Directory in DoubleCheck:
            Directory_joined = os.path.join(self.MassSpecDirectory_ToObserve, Directory)
            ListMissingRawFiles = MissingFilesFromDatabase(self.Metadata_DB, Directory_joined)
            logger.info("Missing from directory %s after checking past two months: %d files", Directory, len(ListMissingRawFiles))

