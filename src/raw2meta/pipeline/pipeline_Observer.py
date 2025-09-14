from pathlib import Path
import sys
from raw2meta.components.UserInput import get_UserInput
from raw2meta.components.Observer import  ObservingFolders, q
from raw2meta.config.logger import get_configured_logger
from raw2meta.config.configuration import FileWaitTime
from raw2meta.db.FillDatabase_logic import FillDatabase_Fun, SampleReadyToProcess
import threading
import time
from queue import Empty

logger = get_configured_logger(__name__)

logger.info("Starting Script")

def main():
    Metadata_DB, MassSpecDirectory_ToObserve = get_UserInput()

    stop_event = threading.Event()
    observer = ObservingFolders(MassSpecDirectory_ToObserve, Metadata_DB)
    observer_thread = threading.Thread(target=observer.Redefine_Directory, args=(stop_event,))
    observer_thread.daemon = False
    observer_thread.start()
    logger.info("Started Observing Folders")

    NoStopSignal = True   
   
    while not stop_event.is_set() and NoStopSignal:
        try:
            file = q.get(block=False, timeout=1)
            file = Path(file).as_posix()

            if Path(file).suffix == ".raw":
                logger.info(f"New file {file} detected by Observer, waiting for {FileWaitTime} minutes.")

                if stop_event.wait(timeout=60 * FileWaitTime):
                    # stop_event set during wait — break main loop to start shutdown
                    break  # I am waiting 5 minutes hoping that then the file will be fully copied, because some files escape the copy check

                if SampleReadyToProcess(file, MassSpecDirectory_ToObserve, Metadata_DB, stop_event=stop_event):

                    FillDatabase_Fun(file, Metadata_DB, stop_event=stop_event)

                    logger.info(f"File {file} processed and database updated, continue observing.")

        except KeyboardInterrupt:
            logger.info("\n Stopping threads...")
            NoStopSignal = False
            stop_event.set()
            observer.ClosingObservations()
            logger.info("closed Observers")
            observer_thread.join()
            logger.info("Main thread exiting cleanly.")
            sys.exit(0)
                        
        except Empty:
            continue

if __name__ == "__main__":
    main()

