from raw2meta.config.paths import PACKAGE_ROOT
from raw2meta.config.logger import get_configured_logger
from raw2meta.config.loadParams import PACKAGE_LOCATION, load_params

logger = get_configured_logger(__name__)

SUPPORTED_FILE_EXTENSIONS = [".raw"]

MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_BASE = 5  # seconds

PARAMS = load_params()

DaysWaiting = PARAMS.get('processing', {}).get('days_waiting')
FileWaitTime = PARAMS.get('processing', {}).get('file_wait_time_minutes')
MinFileSize = PARAMS.get('processing', {}).get('min_file_size_kb')

TablesMetaData = PARAMS.get('data', {}).get('Tables_Metadata_db')

MachinesDict = PARAMS.get('MachinesDict', {})
HPLCDict = PARAMS.get('HPLCDict', {})

logger.info(f"Derived configuration:")
logger.info(f"Waiting {DaysWaiting} days for old projects (DaysWaiting)")
logger.info(f"Waiting {FileWaitTime} min for copying (FileWaitTime)")
logger.info(f"Minimum file size for non corrupt files: {MinFileSize} KB (MinFileSize)")

        


