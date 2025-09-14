
from pathlib import Path
import os
from datetime import datetime
from raw2meta.config.loadParams import PACKAGE_LOCATION, load_params

# Always resolve relative to the current file
BASE_DIR = Path(__file__).resolve().parent

PACKAGE_ROOT = Path(__file__).resolve().parents[1]  # Points to raw2meta/

CONFIG_DIR = PACKAGE_ROOT / "config"

# For a file in the same folder:
config_path = BASE_DIR / "MachinesDict.json"


Thermo_path = PACKAGE_ROOT / "RawFileReader" / "RawFileReader_dll" / "Net471"
TempFolder = PACKAGE_LOCATION /  "TEMP"

# logfiles
currentdate = datetime.now().strftime("%Y%m%d")
LOGS_DIR = PACKAGE_LOCATION / "Logs"
Logfile_corrupt = LOGS_DIR / f"{currentdate}_FilesWithError.log"
Logfile_empty = LOGS_DIR / f"{currentdate}_EmptyFiles.log"
Logfile_Integrity = LOGS_DIR / f"{currentdate}_AlreadyinDB.log"

# Get runtime parameters
PARAMS = load_params()

# Derived configuration (combines internal config with external params)
metadata_db_path = PARAMS.get('data', {}).get('metadata_db_path')

if metadata_db_path == "Metadata.sqlite":
    DEFAULT_METADATA_DB = Path(PACKAGE_LOCATION /"Metadata.sqlite").as_posix()
elif os.path.isfile(metadata_db_path):
    DEFAULT_METADATA_DB = metadata_db_path
else:
    DEFAULT_METADATA_DB = Path(PACKAGE_LOCATION /metadata_db_path).as_posix()

mass_spec_directory = PARAMS.get('data', {}).get('mass_spec_directory')
DEFAULT_MASS_SPEC_DIR = mass_spec_directory

