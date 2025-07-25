# raw2meta
Observes backup directory for new raw files. The metadata is extracted and stored in an SQLite database for later use.  
Only runs on windows because of dependencies in the RawFileReader from Thermo.  
Ongoing project

#### features: 
- Creates a sqlite db with two relational tables:
  1. Project metadata:
     - ProjectID
     - Instrument
     - SoftwareVersion
     - Method
     - HPLC
     - TimeRange
     - FAIMSattached

  2. Sample metadata
     - SampleName_ID
     - ProjectID (foreign key)
     - CreationDate
     - Vial
     - InjectionVolume
     - InitialPressure_Pump
     - MinPressure_Pump (only for Neo)
     - MaxPressure_Pump (only for Neo)
     - Std_Pressure_Pump (only for Neo)
     - AnalyzerTemp_mean
     - AnalyzerTemp_std
     - Error

- Instruments and HPLCs are defined in a Machines Dictionary and an HPLC Dictionary, which are currently hard-coded in the ETL_Functions.py file. These dictionaries translate the names of machines in the instrument method into shorter, more readable names.
- The time range is the time range of the gradient.
- The method is the name of the method file.
- The pump pressure and analyzer temperature are recorded to observe the stability of the measurements. Only 1000 scans over the entire range are extracted for pump pressure to reduce time. The pump pressure is only recorded on the Thermo Vanquish Neo system, not the EvoSep or the EASY nLC-1200, and therefore also only available for these samples.
- For analyzer temperature, 500 scans are extracted over the entire range.
- std = standard deviation
  
- The project ID is defined as the first three parts of the raw file name. In our case, they are standardized as follows:  \[machinename]\_\[date]\_\[initials of person who measures]. 
- When a QC (quality control) Hela standard is measured at the beginning of a project, the extracted metadata is stored in a .json file in the TEMP folder. Later, when the first sample of the project is measured, the metadata is extracted to ensure that the sample receives the correct project ID and that the extracted metadata is not that of the Hela standard.

- Creates log files for corrupt or empty files, or for files that were in the database but somehow skipped the initial check, so they can be reviewed later.

- Files that appear empty because I cannot detect scans and are below 15 KB are marked as corrupt and inserted into the database as such to avoid reprocessing.
- I've noticed that sometimes files appear empty at the MS level, yet they show traces of pump pressure and are large in size. It seems like these files are accessible even though they have not been fully copied. Therefore, I include multiple waiting times and don't insert them into the database as corrupt so they can be entered later. (I will improve the processing of these files in the future.)


 
#### To Do:
- [ ] Include files from Bruker machines
- [ ] Clean up the code, remove repetitions, and make it more generic
- [ ] Reprocess files that appear empty but are just not completely copied
- [ ] Include reprocessing of a corrupt file in the main script when it is replaced by the correct file
- [ ] Start observing the folder for the next month as soon as it is created, not at the beginning of the month


#### Dependencies:
Uses Python 3.12
- pandas (V 2.3.1), numpy (V 2.3.1), pythonnet (V 3.0.5) and watchdog (V 6.0.0) 
- [RawFileReaderFiles from Net471](https://github.com/thermofisherlsms/RawFileReader)  
  These files must be unblocked after downloading.  
  If you want to use them, you must agree to the license agreement.  

- Only runs on Windows because some of the Thermo RawFileReader functions depend on Windows   

#### structure:
- The project consists three main files:
  + The observer checks the backup directory for new raw files in subfolders. Since we have subfolders organized by month, the script changes which folders it observes every month.
  + The backlog processor looks for all raw files in the directory and adds them to the database, making it easy to add older files.
  + The file that contains the classes and functions.  
   
- There are three additional helper scripts:
  + The CheckIngestionStatus script looks at which months the files in the database come from, how many are present, and how many are missing.
  + The InsertCorruptFileEntry script adds a corrupted file. This is not part of the main script yet. This way, the file can be checked manually. It tries to read the file again before inserting it as corrupt.
  + The ReplaceCorruptFileEntry script updates the entry in the database after the corrupt file is replaced.

All scripts can be started with batch scripts. 
    
#### Folder Structure:
raw2meta/  
├── ETL_Functions.py  
├── ETL_BacklogProcessor.py   
├── run_BacklogProcessor.bat   
├── ETL_Observer.py    
├── run_Observer.bat   
├── CheckIngestionStatus.py     
├── run_CheckIngestionStatus.bat    
├── ETL_ReplaceCorruptFileEntry.py    
├── run_ReplaceCorruptFile.bat    
├── ETL_InsertCorruptFileEntry.py    
├── run_InsertCorruptFile.bat      
├── Metadata.sqlite  
├── RawFileReader_dll/    
│   └── Net471/    
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.RawFileReader.dll    
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.MassPrecisionEstimator.dll    
├── TEMP/    

If run via bat file:    
├── WinPython/    
│   └── python/  
│      └──python.exe  
