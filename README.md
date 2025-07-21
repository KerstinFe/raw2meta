# raw2meta
Observes backup directory for new raw files. Metadata is extracted and stored in sqlite database for later use. 

#### features: 
- Creates a sqlite db with two relational tables:
  1. Project Metadata:
     - ProjectID
     - Instrument
     - SoftwareVersion
     - Method
     - HPLC
     - TimeRange
     - FAIMSattached

  2. Sample Metadata
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

- Instruments and HPLCs are defined in a Machines Dictonary and a HPLC Dictionary that are so far hard coded in the sqlLite_Function.py. The dictionaries are used to translate the names of machines in the instrument method to shorter and readable names.
- The time range is the time range of the gradient.
- The method is the name of the method file.
- The pump pressure and analyzer temperature are recorded to observe stability of measurements. For pump pressure only 1000 scans over the whole range are extracted to reduce time. This is only recoreded for the Thermo Vanquish Neo system and not for EvoSep or EASY nLC-1200.  For the analyzer temperature 500 scans are extraced over the whole range. std = standard deviation
  
- The ProjectID is defined as the first 3 parts of the rawfile name. In our case they are standardised to \[machinename]\_\[date]\_\[initials of person who measures]. 
- When a QC Hela standard is measured before a project, the extracted metadata is stored in a .json file in the TEMP folder and later extracted when the first sample of the project is measured to make sure it gets the correct project ID and the extraced metadata information is not that of the Hela standard.

- The project contains two main files.
  + One with the observer to check for new rawfiles in subfolders the backup directory. We have subfolders per month so the script changes every month which folders it observes.
  + One that looks for all raw files in the directory to add them into the database so that also older files can be added to the database easily.
 
- Creates log files for corrupt files, empty files, or if files were already in database and somehow skipped the initial check so these can later be checked.
  + Corrupt files can also be files that are not readable with the RawFileReader framework from Thermo or where the machine and hplc dictionary defines the wrong machine and therefore the script tries to read traces in rawfiles that are not recorded. 
 
#### to do:
- [ ] include bruker machines
- [ ] clean up code and make it more generic

#### dependencies:
Uses python 3.12
- pandas (V 2.3.1), numpy (V 2.3.1), pythonnet (V 3.0.5) and watchdog (V 6.0.0) to be installed
- [RawFileReaderFiles from Net471](https://github.com/thermofisherlsms/RawFileReader)
  these files need to be unblocked after download!

- only runs on Windows as part of the Thermo RawFileReader functions depend on Windows.   
 
#### folder structure:
raw2meta/  
├── RawFileReader_dll/  
│   └── Net471/  
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.RawFileReader.dll  
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.MassPrecisionEstimator.dll  
├── TEMP/  
├── run_crawler.bat   
├── run_Observer.bat   
  
if run via bat file:  
├── WinPython/  
│   └── python/  
│      └──python.exe  
