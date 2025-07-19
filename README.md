# raw2meta
Observes backup directory for new raw files. Metadata is extracted and stored in sqlite database for later use. 

### dependencies:
Uses python3
- pandas, numpy, pythonnet and watchdog to be installed
- [RawFileReaderFiles from Net471](https://github.com/thermofisherlsms/RawFileReader)
  these files need to be unblocked after download!
 
### folder structure:
raw2meta/  
├── RawFileReader_dll/  
│   └── Net471/  
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.RawFileReader.dll  
│&nbsp;&nbsp;&nbsp;&nbsp;└──ThermoFisher.CommonCore.MassPrecisionEstimator.dll  
├── run_crawler.bat   
├── run_Observer.bat   
  
if run via bat file:  
├── WinPython/  
│   └── python/  
│      └──python.exe  
