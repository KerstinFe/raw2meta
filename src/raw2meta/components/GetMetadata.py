from raw2meta.helper.common import RawFileReaderManager, SplitProjectName
import numpy as np
import re
import os
from pathlib import Path
from raw2meta.helper.Exceptions import HandlingCorruptFileError, HandlingEmptyFileError
from raw2meta.RawFileReader.ImportRawFileReaderFunctions import Environment, ChromatogramSignal,  Device, ChromatogramTraceSettings, TraceType
from raw2meta.config.configuration import MachinesDict, HPLCDict
from datetime import datetime
from raw2meta.entity.entities import SampleEntry, ProjectEntry
from raw2meta.config.logger import get_configured_logger
from System import ArgumentOutOfRangeException, NullReferenceException
from typing import Union, Optional, List, Tuple, Dict, Any

logger = get_configured_logger(__name__)

class MetadataLists:

    """These methods get the actual data from the raw files. 
      They return lists that can be inserted into the database.
        The main method is GetArray_SampleMetadata which calls the other methods as needed.
        The class is initialized with the path to the raw file.
        Note that the class uses the RawFileReaderManager context manager to ensure that the raw file is properly disposed of after use.
        1) GetChromatogram reads the pump pressure from the chromatogram data if a Neo is used.
        2) DataFrom_TrailerExtraFields reads the trailer extra fields to get the analyzer temperature and whether a FAIMS is attached.
        3) GetArray_SampleMetadata is the main method that combines the data and returns the SampleEntry and ProjectEntry dataclasses.
        4) _get_from_dict is a helper method to get the machine names from the dictionaries.

        """

    def __init__(self, RawfilePath: Union[str, Path]) -> None:
        """Initializes the MetadataLists class with the path to the raw file.
        :param RawfilePath: Path to the raw file.
        :type RawfilePath: Union[str, Path]
        :return: None
        :rtype: None

         """
        self.RawfilePath = Path(RawfilePath).as_posix()

    def GetChromatogram(self, NumberOfScansUsed: int = 1000) -> Tuple[float, float, float, float]:
        """
        This method reads the pump pressure from the chromatogram data.
        It uses the RawFileReader to select the appropriate instrument and trace type.
        It then samples the chromatogram data at a number of scans and calculates the initial, min, max and std of the pump pressure.
        Note that the NumberOfScansUsed can be adjusted to balance between speed and accuracy.
        Note that the pump pressure is only available if a Neo is used.
        
        To get the pump pressure, the following settings are used:

        rawFile.SelectInstrument(Device.Analog, 2)
        this is the A/D Card 2 = Pump Pressure 
        Tracetype to look at is A2DChannel1 for pump pressure 
        
        for Sampler Pressure set (Device.Analog, 1)

        :param NumberOfScansUsed: Number of scans to sample from the chromatogram data, defaults to 1000
        :type NumberOfScansUsed: int, optional
        :return: InitialPumpPressure, MinPumpPressure, MaxPumpPressure, Std_PumpPressure
        :rtype: Tuple[float, float, float, float]

                  
        """

       

        with RawFileReaderManager(self.RawfilePath) as rawFile:
            
            rawFile.SelectInstrument(Device.Analog, 2) # this is the A/D Card 2 = Pump Pressure, for Sampler Pressure set (Device.Analog, 1)
            firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum
            lastScanNumber = rawFile.RunHeaderEx.LastSpectrum
            settings = ChromatogramTraceSettings(TraceType.A2DChannel1)
            
            ScansToCheck = np.linspace(firstScanNumber, lastScanNumber, NumberOfScansUsed, dtype = int).tolist()
    
            trace_list = [] 
            for ScanNum in ScansToCheck:
                data= rawFile.GetChromatogramData([settings], ScanNum, ScanNum)
                trace = ChromatogramSignal.FromChromatogramData(data)
                trace_list.append(list(trace[0].Intensities))

            def _get_pressure_stats(trace_list):
                values = [x[0] for x in trace_list]
                return values[0], min(values), max(values), np.std(values)

            InitialPumpPressure, MinPumpPressure, MaxPumpPressure, Std_PumpPressure = _get_pressure_stats(trace_list)

        return InitialPumpPressure, MinPumpPressure, MaxPumpPressure ,Std_PumpPressure


    def DataFrom_TrailerExtraFields(self) -> Tuple[float, float, str]:
        """Reads and reports the trailer extra data fields present in the RAW file.
    
        :return: AnalyzerTemp_mean, AnalyzerTemp_std, FAIMSattached
        :rtype: Tuple[float, float, str]
        :raises ValueError: If the extracted values are not as expected.
        :raises HandlingEmptyFileError: If the raw file is empty (last scan number is 0).
        :raises HandlingCorruptFileError: If the raw file cannot be opened or read.

        I am only interested in the Analyzer temperature and whether a FAIMS is attached, so I am searching for these fields.
        The FAIMS field is not always reported and if so, I report that. 
        The Analyzer temperature is reported as mean and std of 500 scans evenly distributed over the whole run.

        """
        with RawFileReaderManager(self.RawfilePath) as rawFile:
            rawFile.SelectInstrument(Device.MS, 1)
            firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum
            lastScanNumber = rawFile.RunHeaderEx.LastSpectrum
            trailerFields = rawFile.GetTrailerExtraHeaderInformation()
            
            if lastScanNumber == 0:
                raise HandlingEmptyFileError
            
            NamesField =[None,None]
            FieldNums = [None,None]
            
            i = 0
            for field in trailerFields:
                if re.search("Analyzer Temperature", field.Label) is not None:
                    FieldNums[0] =i
                    NamesField[0] =field.Label
                if re.search("FAIMS Attached", field.Label) is not None:
                    FieldNums[1] =i
                    NamesField[1] =field.Label
                    
                i +=1
           
            # because this might be different with different machines, I want an error to be raised in case I read wrong data
            if not (NamesField == ["Analyzer Temperature:","FAIMS Attached:"] or NamesField == ["Analyzer Temperature:",None]) :
                raise ValueError("Extracted Values in ListTrailerFields Function not as expected. Check the tailer field numbers. Values in Namesfield: " + f'{NamesField}') from None
            
            ScansToCheck = np.linspace(firstScanNumber, lastScanNumber, 500, dtype = int).tolist()
            # trailerValues200_Temp =[]
            trailerValues200_Temp = [rawFile.GetTrailerExtraValue(ScanNum,FieldNums[0]) for ScanNum in ScansToCheck]
                          
            AnalyzerTemp_mean= np.mean(trailerValues200_Temp).item()
            AnalyzerTemp_std= np.std(trailerValues200_Temp).item()   

            if NamesField[1] is not None:
                FAIMSattached= str(rawFile.GetTrailerExtraValue(1,FieldNums[1]))
            else:
                FAIMSattached = "notRecorded"
           
        return AnalyzerTemp_mean,AnalyzerTemp_std,FAIMSattached
        
    
    def GetArray_SampleMetadata(self) -> Tuple[SampleEntry, ProjectEntry]:
        
        """
        This method first checks which machines (HPLC & MS) were used to aquire the data. Dependent on whether a Neo was used or not, it reads the pump pressure or not.
        It then creates the lists that can be inserted into the database. 
        :return: SQLValues_Samples, SQLValues_Project
        :rtype: Tuple[SampleEntry, ProjectEntry]
        """
        logger.info(f"Reading Metadata from {self.RawfilePath}")
        with RawFileReaderManager(self.RawfilePath) as rawFile:
            try:
                rawFile.SelectInstrument(Device.MS, 1)
            except NullReferenceException:
                raise HandlingCorruptFileError
            
            """ Note from RawFileReader Package:
             Read the first instrument method (most likely for the MS portion of
             the instrument).  NOTE: This method reads the instrument methods
             from the RAW file but the underlying code uses some Microsoft code
             that hasn't been ported to Linux or MacOS.  Therefore this method
             won't work on those platforms therefore the check for Windows."""
            
           
            MachineCombination = []

            if 'Windows' in str(Environment.OSVersion):
                try:
                    DevNames = rawFile.GetAllInstrumentFriendlyNamesFromInstrumentMethod()
                except NullReferenceException:
                    raise HandlingCorruptFileError
                
                deviceNames = [Dev for Dev in DevNames]

                if len(deviceNames) ==1:
                    deviceNames.append("EvoSep") 
                    
                if ((deviceNames[0] in MachinesDict) or (deviceNames[0] in HPLCDict)) and ((deviceNames[1] in MachinesDict) or (deviceNames[1] in HPLCDict)):
                    MachineCombination.append(self._get_from_dict(deviceNames, MachinesDict, "First Device not defined in Dictionary"))
                    MachineCombination.append(self._get_from_dict(deviceNames, HPLCDict, "Second Device not defined in Dictionary"))
     

            startTime = rawFile.RunHeaderEx.StartTime
            endTime = rawFile.RunHeaderEx.EndTime
            TimeRange= (str(startTime)+ "-"+ str(endTime))
            
            Name = os.path.basename(self.RawfilePath)
            ProjectID, _, _, ProjectID_Date = SplitProjectName(self.RawfilePath)

            CreationDate_print = rawFile.FileHeader.CreationDate
            CreationDate_print = datetime(
                    CreationDate_print.Year,
                    CreationDate_print.Month,
                    CreationDate_print.Day,
                    CreationDate_print.Hour,
                    CreationDate_print.Minute,
                    CreationDate_print.Second,
                    CreationDate_print.Millisecond, 
                ).strftime("%Y-%m-%d %H:%M:%S.000")
        
            Vial = rawFile.SampleInformation.Vial
            InjectionVolume = rawFile.SampleInformation.InjectionVolume
        
            InstrumentMethod_print = rawFile.SampleInformation.InstrumentMethodFile
        
            SoftwareVersion = rawFile.GetInstrumentData().SoftwareVersion

        AnalyzerTemp_mean,AnalyzerTemp_std,FAIMSattached = self.DataFrom_TrailerExtraFields()  
        
        SQLValues_Project = ProjectEntry(ProjectID,ProjectID_Date,MachineCombination[0],SoftwareVersion,InstrumentMethod_print,MachineCombination[1],
                                             TimeRange,FAIMSattached) 
          
        if "Neo" in MachineCombination:
           
            try: # in the beginning some Neos we not configured yet to record/safe the pump pressure, so there were some errors with older files
                InitialPressure_Pump, MinPressure_Pump, MaxPressure_Pump, Std_Pressure_Pump = self.GetChromatogram()

                SQLValues_Samples = SampleEntry(Name,ProjectID,CreationDate_print,
                    Vial,InjectionVolume, InitialPressure_Pump, MinPressure_Pump, MaxPressure_Pump,Std_Pressure_Pump, AnalyzerTemp_mean,AnalyzerTemp_std) 
                
            except ArgumentOutOfRangeException:
                SQLValues_Samples = SampleEntry(Name,ProjectID,CreationDate_print,
                    Vial,InjectionVolume, AnalyzerTemp_mean=AnalyzerTemp_mean, AnalyzerTemp_std=AnalyzerTemp_std) 
           
        else:
            SQLValues_Samples = SampleEntry(Name,ProjectID,CreationDate_print,
                Vial,InjectionVolume, AnalyzerTemp_mean=AnalyzerTemp_mean, AnalyzerTemp_std=AnalyzerTemp_std)

        logger.info('Closed {}'.format(self.RawfilePath))

        return  SQLValues_Samples, SQLValues_Project

    def _get_from_dict(self, device_names: List[str], lookup_dict: Dict[str, Any], error_msg: str) -> Any:
        """Helper method to get the machine names from the dictionaries.
        :param device_names: List of device names from the raw file.
        :param lookup_dict: Dictionary to look up the device names.
        :param error_msg: Error message to raise if the device name is not found.
        """
        for name in device_names:
            if name in lookup_dict:
                return lookup_dict[name]
        raise AttributeError(error_msg)    

