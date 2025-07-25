import sys, os
from pathlib import Path

try:

    import clr
       
    CurrentDirectory =  os.path.dirname(os.path.realpath(__file__))
    FileReaderFile1 = Path(os.path.join(CurrentDirectory , "RawFileReader_dll/Net471", "ThermoFisher.CommonCore.RawFileReader")).as_posix()
    FileReaderFile2 = Path(os.path.join(CurrentDirectory , "RawFileReader_dll/Net471", "ThermoFisher.CommonCore.MassPrecisionEstimator")).as_posix()
   
    ''' files have to be unblocked first, with every copying they will be blocked again.
      either do it with GUI (right click, properties, unblock)
      or with command line/powershell: Unblock-File -Path "./ThermoFisher.CommonCore.MassPrecisionEstimator.dll", ...
    ''' 
   
    clr.AddReference(FileReaderFile1)
    clr.AddReference(FileReaderFile2)
    
except AttributeError:
     print("Code canot be restarted immediately, need restart of Kernel", flush = True)
     sys.exit(0)
     
     
from System import *

from ThermoFisher.CommonCore.Data.Business import ChromatogramSignal, ChromatogramTraceSettings, Device, TraceType
from ThermoFisher.CommonCore.RawFileReader import RawFileReaderAdapter


from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
import re
import pandas as pd
import numpy as np
import sqlite3
import json
from datetime import datetime, timedelta,date
from queue import Queue
import dateutil.relativedelta


MachinesDict = {
                "Q Exactive HF-X - Orbitrap MS": "HFX",
                "Orbitrap Astral":"Astral",
                "Orbitrap Exploris 480": "Exploris480",
                "Q Exactive Plus - Orbitrap MS": "Q_Exactive_Plus"}

HPLCDict ={
                "Thermo EASY-nLC" : "nanoLC",
                "Proxeon_EASY-nLC": "nanoLC",
                "Thermo Scientific SII for Xcalibur": "Neo",
                "SiiXcalibur": "EvoSep",
                "EvoSep":"EvoSep"
                
    }

q = Queue()

TempFolder = os.path.join(CurrentDirectory, "TEMP")

Logfile_corrupt = Path(os.path.join(CurrentDirectory,"FilesWithError.log")).as_posix()
Logfile_empty = Path(os.path.join(CurrentDirectory,"EmptyFiles.log")).as_posix()
Logfile_Integrity = Path(os.path.join(CurrentDirectory,"AlreadyinDB.log")).as_posix()

''' fist some small helper functions'''


def SplitProjectName(Name):
    
    Name = os.path.basename(Name)
    Names_splitted = Name.split("_")
    ProjectID = (Names_splitted[0]+"_"+Names_splitted[1]+"_"+Names_splitted[2])
    ProjectID_regex = (Names_splitted[0]+"_"+re.sub("[0-9]{2}$", "[0-9]{2}", Names_splitted[1])+"_"+Names_splitted[2])
    ProjectID_regex_sql =(Names_splitted[0]+"_"+re.sub("[0-9]{2}$", "__", Names_splitted[1])+"_"+Names_splitted[2])
    ProjectID_Date = Names_splitted[1]
    
    return ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date


class RawFileReaderManager:
    '''I created this method to make sure the rawfiles are closed after reading.'''
    def __init__(self, RawfilePath):
        self.RawfilePath = RawfilePath
        
    def __enter__(self):
        self.rawFile = RawFileReaderAdapter.FileFactory(self.RawfilePath)
        return self.rawFile
    
    def __exit__(self,exc_type, exc_value, traceback):
        self.rawFile.Dispose()
   
class HandlingCorruptFileError(Exception):
    "raised when file is not readable with new .NET functions"
    pass


class HandlingEmptyFileError(Exception):
    "raised when file does not contain any scans"
    pass


class NoFittingProjectFound(Exception):
    "raised when a json could not be inserted into db because there was not fitting project ID"
    pass


class SafedAsJsonTempFile(Exception):
    "not an error, but message to avoid that it looks like the file has been written into db when it is only stored as Json"
    pass

def get_UserInput():
    
    ''' Function to get the location of the metadata file and the backup server location when starting the script.
    It assumes that the database is in the same folder, if that is correct the user needs to only press enter and not always enter the whole path.
    '''
    
    flatnames = ''
    columnsTable = ['Metadata_Project', 'Metadata_Sample', 'TEMP_forJoin']
    
    FileToWatch_Metadata = Path(os.path.join(CurrentDirectory, "Metadata.sqlite")).as_posix()
    FileToWatch_Metadata_right = input(f'If {FileToWatch_Metadata} is correct location of Metadata press enter, else enter False.')
    
    if FileToWatch_Metadata_right =='':
        print()
        print( f'{FileToWatch_Metadata} is file? : {os.path.isfile(FileToWatch_Metadata)}')
        print()
        
        if os.path.isfile(FileToWatch_Metadata):
                   
            print("SQL DB for Metadata defined as: " + f'{FileToWatch_Metadata}' )
        
            with sqlite3.connect(FileToWatch_Metadata) as con:
                cur = con.cursor(  )
                names = cur.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
                names = cur.fetchall()
           
            flatnames = [item for t in names for item in t]
                
            if set(flatnames) == set(columnsTable):
                print()
                print("DB with tables exists")
                print()
                
            else:
                 print()
                 print( f'{FileToWatch_Metadata} exists, but does not contain Metadata_Project or Metadata_Sample tables yet.')
                 print()
                 
                 CreateNewTables = input(' The path might belong to a different database. Do you want to create new tables? Enter Yes/No: ')
                 
                 if CreateNewTables == "Yes":
                     Metadata_SQL = Database_Call_MetaDataSQL(FileToWatch_Metadata) 
                     Metadata_SQL.Database_CreateTables()
                     
                     print()
                     print("Tabels in DB created")
                     print()
                     
                 else:
                     FileToWatch_Metadata_right =False
        else:
            CreateNewDatabase = input("Database does not exist. Create New Database? Enter Yes/No: ")
            
            if CreateNewDatabase == "Yes":
                Metadata_SQL = Database_Call_MetaDataSQL(FileToWatch_Metadata) 
                Metadata_SQL.Database_CreateTables()
            else:     
                print("Provided file is not a .sqlite db. Please try again.") 
                FileToWatch_Metadata_right =False

    if not (FileToWatch_Metadata_right ==''):
        
        FileToWatch_Metadata = ''
        
        while not os.path.isfile(FileToWatch_Metadata) or not (flatnames == columnsTable):
            
            FileToWatch_Metadata = input("Set location of SQL Database for Metadata: ")
            FileToWatch_Metadata = FileToWatch_Metadata.replace('"', '')
            FileToWatch_Metadata = FileToWatch_Metadata.replace("'", "")
            FileToWatch_Metadata  = Path(FileToWatch_Metadata).as_posix()
            
            if os.path.isdir(FileToWatch_Metadata):
                print("Provided path is a directory not a file. Please reenter filename")
                     
            elif os.path.isfile(FileToWatch_Metadata):
                print()
                print( f'{FileToWatch_Metadata} is file? : {os.path.isfile(FileToWatch_Metadata)}')
                print()
                
                if not Path(FileToWatch_Metadata).suffix == ".sqlite":
                    print("Provided file is not a .sqlite db. Please try again.")
                else:    
                   print("SQL DB for Metadata defined as: " + f'{FileToWatch_Metadata}' )
                
                   with sqlite3.connect(FileToWatch_Metadata) as con:
                       cur = con.cursor(  )
                       names = cur.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
                       names = cur.fetchall()
                  
                   flatnames = [item for t in names for item in t]

                   if set(flatnames) == set(columnsTable):
                       print()
                       print("DB with tables exists")
                       print()
                       
                   else:
                        print()
                        print( f'{FileToWatch_Metadata} exists, but does not contain Metadata_Project or Metadata_Sample tables yet.')
                        print()
                        
                        CreateNewTables = input(' The path might belong to a different database. Do you want to create new tables? Enter Yes/No: ')
                        
                        if CreateNewTables == "Yes":
                            Metadata_SQL = Database_Call_MetaDataSQL(FileToWatch_Metadata) 
                            Metadata_SQL.Database_CreateTables()
                            print()
                            print("Tabels in DB created")
                            print()
                        
            else:
                  if Path(FileToWatch_Metadata).suffix == ".sqlite":
                     CreateNewDatabase = input("Database does not exist. Create New Database? Enter Yes/No: ")
                     if CreateNewDatabase == "Yes":
                         Metadata_SQL = Database_Call_MetaDataSQL(FileToWatch_Metadata) 
                         Metadata_SQL.Database_CreateTables()
                  else:     
                      print("Provided file is not a .sqlite db. Please try again.")  
             
                
    MassSpecDirectory_ToObserve =''      
    while not os.path.isdir(MassSpecDirectory_ToObserve):      
        MassSpecDirectory_ToObserve = input("Set location of Raw data Backup Server: ")
        MassSpecDirectory_ToObserve = MassSpecDirectory_ToObserve.replace('"', '')
        MassSpecDirectory_ToObserve = MassSpecDirectory_ToObserve.replace("'", "")
        MassSpecDirectory_ToObserve = Path(MassSpecDirectory_ToObserve).as_posix()
        
        print()
        print("Parent Folder for Raw files defined as: " + f'{MassSpecDirectory_ToObserve}' )
        print()

        if  os.path.isdir(MassSpecDirectory_ToObserve):
            print()
            print(f'Now Observing {MassSpecDirectory_ToObserve} for new .raw files.')
        else:
            print()
            print(f'{MassSpecDirectory_ToObserve} is not a valid directory. Please try to reenter.')

    return   FileToWatch_Metadata,  MassSpecDirectory_ToObserve                      



class Database_Call_MetaDataSQL:
    
    ''' This function helps to structure the writing into the database''' 
    
    
    def __init__(self, FileToWatch_Metadata):
        self.FileToWatch_Metadata = FileToWatch_Metadata

    def Database_CreateTables(self):
        
        sql1 = '''CREATE TABLE IF NOT EXISTS Metadata_Project(
        ProjectID text PRIMARY KEY,
        ProjectID_Date text,
        Instrument text,
        SoftwareVersion text,
        Method text,
        HPLC text,
        TimeRange text,
        FAIMSattached text
        )'''
         
    
        sql2 = '''CREATE TABLE IF NOT EXISTS Metadata_Sample(
        SampleName_ID text PRIMARY KEY,
        ProjectID text,
        CreationDate text,
        Vial text,
        InjectionVolume real,
        InitialPressure_Pump real,
        MinPressure_Pump real,
        MaxPressure_Pump real,
        Std_Pressure_Pump real,
        AnalyzerTemp_mean real,  
        AnalyzerTemp_std real,  
        Error text,
        FOREIGN KEY (ProjectID)
               REFERENCES Metadata_Project (ProjectID) 
        )'''

        sql3 =  '''CREATE TABLE IF NOT EXISTS TEMP_forJoin(
        RawfileNames text 
        )'''

        with sqlite3.connect(self.FileToWatch_Metadata) as con:
            
            cur = con.cursor()    
            cur.execute(sql1)
            con.commit()
            cur.execute(sql2)
            con.commit()
            cur.execute(sql3)
            con.commit()
            
    def Database_writeNewEntry(self,SQLStatement, SQLValues):
         with sqlite3.connect(self.FileToWatch_Metadata) as con:
             cur = con.cursor()
             cur.execute(SQLStatement, SQLValues)
             con.commit()


class CreateMetaDataLists:
    
    '''These methods get the actual data from the raw files  '''
    
    def __init__(self, RawfilePath):
         self.RawfilePath = RawfilePath      
              
    def GetChromatogram(self):
        '''
        rawFile.SelectInstrument(Device.Analog, 2)
        this is the A/D Card 2 = Pump Pressure 
        Tracetype to look at is A2DChannel1 for pump pressure 
        
        for Sampler Pressure set (Device.Analog, 1)
                  
        '''
        with RawFileReaderManager(self.RawfilePath) as rawFile:
            
            rawFile.SelectInstrument(Device.Analog, 2) # this is the A/D Card 2 = Pump Pressure, for Sampler Pressure set (Device.Analog, 1)
            firstScanNumber = rawFile.RunHeaderEx.FirstSpectrum
            lastScanNumber = rawFile.RunHeaderEx.LastSpectrum
            settings = ChromatogramTraceSettings(TraceType.A2DChannel1)
            
            ScansToCheck = np.linspace(firstScanNumber, lastScanNumber, 1000, dtype = int).tolist()
    
            trace_list = [] 
            for ScanNum in ScansToCheck:
                data= rawFile.GetChromatogramData([settings], ScanNum, ScanNum)
                trace = ChromatogramSignal.FromChromatogramData(data)
                trace_list.append(list(trace[0].Intensities))
             
            InitialPumpPressure = trace_list[0][0]
            MinPumpPressure = min(trace_list)[0]
            MaxPumpPressure = max(trace_list)[0]    
            Std_PumpPressure = np.std(trace_list).item()
                  
        return InitialPumpPressure, MinPumpPressure, MaxPumpPressure ,Std_PumpPressure
      
        
    def DataFrom_TrailerExtraFields(self):
        '''Reads and reports the trailer extra data fields present in the RAW
        file.
        
        I am only interested in the Analyzer temperature and whether a FAIMS is attached, so I am searching for these fields.
        The FAIMS field is not always reported and if so, I report that. 
        '''
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
            trailerValues200_Temp =[]
            for ScanNum in ScansToCheck:
                trailerValues200_Temp.append(rawFile.GetTrailerExtraValue(ScanNum,FieldNums[0]))
              
            AnalyzerTemp_mean= np.mean(trailerValues200_Temp).item()
            AnalyzerTemp_std= np.std(trailerValues200_Temp).item()   
            if NamesField[1] is not None:
                FAIMSattached= str(rawFile.GetTrailerExtraValue(1,FieldNums[1]))
            else:
                FAIMSattached = "notRecorded"
           
        return AnalyzerTemp_mean,AnalyzerTemp_std,FAIMSattached
        
    
    def GetArray_SampleMetadata(self):
        
        '''
        This method first checks which machines (HPLC & MS) were used to aquire the data. Dependent on whether a Neo was used or not, it reads the pump pressure or not.
        It then creates the lists that can be inserted into the database. 
        '''
        
        with RawFileReaderManager(self.RawfilePath) as rawFile:
            rawFile.SelectInstrument(Device.MS, 1)
          
            ''' Note from RawFileReader Package:
             Read the first instrument method (most likely for the MS portion of
             the instrument).  NOTE: This method reads the instrument methods
             from the RAW file but the underlying code uses some Microsoft code
             that hasn't been ported to Linux or MacOS.  Therefore this method
             won't work on those platforms therefore the check for Windows.'''
        
            MachineCombination = []
            if 'Windows' in str(Environment.OSVersion):
                
                DevNames = rawFile.GetAllInstrumentFriendlyNamesFromInstrumentMethod()
                deviceNames =[]
                for Dev in DevNames:
                    deviceNames.append(Dev)
                 
                if len(deviceNames) ==1:
                    deviceNames.append("EvoSep") 
                    
                if ((deviceNames[0] in MachinesDict) or (deviceNames[0] in HPLCDict)) and ((deviceNames[1] in MachinesDict) or (deviceNames[1] in HPLCDict)):
                      
                        try:
                            MachineCombination.append(MachinesDict[deviceNames[0]])
                        except:
                            try:
                                MachineCombination.append(MachinesDict[deviceNames[1]])
                            except:
                                raise AttributeError("First Device not defined in Dictionary") 
                                
                        try:
                            MachineCombination.append(HPLCDict[deviceNames[0]])
                        except:
                            try: 
                                MachineCombination.append(HPLCDict[deviceNames[1]])
                            except:
                                raise AttributeError("Second Device not defined in Dictionary") 
                              
            startTime = rawFile.RunHeaderEx.StartTime
            endTime = rawFile.RunHeaderEx.EndTime
            TimeRange= (str(startTime)+ "-"+ str(endTime))
            
            Name = os.path.basename(self.RawfilePath)
            ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(self.RawfilePath)
         
            CreationDate_print = rawFile.FileHeader.CreationDate
        
            Vial = rawFile.SampleInformation.Vial
            InjectionVolume = rawFile.SampleInformation.InjectionVolume
        
            InstrumentMethod_print = rawFile.SampleInformation.InstrumentMethodFile
        
            MSInstrument = MachineCombination[0]
            HPLCInstrument = MachineCombination[1]
        
            SoftwareVersion = rawFile.GetInstrumentData().SoftwareVersion
            
        if "Neo" in MachineCombination:
            AnalyzerTemp_mean,AnalyzerTemp_std,FAIMSattached = self.DataFrom_TrailerExtraFields()
            InitialPressure_Pump, MinPressure_Pump, MaxPressure_Pump, Std_Pressure_Pump = self.GetChromatogram()

            SQLValues_Samples = [Name,ProjectID,datetime(
                CreationDate_print.Year,
                CreationDate_print.Month,
                CreationDate_print.Day,
                CreationDate_print.Hour,
                CreationDate_print.Minute,
                CreationDate_print.Second,
                CreationDate_print.Millisecond, 
            ).strftime("%Y-%m-%d %H:%M:%S.000"),
                Vial,InjectionVolume, InitialPressure_Pump, MinPressure_Pump, MaxPressure_Pump,Std_Pressure_Pump, AnalyzerTemp_mean,AnalyzerTemp_std] 
            
            SQLValues_Project =[ProjectID,ProjectID_Date,MSInstrument,SoftwareVersion,InstrumentMethod_print,HPLCInstrument,TimeRange,FAIMSattached] 
            
        else:
            AnalyzerTemp_mean,AnalyzerTemp_std,FAIMSattached = self.DataFrom_TrailerExtraFields()
            SQLValues_Samples = [Name,ProjectID,datetime(
                CreationDate_print.Year,
                CreationDate_print.Month,
                CreationDate_print.Day,
                CreationDate_print.Hour,
                CreationDate_print.Minute,
                CreationDate_print.Second,
                CreationDate_print.Millisecond,  
            ).strftime("%Y-%m-%d %H:%M:%S.000"),
                Vial,InjectionVolume, AnalyzerTemp_mean,AnalyzerTemp_std]
            
            SQLValues_Project =[ProjectID,ProjectID_Date,MSInstrument,SoftwareVersion,InstrumentMethod_print,HPLCInstrument,TimeRange,FAIMSattached] 
    
        print('Closed {}'.format(self.RawfilePath))
        
        return MachineCombination, SQLValues_Samples, SQLValues_Project


class Execute_CreateSQLdbCode(): 
    
    ''' These methods now insert data into the database.
    The logic is quite specific to how we aquire data. 
    '''
    
    def __init__(self, FileToWatch_Metadata):
    
        self.FileToWatch_Metadata = FileToWatch_Metadata 
        self.Metadata_SQL = Database_Call_MetaDataSQL(self.FileToWatch_Metadata)
        
        self.InsertSQL1_all = '''INSERT INTO Metadata_Project(ProjectID,ProjectID_Date, Instrument, SoftwareVersion, 
                                                         Method, HPLC, TimeRange, FAIMSattached) 
                            VALUES(?,?,?,?,?,?,?,?);'''
         
        self.InsertSQL2_Neo = '''INSERT INTO Metadata_Sample (SampleName_ID,ProjectID,CreationDate ,Vial,InjectionVolume,
                            InitialPressure_Pump,MinPressure_Pump,MaxPressure_Pump,Std_Pressure_Pump ,AnalyzerTemp_mean,AnalyzerTemp_std) 
                            VALUES( ?,?,?,?,?,?,?,?,?,?,?);'''

        self.InsertSQL2_nanoLC_evo = '''INSERT INTO Metadata_Sample (SampleName_ID,ProjectID,CreationDate ,Vial,InjectionVolume,AnalyzerTemp_mean,AnalyzerTemp_std) 
                            VALUES( ?,?,?,?,?,?,?);'''
                            
        self.InsertCorruptSample = '''INSERT INTO Metadata_Sample (SampleName_ID,ProjectID,Error) 
                            VALUES( ?,?,?);'''                    
        
        try:
            with sqlite3.connect(self.FileToWatch_Metadata) as con:
                print(f"Opened SQLite database with version {sqlite3.sqlite_version} successfully.")
                
        except sqlite3.OperationalError as e:
            print("Failed to open database:", e)   


    def MissingFilesFromDatabase(self, RawfileList):
       
        ''' This is the function with which I check which samples are not in the database yet'''
        
       
        createTable = '''CREATE TABLE IF NOT EXISTS TEMP_forJoin(
        RawfileNames text 
        )'''
    
        InsertStatement = '''INSERT INTO TEMP_forJoin (RawfileNames) VALUES(?)'''
        
        LeftOuterJoin = ''' SELECT TEMP_forJoin.RawfileNames
        FROM TEMP_forJoin
        LEFT JOIN Metadata_Sample
        ON TEMP_forJoin.RawfileNames =Metadata_Sample.SampleName_ID
        WHERE Metadata_Sample.SampleName_ID IS NULL; '''

        RawfileListTuple = [(x,) for x in RawfileList]

        with sqlite3.connect(self.FileToWatch_Metadata) as con:
       
            cur = con.cursor()
            cur.execute(createTable)
            cur.execute("DELETE FROM TEMP_forJoin")
            con.commit()
            cur.executemany(InsertStatement,  RawfileListTuple)
            con.commit()
            MissingRawFiles = con.execute(LeftOuterJoin).fetchall()
            cur.execute("DELETE FROM TEMP_forJoin")

        return MissingRawFiles
         
    def SampleNotInDatabase(self, RawfilePath):
       
        Name = os.path.basename(RawfilePath)
        with sqlite3.connect(self.FileToWatch_Metadata) as con:
            cur = con.cursor()
            RowCount = cur.execute('''SELECT EXISTS(SELECT 1 FROM Metadata_Sample 
                                          WHERE SampleName_ID LIKE (?) LIMIT 1)''', (Name,)).fetchone()
            ReturnValue, = RowCount
  
        return ReturnValue==0
        
    
    def FillDatabase(self,RawfilePath):     
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
        
        '''
      
        try:
            MetadataOutput = CreateMetaDataLists(RawfilePath)
            MachineCombination, SQLValues_Samples , SQLValues_Project= MetadataOutput.GetArray_SampleMetadata()
            
        except ArgumentOutOfRangeException:
            raise HandlingCorruptFileError
            
        except IndexOutOfRangeException: 
            raise HandlingEmptyFileError
        
        with sqlite3.connect(self.FileToWatch_Metadata) as con:
             RowCount_Proj = pd.read_sql_query('''SELECT COUNT(ProjectID) FROM Metadata_Project 
                                                WHERE ProjectID = ?;
                                         ''',con, params= (SQLValues_Project[0],))
             
             RowCount_Proj =RowCount_Proj.iloc[0]["COUNT(ProjectID)"]
        
        if RowCount_Proj==0:
            if (re.search("HSstd",SQLValues_Samples[0])) or (re.search("[Ss]tandar[dt]",SQLValues_Samples[0])):
                
                ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(SQLValues_Project[0])
               
                with sqlite3.connect(self.FileToWatch_Metadata) as con:
                     Regex_Proj = pd.read_sql_query('''SELECT ProjectID, ProjectID_Date FROM Metadata_Project 
                                                        WHERE ProjectID LIKE ?;
                                                 ''',con, params= (ProjectID_regex_sql,))
                                        
                if len(Regex_Proj)==0:                                 
                               
                    JsonDict = {"SQLValues_Project" : SQLValues_Project,
                                "SQLValues_Samples" : SQLValues_Samples}
                    json_object = json.dumps(JsonDict, indent=2)
                    FileName =(SQLValues_Project[0]+".json")
                    
                    if FileName in os.listdir(TempFolder):
                        FileName =(SQLValues_Project[0]+"__"+SQLValues_Samples[0]+".json")
                    
                    FilePathjson = os.path.join(TempFolder, FileName)
                    
                    with open(FilePathjson, "w") as outfile:
                        outfile.write(json_object)  
                
                    raise SafedAsJsonTempFile                        
                
                else: 
                    Dist = list(Regex_Proj["ProjectID_Date"])
                    Dist = [int(x)-int(ProjectID_Date)  for x in Dist]
                    nearestDate = Dist.index(min(Dist))
                    SQLValues_Samples[1] = Regex_Proj["ProjectID"][nearestDate]
                       
                    if "Neo" in SQLValues_Project[5]:
                        self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_Neo, SQLValues_Samples) 
                                                 
                    else:   
                        self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_nanoLC_evo, SQLValues_Samples) 
                        
            else:
                 if "Neo" in MachineCombination:
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_Neo, SQLValues_Samples)             
                 
                 else:
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_nanoLC_evo, SQLValues_Samples)   
                 
                 ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(SQLValues_Project[0])   
                
                 for file in os.listdir(TempFolder):
                   
                     if re.search(ProjectID_regex, file):
                         try:
                             self.FillDatabaseWithJson(file)
                         except NoFittingProjectFound:
                             print("Project not in DB yet.")
   
        else:
            if "Neo" in MachineCombination:
                self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_Neo, SQLValues_Samples)  
                               
            else:
                self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_nanoLC_evo, SQLValues_Samples) 
        
    
    def FillDatabaseWithJson(self, Tempfile):    
        
        ''' This is to shorten the code above.
        I am reading in the .json file, and check again for the Project ID.
        This is important because some hela standards are measured without a matching project following them.
        For example if they were maintained by a different person than who measures later.
        These samples are inserted later with their own project ID. 
        
        Sometimes people are lucky and measure multiple projects in the same month on the same machine.
        The helas around these projects need to be matched to the correct one. I decided to just take the one with the closest date. 
        This might not always be ideal and correct but for now is the simplest solution. 
        '''
               
        NameToSplit = re.sub(".json", "", Tempfile)
        ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(NameToSplit)
      
        with sqlite3.connect(self.FileToWatch_Metadata) as con:
             Regex_Proj = pd.read_sql_query('''SELECT ProjectID, ProjectID_Date FROM Metadata_Project 
                                                WHERE ProjectID LIKE ?;
                                         ''',con, params= (ProjectID_regex_sql,))
                
        if len(Regex_Proj)==0:
            print(f"No fitting Project found for file {Tempfile}.")
            raise NoFittingProjectFound
            
        else:
            print(f'TEMP file {Tempfile} used', flush = True)
            with open(os.path.join(TempFolder, Tempfile), 'r') as openfile:
                json_object = json.load(openfile)
                SQLValues_Samples_json = json_object["SQLValues_Samples"]
                SQLValues_Project_json=  json_object["SQLValues_Project"]
             
                               
            Dist = list(Regex_Proj["ProjectID_Date"])
            Dist = [int(x)-int(ProjectID_Date)  for x in Dist]
           
            nearestDate = Dist.index(min(Dist))
            SQLValues_Samples_json[1] = Regex_Proj["ProjectID"][nearestDate]
               
            if "Neo" in SQLValues_Project_json[5]:
                self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_Neo, SQLValues_Samples_json) 
                                         
            else: 
                self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_nanoLC_evo, SQLValues_Samples_json)  
       
            os.remove(os.path.join(TempFolder, Tempfile)) 
            print(f'TEMP Tempfile {Tempfile} deleted', flush = True)
        
    def FillDatabaseWithJson_KeepProject(self, Tempfile):    

        ''' This is mostly for Hela standards that are measured without a matching project.
        This happens for example when some is maintaining the machine without using it for their project afterwards.
        These will not fit into the logic of the functions above but I still want them to be inserted after a couple of days.
        
        '''        
                        
        print(f'TEMP file {Tempfile} used', flush = True)
        
        with open(os.path.join(TempFolder, Tempfile), 'r') as openfile:
            json_object = json.load(openfile)
            SQLValues_Samples_json = json_object["SQLValues_Samples"]
            SQLValues_Project_json=  json_object["SQLValues_Project"]
    
               
        if "Neo" in SQLValues_Project_json[5]:
            self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project_json)
            self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_Neo, SQLValues_Samples_json)
                                     
        else:  
            self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project_json)
            self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL2_nanoLC_evo, SQLValues_Samples_json)  
   
        os.remove(os.path.join(TempFolder, Tempfile)) 
        
        print(f'TEMP Tempfile {Tempfile} deleted', flush = True)    
                        
                               
    def FillDatabase_Error(self,RawfilePath, Error):     
      
        '''Some files can be corrupt. Because I want to connect the database to a dashboard where users can follow their projects
         I want these files to be reported as corrupt so the user has the option to fix or report the problem.
         Therefore I add them only with sample and project ID and with an Error attached to them.
        '''
      
        rawFileName = os.path.basename(RawfilePath)
        ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(rawFileName)
        SQLValues_Samples_Error = [rawFileName, ProjectID, Error]
                
        with sqlite3.connect(self.FileToWatch_Metadata) as con:
             RowCount_Proj = pd.read_sql_query('''SELECT COUNT(ProjectID) FROM Metadata_Project 
                                                WHERE ProjectID = ?;
                                         ''',con, params= (ProjectID,))
             
             RowCount_Proj =RowCount_Proj.iloc[0]["COUNT(ProjectID)"]
        
        if RowCount_Proj==0:
                                          
            with sqlite3.connect(self.FileToWatch_Metadata) as con:
                 Regex_Proj = pd.read_sql_query('''SELECT ProjectID, ProjectID_Date FROM Metadata_Project 
                                                    WHERE ProjectID LIKE ?;
                                             ''',con, params= (ProjectID_regex_sql,))
                             
            if len(Regex_Proj)==0:  
                self.Metadata_SQL.Database_writeNewEntry(self.InsertCorruptSample,SQLValues_Samples_Error ) 
                                    
            else: 
                Dist = list(Regex_Proj["ProjectID_Date"])
                Dist = [int(x)-int(ProjectID_Date)  for x in Dist]
                nearestDate = Dist.index(min(Dist))
                SQLValues_Samples_Error[1] = Regex_Proj["ProjectID"][nearestDate]
                   
                self.Metadata_SQL.Database_writeNewEntry(self.InsertCorruptSample,SQLValues_Samples_Error ) 
     
        else:
            self.Metadata_SQL.Database_writeNewEntry(self.InsertCorruptSample,SQLValues_Samples_Error ) 
               
    def ReplaceErrorFile(self,RawfilePath ):   
        
        '''Some files can be corrupt. This can be due to copying issues during backup.
        This function can be used to replace the entry of the corrupt file after it has been replaced.
        I am aiming at including it into the main logic but have not done this so far. 
        '''
         
        UpdateSQL2_Neo_error = '''UPDATE Metadata_Sample 
                                SET ProjectID = ?,
                                CreationDate =?,
                                Vial =?,
                                InjectionVolume =?,
                                InitialPressure_Pump =?,
                                MinPressure_Pump =?,
                                MaxPressure_Pump =?,
                                Std_Pressure_Pump =?,
                                AnalyzerTemp_mean =?,
                                AnalyzerTemp_std =?,
                                Error = "ErrorUpdated"
           
                                WHERE SampleName_ID LIKE ?;'''

      
       
        UpdateSQL2_nanoLC_evo_error = '''UPDATE Metadata_Sample 
                            SET ProjectID = ?,
                            CreationDate =?,
                            Vial =?,
                            InjectionVolume =?,
                            AnalyzerTemp_mean =?,
                            AnalyzerTemp_std =?,
                            Error = "ErrorUpdated"
                            
                            WHERE SampleName_ID LIKE ?;'''
                          
        try:
            MetadataOutput = CreateMetaDataLists(RawfilePath)
            MachineCombination, SQLValues_Samples , SQLValues_Project= MetadataOutput.GetArray_SampleMetadata()
            
        except ArgumentOutOfRangeException:
            raise HandlingCorruptFileError
            
        except IndexOutOfRangeException: 
            raise HandlingEmptyFileError
        
        with sqlite3.connect(self.FileToWatch_Metadata) as con:
             RowCount_Proj = pd.read_sql_query('''SELECT COUNT(ProjectID) FROM Metadata_Project 
                                                WHERE ProjectID = ?;
                                         ''',con, params= (SQLValues_Project[0],))
             
             RowCount_Proj =RowCount_Proj.iloc[0]["COUNT(ProjectID)"]
        
        ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(SQLValues_Project[0])
        
        SQLValues_Samples_update =  list(SQLValues_Samples)
        SQLValues_Samples_update.pop(0)
        SQLValues_Samples_update.append(SQLValues_Samples[0])
        
        if RowCount_Proj==0:
            if (re.search("HSstd",SQLValues_Samples[0])) or (re.search("[Ss]tandar[dt]",SQLValues_Samples[0])):
        
                with sqlite3.connect(self.FileToWatch_Metadata) as con:
                    Regex_Proj = pd.read_sql_query('''SELECT ProjectID, ProjectID_Date FROM Metadata_Project 
                                                        WHERE ProjectID LIKE ?;
                                                 ''',con, params= (ProjectID_regex_sql,))
                                                              
                if len(Regex_Proj)==0:                                 
                    if "Neo" in MachineCombination:
                        self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                        self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_Neo_error, SQLValues_Samples_update)             
                    else:
                        self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                        self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_nanoLC_evo_error, SQLValues_Samples_update)              
                    
                else: 
                    Dist = list(Regex_Proj["ProjectID_Date"])
                    Dist = [int(x)-int(ProjectID_Date)  for x in Dist]
                    nearestDate = Dist.index(min(Dist))
                    SQLValues_Samples[1] = Regex_Proj["ProjectID"][nearestDate]
                       
                    if "Neo" in SQLValues_Project[5]:
                        self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_Neo_error,SQLValues_Samples_update) 
                                                 
                    else:   
                        self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_nanoLC_evo_error, SQLValues_Samples_update)   
                        
            else:
                 if "Neo" in MachineCombination:
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                     self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_Neo_error, SQLValues_Samples_update)             
                 else:
                     self.Metadata_SQL.Database_writeNewEntry(self.InsertSQL1_all,SQLValues_Project)
                     self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_nanoLC_evo_error, SQLValues_Samples_update)    

        else:
            if "Neo" in MachineCombination:
                self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_Neo_error, SQLValues_Samples_update)  
                               
            else:
                self.Metadata_SQL.Database_writeNewEntry(UpdateSQL2_nanoLC_evo_error, SQLValues_Samples_update)               
    
''' 
Now the functions follow that I need to observe the backup server for new files.

''' 
    
class MyHandler(LoggingEventHandler):
    def __init__(self, q):
        super().__init__()
        self.q = q
        
    def on_created(self, event):
        if event.event_type == "created" and not event.is_directory:
            self.q.put(event.src_path)
            
            
        
def start_watch(path_to_watch, q):
    
    print("Watching started", flush=True)
    
    handler = MyHandler(q)
    observer = Observer()
    observer.schedule(handler, path=path_to_watch, recursive=True)
    observer.deamon =True
    observer.start()
    
    print("Watching directory:", path_to_watch, flush=True)
    return observer
 
class ObservingFolders():
    
    '''
    On our backup server the files are sorted by month, e.g. "202501", "202502", etc.
    Because the date of the project stays the same throughout the measurements it can 
    happen that in the beginning of the month the files will still be sorted into the last month. 
    Therefore the last month will still be monitored for new files. 
    
    On the first of the month I change the folders that are being monitored.
    I let the script rerun the past two months and the now current month to check for missing files before it moves on.
    The observers are then closed, the new months to observe are set and the observers are started again.
    
    
    '''
    
    def __init__(self, MassSpecDirectory_ToObserve, FileToWatch_Metadata):
        self.MassSpecDirectory_ToObserve = MassSpecDirectory_ToObserve
        self.FileToWatch_Metadata = FileToWatch_Metadata
        self.DirectoryObserver1 = ""
        self.DirectoryObserver2 = ""
        self.DirectoryObserver3 = ""
        
    def Redefine_Directory( self, stop_event):
        
        print("Running Redefine Directory")
        
        while not stop_event.is_set(): 
            
            print("Rerunning past 2 months")
            self.RerunningTwoMonths(stop_event) 
            print("Finished rerunning", flush =True)
            
            self.ClosingObservations()    
            print("Observers closed",flush= True)
            
            if stop_event.is_set():
                break
            
            currentMonth_Date = date.today()
            lastMonth = currentMonth_Date + dateutil.relativedelta.relativedelta(months=-1)
            lastMonth = lastMonth.strftime("%Y%m")
            TwoMonthsAgo = currentMonth_Date + dateutil.relativedelta.relativedelta(months=-2)
            TwoMonthsAgo = TwoMonthsAgo.strftime("%Y%m")
            OneMonthAgo = date.today() -timedelta(days=30)
            OneMonthAgo  = OneMonthAgo.strftime("%Y%m%d")
            nextMonth_date = currentMonth_Date + dateutil.relativedelta.relativedelta(months=+1)
            nextMonth =nextMonth_date.strftime("%Y%m")
            currentMonth  = date.today().strftime("%Y%m")
            
            LastMonths_Directory =  Path(os.path.join(self.MassSpecDirectory_ToObserve, lastMonth)).as_posix()
            ThisMonths_Directory =  Path(os.path.join(self.MassSpecDirectory_ToObserve, currentMonth)).as_posix()
            NextMonths_Directory =  Path(os.path.join(self.MassSpecDirectory_ToObserve, nextMonth)).as_posix()
            
            print("Folders updated, now restarting observer")
            
            if os.path.isdir(LastMonths_Directory):
                self.DirectoryObserver1 = start_watch(LastMonths_Directory, q)
            
            if os.path.isdir(ThisMonths_Directory):
                self.DirectoryObserver2 = start_watch(ThisMonths_Directory, q)
    
            if os.path.isdir(NextMonths_Directory):
                self.DirectoryObserver3 = start_watch(NextMonths_Directory, q)
            
            print("Observer started")
           
            nextMonth = nextMonth_date.replace(day=1)
            timeuntilFirst = nextMonth -currentMonth_Date
            timeuntilFirst =  timeuntilFirst.total_seconds()
            stop_event.wait(timeout =timeuntilFirst) 
            
            
    def ClosingObservations(self):
        
        for observer in [self.DirectoryObserver1, self.DirectoryObserver2, self.DirectoryObserver3]:
            if not isinstance(observer, str):              
                if observer.is_alive():
                    observer.stop()
                    observer.join()
           
       
    def RerunningTwoMonths(self, stop_event):
       
       '''
        Here I check the past two months whether files are missing in the database and 
        whether there are temporary files that still need to be inserted. 
       '''
        
       currentMonth_Date = date.today()
       lastMonth = currentMonth_Date + dateutil.relativedelta.relativedelta(months=-1)
       lastMonth = lastMonth.strftime("%Y%m")
       TwoMonthsAgo = currentMonth_Date + dateutil.relativedelta.relativedelta(months=-2)
       TwoMonthsAgo = TwoMonthsAgo.strftime("%Y%m")
       currentMonth = date.today().strftime("%Y%m")
       
       DoubleCheck = [TwoMonthsAgo, lastMonth,currentMonth]
       SQL_DB = Execute_CreateSQLdbCode(self.FileToWatch_Metadata)
       loop_stop_event1 = False
       
       while (not stop_event.is_set()) and (not loop_stop_event1):
            try:
                for Directory in DoubleCheck:
                    Directory_joined = os.path.join(self.MassSpecDirectory_ToObserve, Directory)
                    ListRawFiles = os.listdir(Directory_joined)
                    ListRawFiles = [file for file in ListRawFiles if re.search(".raw",file)]
                    ListMissingRawFiles = SQL_DB.MissingFilesFromDatabase(ListRawFiles)
                 
                    print(f"Missing from Directory {Directory}: {len(ListMissingRawFiles)} files", flush = True)
                    for rawFileTup in ListMissingRawFiles:
                       
                        rawFile, = rawFileTup  
                  
                        if Path(rawFile).suffix == ".raw":
                            
                            if SQL_DB.SampleNotInDatabase(rawFile):
                                rawFile_fullpath = os.path.join(Directory_joined, rawFile)
                                rawFile_fullpath = Path(rawFile_fullpath).as_posix()
                                print(rawFile, flush = True)
                                try:
                                    
                                    SQL_DB.FillDatabase(rawFile_fullpath) 
                                    print(" DB Updated", flush =True)
                                
                                except SafedAsJsonTempFile:
                                    print("Data stored in temp file", flush =True)    
                                
                                except sqlite3.IntegrityError:
                                    with open(Logfile_Integrity, "a") as logfile:
                                        logfile.write((rawFile_fullpath + "\n"))
                                    print("File already in DB", flush =True)
                      
                                except HandlingCorruptFileError:
                                    with open(Logfile_corrupt, "a") as logfile:
                                        logfile.write((rawFile_fullpath + "\n"))
                                    print("Error while loading file, needs further inspection", flush =True)
                                    
                                except HandlingEmptyFileError:
                                    filesize = os.path.getsize(rawFile_fullpath)/1000
                                    if filesize < 15000:
                                        with open(Logfile_corrupt, "a") as logfile:
                                            logfile.write((rawFile_fullpath + "\n"))
                                        print("No Scans found in file and size below 15000 kb: file likely corrupt or empty.", flush =True)  
                                        
                                        try:
                                            SQL_DB.FillDatabase_Error(rawFile, "CorruptFile")
                                            
                                        except sqlite3.IntegrityError:
                                            print("Corrupt file already in DB", flush =True)
                                    else:    
                                        with open(Logfile_empty, "a") as logfile:
                                            logfile.write((rawFile_fullpath + "\n"))
                                        print("No Scans found in file, but filesize above 15000 kb, needs further inspection.", flush =True)    
                 
                        if stop_event.is_set():
                            break
                    if stop_event.is_set():
                        break    
                    
                    loop_stop_event1 = True
                    
                loop_stop_event2 = False    
                
                print("Start going through temporary files", flush =True)
                print(f"{len(os.listdir(TempFolder))} files in TEMP folder", flush = True)

                DaysAgo = date.today() -timedelta(days=5)
                DaysAgo  = DaysAgo.strftime("%Y%m%d") 
                
                while (not stop_event.is_set()) and (not loop_stop_event2):         
                                  
                    for file in os.listdir(TempFolder):
                      
                        ProjectID, ProjectID_regex, ProjectID_regex_sql, ProjectID_Date = SplitProjectName(file)

                        print(f'Sample not in DB yet?: {SQL_DB.SampleNotInDatabase(file)}')   
                        
                        if SQL_DB.SampleNotInDatabase(file):
                            try:
                                SQL_DB.FillDatabaseWithJson(file)
                                
                            except NoFittingProjectFound:
                                if ProjectID_Date < DaysAgo:
                                    try:
                                        SQL_DB.FillDatabaseWithJson_KeepProject(file) 
                                        
                                    except sqlite3.IntegrityError:
                                          with open(Logfile_Integrity, "a") as logfile:
                                              logfile.write((file + "Json" + "\n"))
                                          print("File already in DB, TEMP file deleted", flush =True)  
                                          os.remove(os.path.join(TempFolder, file))  
                                     
                            except sqlite3.IntegrityError:
                                  with open(Logfile_Integrity, "a") as logfile:
                                      logfile.write((file + "Json" + "\n"))
                                  print("File already in DB, TEMP file deleted", flush =True)  
                                  os.remove(os.path.join(TempFolder, file))     
                                                                                           
                        else:     
                            os.remove(os.path.join(TempFolder, file))  
                            print(f' Sample already in DB. TEMP file {file} deleted', flush = True)                  

                        loop_stop_event2 = True                          
                           
                        if stop_event.is_set():
                             break  
                         
                    loop_stop_event2 = True  
                loop_stop_event2 = True           
                
                print("Temporary files done", flush =True)           
                
            except KeyboardInterrupt:
                
                ConnectionToRawfile = CreateMetaDataLists(rawFile_fullpath)
                ConnectionToRawfile.ClosingRawFile()
                print("Keyboard Interrupt triggered, rawfile was closed, now closing python")
                sys.exit(0)
                
       for Directory in DoubleCheck:
            Directory_joined = os.path.join(self.MassSpecDirectory_ToObserve, Directory)
            ListRawFiles = os.listdir(Directory_joined)
            ListRawFiles = [file for file in ListRawFiles if re.search(".raw",file)]
            ListMissingRawFiles = SQL_DB.MissingFilesFromDatabase(ListRawFiles)
         
            print(f"Missing from directory {Directory} after checking past two months: {len(ListMissingRawFiles)} files", flush = True)      
                   
            
                    