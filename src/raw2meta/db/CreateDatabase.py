import sqlite3
from typing import Union
from pathlib import Path


def Database_CreateTables(Metadata_DB: Union[str, Path]) -> None:
    """Create the necessary tables in the database.

    :param Metadata_DB: The path to the metadata database.
    :type Metadata_DB: Union[str, Path]
    :return: None
    :rtype: None
    """
    
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

    with sqlite3.connect(Metadata_DB) as con:
        
        cur = con.cursor()    
        cur.execute(sql1)
        con.commit()
        cur.execute(sql2)
        con.commit()
        cur.execute(sql3)
        con.commit()
