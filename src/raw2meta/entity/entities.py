from dataclasses import dataclass
from typing import Optional

@dataclass
class SampleEntry:
    SampleName_ID: str
    ProjectID: str
    CreationDate: str
    Vial: str
    InjectionVolume: float
    InitialPressure_Pump: Optional[float] = None
    MinPressure_Pump: Optional[float] = None
    MaxPressure_Pump: Optional[float] = None
    Std_Pressure_Pump: Optional[float] = None
    AnalyzerTemp_mean: float = None
    AnalyzerTemp_std: float = None

@dataclass
class ProjectEntry:
    ProjectID: str
    ProjectID_Date: str
    MSInstrument: str
    SoftwareVersion: str
    InstrumentMethod_print: str
    HPLCInstrument: str
    TimeRange: str
    FAIMSattached: str