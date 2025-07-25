title Check Ingestion Status
set currentDirectory=%cd%
echo %currentDirectory%
echo %date% %time%
%currentDirectory%/WinPython/python/python.exe %currentDirectory%/CheckIngestionStatus.py %*
pause
