title Replace Corrupt File
set currentDirectory=%cd%
echo %currentDirectory%
%currentDirectory%/WinPython/python/python.exe %currentDirectory%/ETL_InsertSingleCorruptFileEntry.py %*
pause