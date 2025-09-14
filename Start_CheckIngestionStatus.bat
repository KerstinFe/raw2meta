@echo off

title Raw2Meta Check Ingestion Status
echo Starting Raw2Meta File CheckIngestionStatus...
echo Press Ctrl+C to stop
echo ================================

REM Store the directory where this bat file is located
set "PROJECT_DIR=%~dp0"
REM Remove trailing backslash
if "%PROJECT_DIR:~-1%"=="\" set "PROJECT_DIR=%PROJECT_DIR:~0,-1%"

REM Change to project directory
cd /d "%PROJECT_DIR%"

echo Project directory: %PROJECT_DIR%
echo Current directory: %cd%

REM Check if Python is available on the system
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python and ensure it's accessible from command line
    pause
    exit /b 1
)

REM Check if virtual environment exists and is valid
if not exist "%PROJECT_DIR%\.venv\Scripts\python.exe" (
    echo Virtual environment not found or invalid. Creating new one...
    echo Removing old .venv directory if it exists...
    if exist "%PROJECT_DIR%\.venv" rmdir /s /q "%PROJECT_DIR%\.venv"
    
    echo Creating virtual environment...
    call %PROJECT_DIR%\Setup_Python_Environment_forbat.bat

) else (
echo Virtual environment found, testing whether all requirements are installed...
call %PROJECT_DIR%\Setup_Python_Environment_noDel_forbat.bat
)

REM Set PYTHONPATH to include src directory
set "PYTHONPATH=%PROJECT_DIR%\src;%PYTHONPATH%"

echo Running CheckIngestionStatus...
REM Run the script using the virtual environment's Python
"%PROJECT_DIR%\.venv\Scripts\python.exe" "%PROJECT_DIR%\src\raw2meta\pipeline\pipeline_CheckIngestionStatus.py"

echo Raw2Meta processor stopped.
pause
