@echo off

echo Setting up Python environment for Raw2Meta...
echo ================================================

REM Store the directory where this bat file is located
set "PROJECT_DIR=%~dp0"
REM Remove trailing backslash
if "%PROJECT_DIR:~-1%"=="\" set "PROJECT_DIR=%PROJECT_DIR:~0,-1%"

REM Change to project directory
cd /d "%PROJECT_DIR%"

echo Project directory: %PROJECT_DIR%

REM Check if WinPython is extracted
if not exist "%PROJECT_DIR%\WinPython" (
    if exist "%PROJECT_DIR%\WinPython.7z" (
        echo WinPython.7z found but not extracted. Please extract it first.
        echo You can use 7zip to extract WinPython.7z to create a WinPython folder
        echo Or manually extract it and rerun this script
        pause
        exit /b 1
    ) else (
        echo ERROR: WinPython.7z not found in project directory
        pause
        exit /b 1
    )
)

REM Find Python executable in WinPython
for /f "delims=" %%i in ('dir /b /s "%PROJECT_DIR%\WinPython\python*.exe" 2^>nul ^| findstr /v Scripts ^| findstr /v DLLs') do (
    set "PYTHON_EXE=%%i"
    goto :found_python
)

echo ERROR: Could not find python.exe in WinPython folder
echo Please check that WinPython is properly extracted
pause
exit /b 1

:found_python
echo Found Python: %PYTHON_EXE%
"%PYTHON_EXE%" --version



REM Activate virtual environment and install packages
echo Activating virtual environment...
call "%PROJECT_DIR%\.venv\Scripts\activate.bat"

echo Installing packages...
if exist "%PROJECT_DIR%\requirements.txt" (
    pip install -r "%PROJECT_DIR%\requirements.txt"
) else (
    echo Installing basic packages...
    pip install APScheduler pandas numpy matplotlib plotly watchdog pyyaml python-dateutil pythonnet pywin32
)

echo Setup complete! Virtual environment is ready.

