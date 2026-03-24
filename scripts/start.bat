@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo ======================================
echo     Health Portrait ETL Startup Script
echo ======================================

REM Check Python environment
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python environment not found
    pause
    exit /b 1
)

echo.
echo Select startup mode:
echo 1. Run ETL data processing (single execution)
echo 2. Start ETL scheduler (continuous)
echo 3. Start API service
echo 4. View project status
echo 5. Validate project configuration
echo 6. View logs
echo.
set /p choice=Please enter option (1-6): 

if \"%choice%\"==\"1\" (
    echo Starting ETL data processing...
    python main.py
    goto end
)

if \"%choice%\"==\"2\" (
    echo Starting ETL scheduler...
    echo ETL task will run periodically, press Ctrl+C to stop
    set /p hours=Enter schedule interval in hours (default 24): 
    if \"!hours!\"==\"\" set hours=24
    python -c \"from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start(!hours!)\"
    goto end
)

if \"%choice%\"==\"3\" (
    echo Starting API service...
    echo Service will start at http://localhost:5000
    echo API docs: http://localhost:5000/api/docs
    python app.py
    goto end
)

if \"%choice%\"==\"4\" (
    echo Project status:
    echo - Config file: config\\settings.py
    echo - ETL state: config\\etl_state.json
    if exist \"config\\etl_state.json\" (
        echo - Last execution time:
        type \"config\\etl_state.json\"
    ) else (
        echo - ETL not executed yet
    )
    goto end
)

if \"%choice%\"==\"5\" (
    echo Validating project configuration...
    python check_config.py
    goto end
)

if \"%choice%\"==\"6\" (
    echo Recent log files:
    if exist \"logs\" (
        dir /b logs
    ) else (
        echo No log directory found
    )
    echo.
    set /p logfile=Enter log filename to view (e.g. main.log): 
    if exist \"logs\\!logfile!\" (
        echo Showing last 20 lines:
        powershell \"Get-Content 'logs\\!logfile!' | Select-Object -Last 20\"
    ) else (
        echo Log file does not exist
    )
    goto end
)

echo Invalid option
pause
exit /b 1

:end
echo.
echo Operation completed!
pause