# PowerShell Startup Script - Health Portrait ETL Project
# Usage: .\start.ps1 or powershell -ExecutionPolicy Bypass -File start.ps1

param(
    [string]$Mode = "",
    [int]$Hours = 24
)

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "    Health Portrait ETL Startup Script" -ForegroundColor Cyan  
Write-Host "======================================" -ForegroundColor Cyan

# Check Python environment
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python Environment: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python environment not found" -ForegroundColor Red
    Read-Host "Press any key to exit"
    exit 1
}

# # Check dependencies
# Write-Host "Checking dependencies..." -ForegroundColor Yellow
# $deps = @("neo4j", "psycopg2", "pyodbc", "requests", "Flask")
# $missing = @()
# foreach ($dep in $deps) {
#     $result = pip show $dep 2>&1
#     if ($LASTEXITCODE -ne 0) {
#         $missing += $dep
#     }
# }

# if ($missing.Count -gt 0) {
#     Write-Host "Warning: Missing packages: $($missing -join ', ')" -ForegroundColor Yellow
#     Write-Host "Please run: pip install -r requirements.txt" -ForegroundColor Yellow
# }

function Show-Menu {
    Write-Host ""
    Write-Host "Select startup mode:" -ForegroundColor White
    Write-Host "1. Run ETL data processing (single execution)" -ForegroundColor White
    Write-Host "2. Start ETL scheduler (continuous)" -ForegroundColor White  
    Write-Host "3. Start API service" -ForegroundColor White
    Write-Host "4. View project status" -ForegroundColor White
    Write-Host "5. Validate project configuration" -ForegroundColor White
    Write-Host "6. View logs" -ForegroundColor White
    Write-Host ""
}

function Invoke-ETL {
    Write-Host "Starting ETL data processing..." -ForegroundColor Green
    python main.py
}

function Invoke-Scheduler {
    Write-Host "Starting ETL scheduler..." -ForegroundColor Green
    Write-Host "ETL task will run every $Hours hours, press Ctrl+C to stop" -ForegroundColor Yellow
    python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start($Hours)"
}

function Invoke-API {
    Write-Host "Starting API service..." -ForegroundColor Green
    Write-Host "Service will start at http://localhost:5000" -ForegroundColor Cyan
    Write-Host "API docs: http://localhost:5000/api/docs" -ForegroundColor Cyan
    python app.py
}

function Show-Status {
    Write-Host "Project status:" -ForegroundColor Green
    Write-Host "- Config file: config\settings.py"
    Write-Host "- ETL state: config\etl_state.json"
    
    if (Test-Path "config\etl_state.json") {
        Write-Host "- Last execution time:" -ForegroundColor Green
        Get-Content "config\etl_state.json" | ConvertFrom-Json | Format-List
    } else {
        Write-Host "- ETL not executed yet" -ForegroundColor Yellow
    }
}

function Invoke-ConfigValidation {
    Write-Host "Validating project configuration..." -ForegroundColor Green
    python check_config.py
}

function Show-Logs {
    Write-Host "Recent log files:" -ForegroundColor Green
    if (Test-Path "logs") {
        Get-ChildItem "logs" -Name | ForEach-Object { Write-Host "  $_" }
        Write-Host ""
        $logfile = Read-Host "Enter log filename to view (e.g. main.log)"
        
        if (Test-Path "logs\$logfile") {
            Write-Host "Showing last 20 lines:" -ForegroundColor Cyan
            Get-Content "logs\$logfile" | Select-Object -Last 20
        } else {
            Write-Host "Log file does not exist" -ForegroundColor Red
        }
    } else {
        Write-Host "No log directory found" -ForegroundColor Red
    }
}

# Main logic
if ($Mode -eq "") {
    do {
        Show-Menu
        $choice = Read-Host "Please enter option (1-6)"
        
        switch ($choice) {
            "1" { Invoke-ETL; break }
            "2" { 
                if ($Hours -eq 24) {
                    $inputHours = Read-Host "Enter schedule interval in hours (default 24)"
                    if ($inputHours -ne "") { $Hours = [int]$inputHours }
                }
                Invoke-Scheduler
                break 
            }
            "3" { Invoke-API; break }
            "4" { Show-Status; break }
            "5" { Invoke-ConfigValidation; break }
            "6" { Show-Logs; break }
            default { 
                Write-Host "Invalid option, please try again" -ForegroundColor Red
                continue
            }
        }
    } while ($false)
} else {
    # Command line mode
    switch ($Mode.ToLower()) {
        "etl" { Invoke-ETL }
        "scheduler" { Invoke-Scheduler }
        "api" { Invoke-API }
        "status" { Show-Status }
        "config" { Invoke-ConfigValidation }
        "logs" { Show-Logs }
        default { 
            Write-Host "Invalid mode: $Mode" -ForegroundColor Red
            Write-Host "Available modes: etl, scheduler, api, status, config, logs" -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "Operation completed!" -ForegroundColor Green
Read-Host "Press any key to exit"