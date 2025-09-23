@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo ======================================
echo     ETL定时调度器
echo ======================================

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python环境
    pause
    exit /b 1
)

REM 检查schedule依赖
python -c "import schedule" >nul 2>&1
if errorlevel 1 (
    echo 错误: 缺少schedule依赖包，请运行: pip install schedule
    pause
    exit /b 1
)

echo.
echo 请选择调度模式:
echo 1. 启动定时调度 (持续运行)
echo 2. 执行一次ETL任务
echo 3. 查看调度状态
echo.
set /p choice=请输入选项 (1-3): 

if "%choice%"=="1" (
    echo.
    set /p hours=请输入调度间隔小时数 (默认24小时): 
    if "!hours!"=="" set hours=24
    echo.
    echo 启动ETL定时调度，间隔 !hours! 小时...
    echo 提示:
    echo - 任务将立即执行一次，然后每 !hours! 小时执行一次
    echo - 按 Ctrl+C 停止调度
    echo - 日志文件保存在 logs\ 目录
    echo.
    python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start(!hours!)"
) else if "%choice%"=="2" (
    echo 执行一次ETL任务...
    python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.run_once()"
) else if "%choice%"=="3" (
    echo ETL调度状态:
    echo - 配置文件: config\settings.py
    echo - 状态文件: config\etl_state.json
    if exist "config\etl_state.json" (
        echo - 最后执行时间:
        type "config\etl_state.json"
    ) else (
        echo - ETL尚未执行
    )
    echo.
    echo 最近的日志文件:
    dir /b logs\*scheduler*.log logs\*main*.log 2>nul | head -5
) else (
    echo 无效选项
    pause
    exit /b 1
)

echo.
echo 操作完成!
pause