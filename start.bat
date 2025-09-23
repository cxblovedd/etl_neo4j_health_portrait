@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

echo ======================================
echo     健康画像ETL项目启动脚本
echo ======================================

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: 未找到Python环境
    pause
    exit /b 1
)

REM 检查依赖
echo 检查依赖包...
pip list | findstr "neo4j psycopg2 pyodbc requests Flask" >nul 2>&1
if errorlevel 1 (
    echo 警告: 部分依赖包可能未安装，请运行: pip install -r requirements.txt
)

echo.
echo 请选择启动模式:
echo 1. 运行ETL数据处理 (单次执行)
echo 2. 启动ETL定时调度 (持续运行)
echo 3. 启动API服务
echo 4. 查看项目状态
echo 5. 验证项目配置
echo 6. 查看日志
echo.
set /p choice=请输入选项 (1-6): 

if "%choice%"=="1" (
    echo 启动ETL数据处理 (单次执行)...
    python main.py
) else if "%choice%"=="2" (
    echo 启动ETL定时调度...
    echo ETL任务将定时执行，按 Ctrl+C 停止
    set /p hours=请输入调度间隔小时数 (默认24小时): 
    if "!hours!"=="" set hours=24
    python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start(!hours!)"
) else if "%choice%"=="3" (
    echo 启动API服务...
    echo 服务将在 http://localhost:5000 启动
    echo API文档地址: http://localhost:5000/api/docs
    python app.py
) else if "%choice%"=="4" (
    echo 项目状态:
    echo - 配置文件: config\settings.py
    echo - ETL状态: config\etl_state.json
    if exist "config\etl_state.json" (
        echo - 最后执行时间:
        type "config\etl_state.json"
    ) else (
        echo - ETL尚未执行
    )
) else if "%choice%"=="5" (
    echo 验证项目配置...
    python check_config.py
) else if "%choice%"=="6" (
    echo 最近的日志文件:
    dir /b logs\ 2>nul | head -10
    echo.
    set /p logfile=输入要查看的日志文件名 (例如: main.log): 
    if exist "logs\!logfile!" (
        echo 显示最后50行:
        powershell "Get-Content 'logs\!logfile!' | Select-Object -Last 50"
    ) else (
        echo 日志文件不存在
    )
) else (
    echo 无效选项
    pause
    exit /b 1
)

echo.
echo 操作完成!
pause