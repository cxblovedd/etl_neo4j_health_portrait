#!/bin/bash

# 健康画像ETL项目启动脚本

echo "======================================"
echo "    健康画像ETL项目启动脚本"
echo "======================================"

# 检查Python环境
python --version
if [ $? -ne 0 ]; then
    echo "错误: 未找到Python环境"
    exit 1
fi

# 检查依赖
echo "检查依赖包..."
pip list | grep -E "(neo4j|psycopg2|pyodbc|requests|Flask)" > /dev/null
if [ $? -ne 0 ]; then
    echo "警告: 部分依赖包可能未安装，请运行: pip install -r requirements.txt"
fi

# 选择启动模式
echo ""
echo "请选择启动模式:"
echo "1. 运行ETL数据处理 (单次执行)"
echo "2. 启动ETL定时调度 (持续运行)"
echo "3. 启动API服务"
echo "4. 查看项目状态"
echo "5. 验证项目配置"
echo "6. 查看日志"
echo ""
read -p "请输入选项 (1-6): " choice

case $choice in
    1)
        echo "启动ETL数据处理 (单次执行)..."
        python main.py
        ;;
    2)
        echo "启动ETL定时调度..."
        echo "ETL任务将定时执行，按 Ctrl+C 停止"
        read -p "请输入调度间隔小时数 (默认24小时): " hours
        hours=${hours:-24}
        python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start($hours)"
        ;;
    3)
        echo "启动API服务..."
        echo "服务将在 http://localhost:5000 启动"
        echo "API文档地址: http://localhost:5000/api/docs"
        python app.py
        ;;
    4)
        echo "项目状态:"
        echo "- 配置文件: config/settings.py"
        echo "- ETL状态: config/etl_state.json"
        if [ -f "config/etl_state.json" ]; then
            echo "- 最后执行时间:"
            cat config/etl_state.json
        else
            echo "- ETL尚未执行"
        fi
        ;;
    4)
        echo "最近的日志文件:"
        ls -la logs/ | head -10
        echo ""
        read -p "输入要查看的日志文件名 (例如: main.log): " logfile
        if exist "logs/$logfile"; then
            tail -50 "logs/$logfile"
        else
            echo "日志文件不存在"
        fi
        ;;
    *)
        echo "无效选项"
        exit 1
        ;;
esac

echo ""
echo "操作完成!"