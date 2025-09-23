#!/bin/bash

# ETL定时调度启动脚本

echo "======================================"
echo "    ETL定时调度器"
echo "======================================"

# 检查Python环境
python --version
if [ $? -ne 0 ]; then
    echo "错误: 未找到Python环境"
    exit 1
fi

# 检查schedule依赖
python -c "import schedule" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "错误: 缺少schedule依赖包，请运行: pip install schedule"
    exit 1
fi

echo ""
echo "请选择调度模式:"
echo "1. 启动定时调度 (持续运行)"
echo "2. 执行一次ETL任务"
echo "3. 查看调度状态"
echo ""
read -p "请输入选项 (1-3): " choice

case $choice in
    1)
        echo ""
        read -p "请输入调度间隔小时数 (默认24小时): " hours
        hours=${hours:-24}
        echo ""
        echo "启动ETL定时调度，间隔 $hours 小时..."
        echo "提示:"
        echo "- 任务将立即执行一次，然后每 $hours 小时执行一次"
        echo "- 按 Ctrl+C 停止调度"
        echo "- 日志文件保存在 logs/ 目录"
        echo ""
        python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.start($hours)"
        ;;
    2)
        echo "执行一次ETL任务..."
        python -c "from scheduler.scheduler import ETLScheduler; scheduler = ETLScheduler(); scheduler.run_once()"
        ;;
    3)
        echo "ETL调度状态:"
        echo "- 配置文件: config/settings.py"
        echo "- 状态文件: config/etl_state.json"
        if [ -f "config/etl_state.json" ]; then
            echo "- 最后执行时间:"
            cat config/etl_state.json | python -m json.tool 2>/dev/null || cat config/etl_state.json
        else
            echo "- ETL尚未执行"
        fi
        echo ""
        echo "最近的日志文件:"
        ls -la logs/ | grep -E "(scheduler|main).*\.log" | head -5
        ;;
    *)
        echo "无效选项"
        exit 1
        ;;
esac

echo ""
echo "操作完成!"