import schedule
import time
import logging
from core.logger import setup_logger
from main import main as run_etl

logger = setup_logger('scheduler')

class ETLScheduler:
    """ETL定时调度器，定时运行项目主ETL流程"""
    
    def __init__(self):
        pass
        
    def run_etl_job(self):
        """执行ETL任务"""
        logger.info("开始执行定期ETL任务...")
        try:
            run_etl()
            logger.info("定期ETL任务执行成功。")
        except Exception as e:
            logger.error(f"定期ETL任务执行中发生异常: {e}", exc_info=True)
            
    def start(self, interval_hours=24):
        """启动定时调度"""
        logger.info(f"启动ETL定时调度，间隔{interval_hours}小时")
        
        # 设置定时任务
        schedule.every(interval_hours).hours.do(self.run_etl_job)
        
        # 立即执行一次
        logger.info("立即执行一次ETL任务")
        self.run_etl_job()
        
        # 持续运行定时任务
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次是否有待执行的任务
            
    def run_once(self):
        """执行一次ETL任务"""
        logger.info("手动执行一次ETL任务")
        self.run_etl_job()
        
if __name__ == "__main__":
    scheduler = ETLScheduler()
    scheduler.run_once()  # 执行一次
