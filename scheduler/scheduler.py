import schedule
import time
import json
import datetime
import os
from config.settings import Config
from etl.utils.logger import setup_logger
from etl.utils.sqlserver import SQLServerConnection
from scheduler.job_manager import JobManager

logger = setup_logger('scheduler')

class ETLScheduler:
    """ETL定时调度器，负责定时从SQL Server获取患者ID并触发ETL任务"""
    
    def __init__(self):
        self.job_manager = JobManager()
        self.db_connection = SQLServerConnection()
        self.state_file_path = Config.STATE_FILE_PATH
        
    def _load_last_run_time(self):
        """从状态文件加载上次运行时间"""
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)
                    last_run_time = state.get('last_run_time')
                    if last_run_time:
                        return datetime.datetime.fromisoformat(last_run_time)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"读取状态文件出错: {e}")
        return None
        
    def _save_last_run_time(self):
        """保存当前运行时间到状态文件"""
        now = datetime.datetime.now()
        state = {'last_run_time': now.isoformat()}
        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(state, f)
            logger.info(f"已保存运行时间: {now.isoformat()}")
        except Exception as e:
            logger.error(f"保存状态文件出错: {e}")
            
    def run_etl_job(self):
        """执行ETL任务"""
        logger.info("开始执行ETL任务...")
        
        # 加载上次运行时间
        last_run_time = self._load_last_run_time()
        if last_run_time:
            logger.info(f"上次运行时间: {last_run_time.isoformat()}")
        else:
            logger.info("首次运行或无法获取上次运行时间")
            
        # 从SQL Server获取患者ID列表
        patient_ids = self.db_connection.load_patient_ids(last_run_time)
        
        if not patient_ids:
            logger.info("没有需要处理的患者数据")
            return
            
        logger.info(f"获取到{len(patient_ids)}个患者ID，开始处理...")
        
        # 分批处理患者数据
        batch_size = Config.BATCH_SIZE
        for i in range(0, len(patient_ids), batch_size):
            batch = patient_ids[i:i+batch_size]
            logger.info(f"处理批次 {i//batch_size + 1}/{(len(patient_ids)-1)//batch_size + 1}，包含{len(batch)}个患者")
            self.job_manager.process_batch(batch)
            
        # 所有批次处理完成后，统一重试失败任务
        retry_count = 0
        while not self.job_manager.error_queue.empty() and retry_count < Config.RETRY_TIMES:
            retry_count += 1
            failed_count = self.job_manager.error_queue.qsize()
            logger.info(f"重试第{retry_count}次，剩余{failed_count}个失败任务")
            self.job_manager.retry_failed()
            if retry_count < Config.RETRY_TIMES:
                time.sleep(Config.RETRY_DELAY)
                
        # 保存本次运行时间
        self._save_last_run_time()
        logger.info("ETL任务执行完成")
        
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
        logger.info("执行一次ETL任务")
        self.run_etl_job()
        
if __name__ == "__main__":
    # 测试代码
    scheduler = ETLScheduler()
    scheduler.run_once()  # 执行一次
    # scheduler.start(24)  # 每24小时执行一次