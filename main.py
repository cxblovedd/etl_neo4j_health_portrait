import datetime # 导入 datetime 模块
import json # 用于读写状态文件
import os # 用于文件路径操作
import time # 用于重试延迟
from datetime import timezone, timedelta # 用于时区处理

from config.settings import Config
from scheduler.job_manager import JobManager
from etl.utils.logger import setup_logger
from etl.utils.sqlserver import SQLServerConnection

logger = setup_logger('main')

# 定义状态文件的路径
STATE_FILE = Config.STATE_FILE_PATH

def load_last_load_timestamp():
    """从状态文件加载上次成功加载的时间戳"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
                timestamp_str = state.get("last_successful_load_time")
                if timestamp_str:
                    # 将北京时间格式的字符串转换回datetime对象
                    # 支持两种格式：新的北京时间格式和旧的UTC格式（向后兼容）
                    try:
                        # 尝试解析北京时间格式: 2025-09-23 17:53:31 (Beijing)
                        if ' (Beijing)' in timestamp_str:
                            beijing_tz = timezone(timedelta(hours=8))
                            clean_timestamp = timestamp_str.replace(' (Beijing)', '')
                            dt = datetime.datetime.strptime(clean_timestamp, '%Y-%m-%d %H:%M:%S')
                            return dt.replace(tzinfo=beijing_tz)
                        else:
                            # 兼容旧的ISO格式
                            return datetime.datetime.fromisoformat(timestamp_str)
                    except ValueError:
                        # 如果解析失败，尝试ISO格式
                        return datetime.datetime.fromisoformat(timestamp_str)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read or parse state file {STATE_FILE}: {e}. Assuming no previous run.")
    return None

def save_last_load_timestamp(timestamp):
    """将当前成功加载的时间戳保存到状态文件"""
    try:
        # 转换为北京时间格式
        beijing_tz = timezone(timedelta(hours=8))
        beijing_time = timestamp.astimezone(beijing_tz)
        beijing_time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S') + ' (Beijing)'
        
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            # 保存为北京时间格式的字符串
            json.dump({"last_successful_load_time": beijing_time_str}, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved current load timestamp to {STATE_FILE}: {beijing_time_str}")
    except IOError as e:
        logger.error(f"Could not write to state file {STATE_FILE}: {e}")

def load_empi_list(last_load_timestamp=None):
    """
    从 SQL Server 数据库的 ai_patients 表中加载 patient_id 列表。
    如果提供了 last_load_timestamp，则只加载在该时间之后更新的记录。
    """
    empi_list = []
    sql_conn = None
    try:
        logger.info(f"Connecting to SQL Server database '{Config.SQL_DATABASE}' on {Config.SQL_HOST}:{Config.SQL_PORT} to load EMPI list...")
        sql_conn = SQLServerConnection()
        empi_list = sql_conn.load_patient_ids(last_load_timestamp)
        logger.info(f"Successfully loaded {len(empi_list)} EMPIs from SQL Server.")
        
    except Exception as e:
        logger.error(f"SQL Server Error: {e}")
        return []
    finally:
        if sql_conn:
            sql_conn.close()
            logger.info("SQL Server connection closed.")
            
    return empi_list

def main():
    job_manager = JobManager()
    all_batches_successful = True # 标志所有批次是否都成功处理（包括重试）
    
    # 获取当前时间，作为本次运行的"开始时间"
    # 如果所有操作都成功，这个时间将作为下次运行的 "last_load_timestamp"
    beijing_tz: timezone = timezone(timedelta(hours=8))
    current_run_start_time = datetime.datetime.now(tz=beijing_tz) # 使用北京时间

    try:
        last_successful_run_time = load_last_load_timestamp()
        
        empi_list = load_empi_list(last_load_timestamp=last_successful_run_time)
        
        if not empi_list:
            logger.warning("EMPI list is empty. No new data to process since last run or no data at all.")
            # 即使没有数据处理，也应该更新时间戳，表示我们检查过了
            save_last_load_timestamp(current_run_start_time)
            return

        total_batches = (len(empi_list) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
        
        for i in range(0, len(empi_list), Config.BATCH_SIZE):
            batch = empi_list[i:i + Config.BATCH_SIZE]
            logger.info(f"处理第 {i//Config.BATCH_SIZE + 1}/{total_batches} 批，{len(batch)} 条记录")
            job_manager.process_batch(batch) # process_batch 内部处理错误并放入 error_queue
        
        # 重试失败记录
        retry_count = 0
        while not job_manager.error_queue.empty() and retry_count < Config.RETRY_TIMES:
            retry_count += 1
            failed_count_before = job_manager.error_queue.qsize()
            logger.info(f"重试第{retry_count}次，剩余{failed_count_before}个失败任务...")
            
            job_manager.retry_failed()
            
            failed_count_after = job_manager.error_queue.qsize()
            if failed_count_after == 0:
                logger.info(f"第{retry_count}次重试后，所有任务已成功处理")
                break
            elif failed_count_after < failed_count_before:
                logger.info(f"第{retry_count}次重试后，还剩{failed_count_after}个失败任务")
            else:
                logger.warning(f"第{retry_count}次重试没有减少失败任务数量")
            
            # 重试间隔
            if retry_count < Config.RETRY_TIMES and not job_manager.error_queue.empty():
                logger.info(f"等待{Config.RETRY_DELAY}秒后进行下一次重试...")
                time.sleep(Config.RETRY_DELAY)
        
        # 检查最终结果
        if not job_manager.error_queue.empty():
            all_batches_successful = False
            failed_count = job_manager.error_queue.qsize()
            logger.error(f"{failed_count} EMPIs still failed after {retry_count} retries.")
            
            # 记录最终失败的EMPIs（可选）
            if failed_count <= 10:  # 只记录少量失败记录，避免日志过长
                failed_empis = []
                temp_queue = []
                while not job_manager.error_queue.empty():
                    empi = job_manager.error_queue.get_nowait()
                    failed_empis.append(empi)
                    temp_queue.append(empi)
                
                # 重新放回队列
                for empi in temp_queue:
                    job_manager.error_queue.put(empi)
                
                logger.error(f"Final failed EMPIs: {failed_empis}")
            else:
                logger.error(f"Too many failed EMPIs ({failed_count}), not listing individually.")
        
        if all_batches_successful:
            logger.info("All batches processed successfully (including retries).")
            save_last_load_timestamp(current_run_start_time)
        else:
            logger.warning("Some EMPIs failed to process even after retries. Last load timestamp will not be updated.")
            logger.info("运行完成，但有部分数据处理失败。请检查日志了解详情。")
            
    except Exception as e:
        logger.error(f"程序执行错误: {str(e)}", exc_info=True) # 添加 exc_info=True 来记录堆栈跟踪
        logger.error("由于发生严重错误，本次ETL任务将不更新状态时间戳")
        # 一般性的程序错误，不更新时间戳
        raise
    finally:
        # 清理资源
        try:
            if job_manager and hasattr(job_manager, 'processor') and hasattr(job_manager.processor, 'db') and job_manager.processor.db:
                job_manager.processor.db.close()
                logger.info("Neo4j connection closed via JobManager.")
        except Exception as cleanup_error:
            logger.error(f"Error closing Neo4j connection: {cleanup_error}")
        
        logger.info("ETL任务执行结束")

if __name__ == "__main__":
    main()