import datetime # 导入 datetime 模块
import json # 用于读写状态文件
import os # 用于文件路径操作
import time # 用于重试延迟
from datetime import timezone, timedelta # 用于时区处理

from core.config import Config
from scheduler.job_manager import JobManager
from core.logger import setup_logger
from etl.extractors.sqlserver import SQLServerConnection

logger = setup_logger('main')

# 定义状态文件的路径
STATE_FILE = Config.STATE_FILE_PATH

def _parse_state_timestamp(timestamp_str):
    """兼容历史状态格式，统一返回带时区的 datetime。
    支持：
    1. "2026-06-10 12:00:00" (无时区，默认为北京时区)
    2. "2026-06-10T12:00:00"
    3. "2026-06-10T12:00:00+08:00" (标准 ISO 格式)
    4. "2026-06-10 12:00:00 (Beijing)" (历史格式)
    """
    if not timestamp_str:
        return None

    # 1. 兼容历史带 "(Beijing)" 的字符串
    if ' (Beijing)' in timestamp_str:
        beijing_tz = timezone(timedelta(hours=8))
        clean_timestamp = timestamp_str.replace(' (Beijing)', '')
        dt = datetime.datetime.strptime(clean_timestamp, '%Y-%m-%d %H:%M:%S')
        return dt.replace(tzinfo=beijing_tz)

    # 2. 尝试解析标准 ISO 格式
    clean_str = timestamp_str.replace(' ', 'T')
    try:
        dt = datetime.datetime.fromisoformat(clean_str)
        # 如果无时区信息，默认赋予北京时区 (+8)
        if dt.tzinfo is None:
            beijing_tz = timezone(timedelta(hours=8))
            dt = dt.replace(tzinfo=beijing_tz)
        return dt
    except ValueError:
        # 3. 兜底解析 "%Y-%m-%d %H:%M:%S" 格式
        try:
            dt = datetime.datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
            beijing_tz = timezone(timedelta(hours=8))
            return dt.replace(tzinfo=beijing_tz)
        except ValueError:
            raise ValueError(f"无法解析时间戳: {timestamp_str}")

def _serialize_state_timestamp(timestamp):
    """统一使用 UTC ISO 8601 格式持久化状态。"""
    return timestamp.astimezone(timezone.utc).isoformat(timespec='microseconds')

def load_last_load_timestamp():
    """从状态文件加载上次成功加载的时间戳"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                state = json.load(f)
                timestamp_str = state.get("last_successful_load_time")
                if timestamp_str:
                    try:
                        return _parse_state_timestamp(timestamp_str)
                    except ValueError:
                        logger.warning(f"Invalid timestamp format in state file: {timestamp_str}")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not read or parse state file {STATE_FILE}: {e}. Assuming no previous run.")
    return None

def save_last_load_timestamp(timestamp):
    """将当前成功加载的时间戳保存到状态文件"""
    try:
        timestamp_str = _serialize_state_timestamp(timestamp)
        
        temp_file = STATE_FILE + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump({"last_successful_load_time": timestamp_str}, f, ensure_ascii=False, indent=2)
            
        # 原子替换，防止写入中断导致文件损坏
        os.replace(temp_file, STATE_FILE)
        logger.info(f"Saved current load timestamp to {STATE_FILE}: {timestamp_str}")
    except IOError as e:
        logger.error(f"Could not write to state file {STATE_FILE}: {e}")

def load_failed_patients():
    """从状态文件加载上次失败的患者ID列表"""
    file_path = Config.FAILED_STATE_FILE_PATH
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("failed_patient_ids", [])
        except Exception as e:
            logger.warning(f"Could not read failed patients file: {e}")
    return []

def save_failed_patients(failed_ids):
    """将失败的患者ID列表保存到状态文件"""
    file_path = Config.FAILED_STATE_FILE_PATH
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        temp_file = file_path + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump({"failed_patient_ids": failed_ids}, f, ensure_ascii=False, indent=2)
        # 原子替换，防止写入中断导致文件损坏
        os.replace(temp_file, file_path)
        logger.info(f"Saved {len(failed_ids)} failed EMPIs to {file_path}")
    except IOError as e:
        logger.error(f"Could not write to failed patients file {file_path}: {e}")

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
    
    # 获取当前时间，作为本次运行的"开始时间"
    # 如果所有操作都成功，这个时间将作为下次运行 of "last_load_timestamp"
    current_run_start_time = datetime.datetime.now(tz=timezone.utc)

    try:
        last_successful_run_time = load_last_load_timestamp()
        
        # 1. 加载上次运行失败的患者ID以进行重试
        previously_failed_ids = load_failed_patients()
        if previously_failed_ids:
            logger.info(f"Loaded {len(previously_failed_ids)} previously failed EMPIs to retry.")
            
        # 2. 从 SQL Server 加载增量更新的患者ID
        new_empi_list = load_empi_list(last_load_timestamp=last_successful_run_time)
        
        # 3. 合并新老数据并去重
        empi_set = set(previously_failed_ids) | set(new_empi_list)
        empi_list = list(empi_set)
        
        if not empi_list:
            logger.warning("No new or failed EMPIs to process.")
            # 即使没有数据处理，也应该更新时间戳，表示我们检查过了，同时清空失败文件记录
            save_last_load_timestamp(current_run_start_time)
            save_failed_patients([])
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
        
        # 收集最终失败的患者ID并保存，以防止后续运行遗漏，同时清空队列释放内存
        final_failed_empis = []
        while not job_manager.error_queue.empty():
            final_failed_empis.append(job_manager.error_queue.get_nowait())
            
        if final_failed_empis:
            logger.error(f"{len(final_failed_empis)} EMPIs still failed after all retries.")
            if len(final_failed_empis) <= 10:
                logger.error(f"Final failed EMPIs: {final_failed_empis}")
            else:
                logger.error("Too many failed EMPIs, not listing individually.")
            save_failed_patients(final_failed_empis)
        else:
            logger.info("All EMPIs processed successfully.")
            save_failed_patients([]) # 运行成功，清空历史失败记录
            
        # 无论本次运行是否有部分数据处理失败，由于失败患者ID已单独持久化在文件里，
        # 我们可以安全地保存本次开始时间作为增量更新的 last_successful_load_time，防止大量已成功的患者重复拉取。
        save_last_load_timestamp(current_run_start_time)
            
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
