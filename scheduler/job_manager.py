from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from core.config import Config
from core.logger import setup_logger
from etl.extractors.health_portrait_api import HealthPortraitAPI
from etl.processors.health_portrait import HealthPortraitProcessor
import json
import time

logger = setup_logger('job_manager')

class JobManager:
    def __init__(self):
        self.api = HealthPortraitAPI()
        self.processor = HealthPortraitProcessor()
        self.error_queue = Queue()
    
    def process_batch(self, empi_list):
        patient_data_map = {}
        
        # 1. 阶段一：并发获取所有患者数据
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_empi = {
                executor.submit(self.api.get_health_portrait, empi): empi 
                for empi in empi_list
            }
            for future in as_completed(future_to_empi):
                empi = future_to_empi[future]
                try:
                    data = future.result()
                    if data:
                        patient_data_map[empi] = data
                    else:
                        self.error_queue.put(empi)
                except Exception as e:
                    logger.error(f"获取数据失败 - EMPI: {empi}, 错误: {str(e)}")
                    self.error_queue.put(empi)
        
        if not patient_data_map:
            return

        # 2. 阶段二：单线程批量预创建 Condition 节点
        conditions_to_create = []
        unique_cond_keys = set()
        
        for data in patient_data_map.values():
            encounters = data.get('encounters', []) or []
            for enc in encounters:
                for diag in (enc.get('diagnoses', []) or []):
                    code = diag.get('diagnosisNo')
                    name = diag.get('diagnosisName')
                    if code or name:
                        key = f"code:{code}" if code else f"name:{name}"
                        if key not in unique_cond_keys:
                            unique_cond_keys.add(key)
                            conditions_to_create.append({'code': code, 'name': name})
                            
                for exam in (enc.get('examinations', []) or []):
                    for finding in (exam.get('findings', []) or []):
                        name = finding.get('diagnosisResult')
                        code = finding.get('diagnosisCode')
                        if name:
                            key = f"name:{name}"
                            if key not in unique_cond_keys:
                                unique_cond_keys.add(key)
                                conditions_to_create.append({'code': code, 'name': name})
                                
            fh_list = data.get('familyHistoryList', []) or []
            for fh in fh_list:
                name = fh.get('relativeDisease')
                if name and name != '不详':
                    key = f"name:{name}"
                    if key not in unique_cond_keys:
                        unique_cond_keys.add(key)
                        conditions_to_create.append({'code': None, 'name': name})

        if conditions_to_create:
            try:
                self.processor.pre_process_conditions(conditions_to_create)
            except Exception as e:
                logger.error(f"批量预创建 Condition 失败: {str(e)}")
        
        # 3. 阶段三：并发处理实体和关系图谱构建
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_empi = {
                executor.submit(self.processor.process, data): empi 
                for empi, data in patient_data_map.items()
            }
            for future in as_completed(future_to_empi):
                empi = future_to_empi[future]
                try:
                    if not future.result():
                        self.error_queue.put(empi)
                except Exception as e:
                    logger.error(f"处理图谱数据失败 - EMPI: {empi}, 错误: {str(e)}")
                    self.error_queue.put(empi)
    
    def retry_failed(self):
        failed_empis = []
        while not self.error_queue.empty():
            failed_empis.append(self.error_queue.get())
        
        if failed_empis:
            logger.info(f"重试 {len(failed_empis)} 条失败记录")
            self.process_batch(failed_empis)