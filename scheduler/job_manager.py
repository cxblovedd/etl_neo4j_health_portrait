from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from core.config import Config
from core.logger import setup_logger
from etl.extractors.health_portrait_api import HealthPortraitAPI
from etl.processors.health_portrait import HealthPortraitProcessor
import time

logger = setup_logger('job_manager')

class JobManager:
    def __init__(self):
        self.api = HealthPortraitAPI()
        self.processor = HealthPortraitProcessor()
        self.error_queue = Queue()

    def _fetch_patient_data(self, empi):
        start_time = time.perf_counter()
        data = self.api.get_health_portrait(empi)
        elapsed = time.perf_counter() - start_time
        return data, elapsed

    def _process_patient_graph(self, empi, data):
        start_time = time.perf_counter()
        result = self.processor.process(data)
        elapsed = time.perf_counter() - start_time
        return result, elapsed
    
    def process_batch(self, empi_list):
        batch_start_time = time.perf_counter()
        patient_data_map = {}
        fetch_durations = {}
        
        # 1. 阶段一：并发获取所有患者数据
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_empi = {
                executor.submit(self._fetch_patient_data, empi): empi 
                for empi in empi_list
            }
            for future in as_completed(future_to_empi):
                empi = future_to_empi[future]
                try:
                    data, elapsed = future.result()
                    fetch_durations[empi] = elapsed
                    logger.info(f"API拉取完成 - EMPI: {empi}, 耗时: {elapsed:.2f}秒")
                    if data:
                        patient_data_map[empi] = data
                    else:
                        self.error_queue.put(empi)
                except Exception as e:
                    logger.error(f"获取数据失败 - EMPI: {empi}, 错误: {str(e)}")
                    self.error_queue.put(empi)

        if fetch_durations:
            logger.info(
                f"批次API拉取完成，共{len(fetch_durations)}人，总耗时: {time.perf_counter() - batch_start_time:.2f}秒"
            )
        
        if not patient_data_map:
            return

        # 2. 阶段二：单线程批量预创建 Condition 节点
        pre_process_start_time = time.perf_counter()
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
                logger.info(
                    f"Condition预创建阶段完成，共{len(conditions_to_create)}个候选节点，耗时: {time.perf_counter() - pre_process_start_time:.2f}秒"
                )
            except Exception as e:
                logger.error(f"批量预创建 Condition 失败: {str(e)}")
        else:
            logger.info("Condition预创建阶段跳过，没有待创建节点")
        
        # 3. 阶段三：并发处理实体和关系图谱构建
        graph_process_start_time = time.perf_counter()
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_empi = {
                executor.submit(self._process_patient_graph, empi, data): empi 
                for empi, data in patient_data_map.items()
            }
            for future in as_completed(future_to_empi):
                empi = future_to_empi[future]
                try:
                    result, elapsed = future.result()
                    logger.info(f"图谱写入完成 - EMPI: {empi}, 耗时: {elapsed:.2f}秒")
                    if not result:
                        self.error_queue.put(empi)
                except Exception as e:
                    logger.error(f"处理图谱数据失败 - EMPI: {empi}, 错误: {str(e)}")
                    self.error_queue.put(empi)

        logger.info(
            f"批次图谱写入阶段完成，共{len(patient_data_map)}人，耗时: {time.perf_counter() - graph_process_start_time:.2f}秒"
        )
        logger.info(
            f"批次处理完成，共{len(empi_list)}人，总耗时: {time.perf_counter() - batch_start_time:.2f}秒"
        )
    
    def retry_failed(self):
        failed_empis = []
        while not self.error_queue.empty():
            failed_empis.append(self.error_queue.get())
        
        if failed_empis:
            logger.info(f"重试 {len(failed_empis)} 条失败记录")
            self.process_batch(failed_empis)
