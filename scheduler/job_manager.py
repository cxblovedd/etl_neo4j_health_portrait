from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from config.settings import Config
from etl.utils.logger import setup_logger
from etl.utils.api import HealthPortraitAPI
from etl.processors.health_portrait import HealthPortraitProcessor
import json

logger = setup_logger('job_manager')

class JobManager:
    def __init__(self):
        self.api = HealthPortraitAPI()
        self.processor = HealthPortraitProcessor()
        self.error_queue = Queue()
    
    def process_batch(self, empi_list):
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_empi = {
                executor.submit(self._process_single, empi): empi 
                for empi in empi_list
            }
            
            for future in as_completed(future_to_empi):
                empi = future_to_empi[future]
                try:
                    if not future.result():
                        self.error_queue.put(empi)
                except Exception as e:
                    logger.error(f"处理失败 - EMPI: {empi}, 错误: {str(e)}")
                    self.error_queue.put(empi)
    
    def _process_single(self, empi):
        # 获取数据
        patient_data = self.api.get_health_portrait(empi)
        

        # with open("patient.json", 'r', encoding='utf-8') as f:
        #     patient_data = json.load(f)
        
        if not patient_data:
            return False
            
        # 处理数据
        return self.processor.process(patient_data)
    
    def retry_failed(self):
        failed_empis = []
        while not self.error_queue.empty():
            failed_empis.append(self.error_queue.get())
        
        if failed_empis:
            logger.info(f"重试 {len(failed_empis)} 条失败记录")
            self.process_batch(failed_empis)