# etl/processors/health_portrait.py

from ..utils.logger import setup_logger
from ..utils.db import Neo4jConnection
# 这里的星号导入已经包含了我们需要的 import_patient_data_from_json 函数
from ..core.etl_patient import *

# 注意: 您项目中的日志记录器似乎有多个版本，这里保留您代码中的版本
# 如果etl.utils.logger中的是health_portrait_logger，则使用 from ..utils.logger import health_portrait_logger as logger
logger = setup_logger('health_portrait')

class HealthPortraitProcessor:
    def __init__(self):
        # 这种方式也可以，但每次process都会创建一个新连接池，如果并发量大建议将db connection设为单例或在外部管理
        self.db = Neo4jConnection()
    
    def process(self, patient_data):
        if not patient_data or not patient_data.get("patientId"):
            logger.warning("接收到空的患者数据，跳过处理。")
            return False  # 明确返回失败状态
            
        try:
            # Neo4j驱动是线程安全的，可以在这里获取session
            with self.db.get_session() as session:
                # 调用内部事务方法
                session.execute_write(self._process_tx, patient_data)
                logger.info(f"处理成功 - PatientId: {patient_data.get('patientId')}")
                return True # 明确返回成功
        except Exception as e:
            # 记录错误日志
            logger.error(f"处理失败 - PatientId: {patient_data.get('patientId')}, 错误: {str(e)}")
            # 【关键修改】向上抛出异常，以便JobManager可以捕获并进行重试
            raise e
    
    def _process_tx(self, tx, patient_data):
        """
        这个方法在数据库事务中执行
        """
        # 这里的调用是正确的
        import_patient_data_from_json(tx, patient_data)
        # 这里不需要返回任何东西，如果发生错误，Neo4j驱动会自动抛出异常