import requests
from config.settings import Config
from .logger import setup_logger

logger = setup_logger('api')

class HealthPortraitAPI:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = Config.BIGDATA_API_BASE_URL
    
    def get_health_portrait(self, patientId):
        url = f"{self.base_url}/datafactory/getHealthPortrait"
        try:
            response = self.session.get(
                url, 
                params={"patientId": patientId},
                timeout=Config.BIGDATA_API_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            if data["code"] == 0:
                return data["data"]
            logger.error(f"API错误 - patientId: {patientId}, 消息: {data['msg']}")
        except Exception as e:
            logger.error(f"API请求失败 - patientId: {patientId}, 错误: {str(e)}")
        return None