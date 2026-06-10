import os
from pathlib import Path
from dotenv import load_dotenv

# CONFIG_DIR 就是 etl_neo4j/config/ 目录的绝对路径
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
# 项目根目录
PROJECT_ROOT = os.path.dirname(CONFIG_DIR)

# 加载 .env 文件中的环境变量
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

class Config:
    # API 调试模式
    API_DEBUG = os.getenv("API_DEBUG", "True").lower() in ("true", "1", "yes")

    # Neo4j配置
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j.haxm.local:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "Weohgust_2025!")
    NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # 大数平台API配置
    BIGDATA_API_BASE_URL = os.getenv("BIGDATA_API_BASE_URL", "http://inside.whitelist.com:1115")
    BIGDATA_API_TIMEOUT = int(os.getenv("BIGDATA_API_TIMEOUT", "10000"))
    
    # 调度配置
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))              # 批处理大小
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "2"))              # 最大并发数
    RETRY_TIMES = int(os.getenv("RETRY_TIMES", "3"))              # 重试次数
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))              # 重试延迟（秒）
    
    # 超时配置
    CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "30"))      # 数据库连接超时
    QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", "300"))          # 查询超时（5分钟）
    
    # 日志配置
    LOG_DIR = os.getenv("LOG_DIR", os.path.join(PROJECT_ROOT, "logs"))  # 使用绝对路径
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE_ENCODING = "utf-8"  # 日志文件编码
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5  # 保留最近5个旧日志文件
    
    # SQL Server 连接配置
    SQL_HOST = os.getenv("SQL_HOST", "10.52.8.78")  # SQL Server IP地址
    SQL_PORT = os.getenv("SQL_PORT", "1433")        # SQL Server 默认端口
    SQL_DATABASE = os.getenv("SQL_DATABASE", "health_portrait") # 数据库名
    SQL_USER = os.getenv("SQL_USER", "health_portrait_user") # 用户名
    SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Yiwenbhu_2025!") # 密码
    SQL_AI_PATIENTS_TABLE = os.getenv("SQL_AI_PATIENTS_TABLE", "ai_patients") # ai_patients 表名
    SQL_PATIENT_ID_COLUMN = os.getenv("SQL_PATIENT_ID_COLUMN", "patient_id") # ai_patients 表中表示患者ID的列名
    SQL_UPDATE_TIME_COLUMN = os.getenv("SQL_UPDATE_TIME_COLUMN", "hxgxsj") # ai_patients 表中表示更新时间的列名
    
    # ETL时间状态文件路径
    STATE_FILE_PATH = os.getenv("STATE_FILE_PATH", os.path.join(PROJECT_ROOT, "data", "state", "etl_state.json"))
    
    # ETL失败患者状态文件路径
    FAILED_STATE_FILE_PATH = os.getenv("FAILED_STATE_FILE_PATH", os.path.join(PROJECT_ROOT, "data", "state", "etl_failed_patients.json"))
    
    @classmethod
    def validate_config(cls):
        """验证配置项的有效性"""
        errors = []
        
        # 验证必要配置
        required_configs = [
            ('NEO4J_URI', cls.NEO4J_URI),
            ('NEO4J_USER', cls.NEO4J_USER), 
            ('NEO4J_PASSWORD', cls.NEO4J_PASSWORD),
            ('BIGDATA_API_BASE_URL', cls.BIGDATA_API_BASE_URL),
            ('SQL_HOST', cls.SQL_HOST),
            ('SQL_DATABASE', cls.SQL_DATABASE)
        ]
        
        for config_name, config_value in required_configs:
            if not config_value:
                errors.append(f"{config_name} 不能为空")
        
        # 验证数值配置
        if cls.BATCH_SIZE <= 0:
            errors.append("BATCH_SIZE 必须大于 0")
        if cls.MAX_WORKERS <= 0:
            errors.append("MAX_WORKERS 必须大于 0")
        if cls.RETRY_TIMES < 0:
            errors.append("RETRY_TIMES 不能为负数")
        if cls.RETRY_DELAY < 0:
            errors.append("RETRY_DELAY 不能为负数")
        
        # 验证目录权限
        try:
            os.makedirs(cls.LOG_DIR, exist_ok=True)
        except Exception as e:
            errors.append(f"无法创建日志目录 {cls.LOG_DIR}: {e}")
        
        return errors
    
    @classmethod 
    def get_env_config(cls, key, default=None):
        """从环境变量获取配置"""
        return os.environ.get(key, default)