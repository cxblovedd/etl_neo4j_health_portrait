import os
from pathlib import Path

# CONFIG_DIR 就是 etl_neo4j/config/ 目录的绝对路径
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
# 项目根目录
PROJECT_ROOT = os.path.dirname(CONFIG_DIR)

class Config:
    # 本地Neo4j配置
    # NEO4J_URI = "bolt://localhost:7687"
    # NEO4J_USER = "neo4j"
    # NEO4J_PASSWORD = "86862486"
    # NEO4J_DATABASE = "neo4j"

    # 测试Neo4j配置
    # NEO4J_URI = "bolt://10.55.108.31:7687"
    # NEO4J_USER = "neo4j"
    # NEO4J_PASSWORD = "hayymoni2018"
    # NEO4J_DATABASE = "neo4j"
    
    # 正式Neo4j配置
    NEO4J_URI = "bolt://neo4j.haxm.local:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "Weohgust_2025!"
    NEO4J_DATABASE = "neo4j"
    
    # 大数平台API配置
    # BIGDATA_API_BASE_URL = "http://10.51.28.117:7080" # 测试地址
    BIGDATA_API_BASE_URL = "http://inside.whitelist.com:1115" # 正式地址
    BIGDATA_API_TIMEOUT = 10000
    
    # 调度配置
    BATCH_SIZE = 50              # 批处理大小
    MAX_WORKERS = 1              # 最大并发数（暂时串行，避免死锁）
    RETRY_TIMES = 3              # 重试次数
    RETRY_DELAY = 5              # 重试延迟（秒）
    
    # 超时配置
    CONNECTION_TIMEOUT = 30      # 数据库连接超时
    QUERY_TIMEOUT = 300          # 查询超时（5分钟）
    
    # 日志配置
    LOG_DIR = os.path.join(PROJECT_ROOT, "logs")  # 使用绝对路径
    LOG_LEVEL = "INFO"
    LOG_FILE_ENCODING = "utf-8"  # 日志文件编码
    LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT = 5  # 保留最近5个旧日志文件
    
    
    # 测试PostgreSQL 连接配置（已弃用）
    # PG_HOST = "10.52.200.1"  # 测试地址
    # PG_HOST = "10.52.31.5" # 正式地址
    # PG_PORT = "5432"    # 例如: "5432"
    # PG_DATABASE = "HOSPITAL_HDW" # 例如: "HOSPITAL_HDW" 或包含 ai_patients 的数据库
    # PG_USER = "gpadmin"      # 例如: "gpadmin"
    # PG_PASSWORD = "wn@123"  # 例如: "wn@123"
    # PG_AI_PATIENTS_TABLE = "ai_patients" # ai_patients 表名
    # PG_PATIENT_ID_COLUMN = "patient_id" # ai_patients 表中表示患者ID的列名
    # PG_UPDATE_TIME_COLUMN = "update_time" # 新增: ai_patients 表中表示更新时间的列名
    
    # SQL Server 连接配置
    SQL_HOST = "10.52.8.78"  # SQL Server IP地址
    SQL_PORT = "1433"        # SQL Server 默认端口
    SQL_DATABASE = "health_portrait" # 数据库名
    SQL_USER = "health_portrait_user" # 用户名
    SQL_PASSWORD = "Yiwenbhu_2025!" # 密码
    SQL_AI_PATIENTS_TABLE = "ai_patients" # ai_patients 表名
    SQL_PATIENT_ID_COLUMN = "patient_id" # ai_patients 表中表示患者ID的列名
    SQL_UPDATE_TIME_COLUMN = "update_time" # ai_patients 表中表示更新时间的列名
    
    # ETL时间状态文件路径
    STATE_FILE_PATH = os.path.join(CONFIG_DIR, "etl_state.json")
    
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