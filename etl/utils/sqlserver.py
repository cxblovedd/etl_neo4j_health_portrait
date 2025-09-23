import pyodbc
from config.settings import Config
from etl.utils.logger import setup_logger

logger = setup_logger('sqlserver')

class SQLServerConnection:
    """SQL Server数据库连接类，用于从SQL Server获取患者ID列表"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_connection()
        return cls._instance
    
    def _init_connection(self):
        """初始化SQL Server连接"""
        try:
            # 尝试不同的连接字符串配置
            connection_strings = [
                # 第一种：使用ODBC Driver 11（更旧的驱动，可能更兼容）
                (
                    f"DRIVER={{ODBC Driver 11 for SQL Server}};"
                    f"SERVER={Config.SQL_HOST},{Config.SQL_PORT};"
                    f"DATABASE={Config.SQL_DATABASE};"
                    f"UID={Config.SQL_USER};"
                    f"PWD={Config.SQL_PASSWORD};"
                    f"Encrypt=no;"
                ),
                # 第二种：使用SQL Server Native Client
                (
                    f"DRIVER={{SQL Server Native Client 11.0}};"
                    f"SERVER={Config.SQL_HOST},{Config.SQL_PORT};"
                    f"DATABASE={Config.SQL_DATABASE};"
                    f"UID={Config.SQL_USER};"
                    f"PWD={Config.SQL_PASSWORD};"
                ),
                # 第三种：简单的SQL Server驱动
                (
                    f"DRIVER={{SQL Server}};"
                    f"SERVER={Config.SQL_HOST};"
                    f"DATABASE={Config.SQL_DATABASE};"
                    f"UID={Config.SQL_USER};"
                    f"PWD={Config.SQL_PASSWORD};"
                ),
                # 第四种：使用IP地址直接连接
                (
                    f"DRIVER={{SQL Server}};"
                    f"SERVER={Config.SQL_HOST}\\SQLEXPRESS;"
                    f"DATABASE={Config.SQL_DATABASE};"
                    f"UID={Config.SQL_USER};"
                    f"PWD={Config.SQL_PASSWORD};"
                ),
                # 第五种：原始配置（ODBC Driver 17）
                (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={Config.SQL_HOST},{Config.SQL_PORT};"
                    f"DATABASE={Config.SQL_DATABASE};"
                    f"UID={Config.SQL_USER};"
                    f"PWD={Config.SQL_PASSWORD};"
                    f"TrustServerCertificate=yes;"
                    f"Encrypt=no;"
                )
            ]
            
            last_error = None
            for i, connection_string in enumerate(connection_strings, 1):
                try:
                    driver_name = 'ODBC Driver 11' if 'ODBC Driver 11' in connection_string else \
                                 'SQL Server Native Client' if 'Native Client' in connection_string else \
                                 'ODBC Driver 17' if 'ODBC Driver 17' in connection_string else 'SQL Server'
                    logger.info(f"尝试连接配置 {i}: 使用{driver_name}驱动")
                    self.conn = pyodbc.connect(connection_string, timeout=10)
                    logger.info(f"成功连接到SQL Server数据库: {Config.SQL_DATABASE} (使用配置 {i} - {driver_name})")
                    return
                except pyodbc.Error as e:
                    last_error = e
                    logger.warning(f"连接配置 {i} ({driver_name}) 失败: {str(e)[:200]}...")  # 截断错误信息
                    continue
            
            # 如果所有配置都失败
            logger.error(f"所有连接配置都失败，最后一个错误: {last_error}")
            self.conn = None
            raise last_error
            
        except Exception as e:
            logger.error(f"SQL Server连接错误: {e}")
            self.conn = None
            raise
    
    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("SQL Server连接已关闭")
    
    def load_patient_ids(self, last_update_time=None):
        """
        从SQL Server加载患者ID列表
        
        Args:
            last_update_time: 上次更新时间，如果提供，则只加载该时间之后更新的记录
            
        Returns:
            list: 患者ID列表
        """
        if not self.conn:
            logger.error("数据库连接不可用")
            return []
            
        patient_ids = []
        cursor = None
        
        try:
            cursor = self.conn.cursor()
            
            query = f"SELECT DISTINCT {Config.SQL_PATIENT_ID_COLUMN} FROM {Config.SQL_AI_PATIENTS_TABLE}"
            params = []
            
            if last_update_time:
                query += f" WHERE {Config.SQL_UPDATE_TIME_COLUMN} > ?"
                params.append(last_update_time)
                logger.info(f"加载{last_update_time}之后更新的患者ID")
            else:
                logger.info("加载所有患者ID")
                
            logger.info(f"执行查询: {query}")
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            rows = cursor.fetchall()
            
            for row in rows:
                patient_ids.append(str(row[0]))
                
            logger.info(f"成功从SQL Server加载{len(patient_ids)}个患者ID")
            
        except pyodbc.Error as e:
            logger.error(f"SQL Server查询错误: {e}")
        finally:
            if cursor:
                cursor.close()
                
        return patient_ids