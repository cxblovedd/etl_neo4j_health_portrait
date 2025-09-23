import logging
import os
from datetime import datetime
from config.settings import Config

def setup_logger(name):
    logger = logging.getLogger(name)
    
    # 防止重复初始化
    if logger.handlers:
        return logger
        
    logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # 确保日志目录存在
    os.makedirs(Config.LOG_DIR, exist_ok=True)
    
    # 文件处理器
    log_file = os.path.join(
        Config.LOG_DIR, 
        f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler = logging.FileHandler(
        log_file, 
        encoding=getattr(Config, 'LOG_FILE_ENCODING', 'utf-8')
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger