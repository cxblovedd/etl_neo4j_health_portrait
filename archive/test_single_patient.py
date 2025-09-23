# test_single_patient.py

import json
from config import settings
# 【关键修正】导入 Neo4jConnection 类，而不是一个不存在的函数
from etl.utils.db import Neo4jConnection 
from etl.utils.logger import setup_logger
from etl.core.etl_patient import import_patient_data_from_json

# --- 配置区 ---
logger = setup_logger('single_test')
JSON_FILE_PATH = 'files/test_patient.json'

# --- 辅助函数 ---
def clear_database_tx(tx):
    """一个在事务中执行的函数，用于清空数据库。"""
    logger.info("正在清空数据库中的所有节点和关系...")
    tx.run("MATCH (n) DETACH DELETE n")
    logger.info("数据库已清空。")

# --- 主测试逻辑 ---
def run_test():
    """执行单文件ETL测试的主函数。"""
    logger.info("--- 开始对 etl_patient.py 进行单文件测试 ---")
    
    # 1. 从文件中加载JSON数据
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            patient_json_data = json.load(f) 
        logger.info(f"成功从 '{JSON_FILE_PATH}' 文件加载数据。")
    except FileNotFoundError:
        logger.error(f"测试失败：找不到测试文件 '{JSON_FILE_PATH}'。请检查文件名和路径。")
        return
    except json.JSONDecodeError as e:
        logger.error(f"测试失败：解析JSON文件时出错。错误：{e}")
        return

    db_connection = None
    try:
        # 2. 【关键修正】创建 Neo4jConnection 类的实例
        db_connection = Neo4jConnection()
        logger.info("成功创建Neo4j数据库连接实例。")

        # 3. (强烈建议) 运行前清空数据库
        # 【关键修正】通过连接实例获取会话
        # with db_connection.get_session() as session:
        #     session.execute_write(clear_database_tx)

        # 4. 【核心测试步骤】在数据库事务中调用ETL核心函数
        logger.info(f"正在为患者 {patient_json_data.get('data', {}).get('patientId')} 执行ETL逻辑...")
        # 【关键修正】再次通过连接实例获取会话
        with db_connection.get_session() as session:
            session.execute_write(import_patient_data_from_json, patient_json_data)
        
        logger.info("--- 单文件ETL测试成功完成！ ---")
        logger.info("现在可以打开Neo4j Browser，使用下面的Cypher查询来验证数据是否已正确导入：")
        logger.info("MATCH (p:Patient {patientId: 52183})-[r]->(n) RETURN p, r, n")

    except Exception as e:
        logger.error(f"测试过程中发生严重错误: {e}", exc_info=True)
    finally:
        # 5. 【关键修正】通过连接实例关闭连接
        if db_connection:
            db_connection.close()
            logger.info("数据库连接已关闭。")


if __name__ == '__main__':
    run_test()