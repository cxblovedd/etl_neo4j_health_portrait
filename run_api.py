from api import app
from core.db import Neo4jConnection
import logging

if __name__ == '__main__':
    # Initialize Neo4j driver using Singleton
    conn = Neo4jConnection()
    try:
        conn.get_session()
        logging.info("成功连接到Neo4j, API服务将启动...")
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logging.critical(f"!!! Neo4j驱动初始化失败，API无法启动: {e} !!!")
