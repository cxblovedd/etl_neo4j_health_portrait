from neo4j import GraphDatabase
from config.settings import Config
import threading

class Neo4jConnection:
    _instance = None
    _lock = threading.Lock()  # 线程安全锁
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:  # 线程安全检查
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.driver = GraphDatabase.driver(
                        Config.NEO4J_URI,
                        auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD)
                    )
        return cls._instance
    
    def close(self):
        with self._lock:
            if hasattr(self, 'driver') and self.driver:
                self.driver.close()
                self.driver = None
    
    def get_session(self):
        if not hasattr(self, 'driver') or not self.driver:
            raise RuntimeError("Neo4j driver 已被关闭或未初始化")
        return self.driver.session(database=Config.NEO4J_DATABASE)