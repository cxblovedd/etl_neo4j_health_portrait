# 测试修复后的代码逻辑
import sys
import os
sys.path.append(os.path.dirname(__file__))

def test_setup_logger():
    print("Testing setup_logger...")
    from core.logger import setup_logger
    
    # 多次调用应该返回同一个logger实例
    logger1 = setup_logger('test')
    logger2 = setup_logger('test')
    
    # 检查handler数量
    assert len(logger1.handlers) == 2, f"Expected 2 handlers, got {len(logger1.handlers)}"
    assert logger1 is logger2, "Should return same logger instance"
    
    print("✅ Logger initialization test passed")

def test_health_portrait_processor():
    """测试HealthPortraitProcessor返回值"""
    from etl.processors.health_portrait import HealthPortraitProcessor
    
    processor = HealthPortraitProcessor()
    
    # 测试空数据情况
    result = processor.process(None)
    assert result is False, f"Expected False for None data, got {result}"
    
    result = processor.process({})
    assert result is False, f"Expected False for empty data, got {result}"
    
    print("✅ HealthPortraitProcessor test passed")

def test_neo4j_connection():
    """测试Neo4j连接的线程安全性"""
    from etl.utils.db import Neo4jConnection
    import threading
    
    connections = []
    
    def create_connection():
        conn = Neo4jConnection()
        connections.append(conn)
    
    # 创建多个线程
    threads = []
    for _ in range(5):
        thread = threading.Thread(target=create_connection)
        threads.append(thread)
        thread.start()
    
    # 等待所有线程完成
    for thread in threads:
        thread.join()
    
    # 检查所有连接是否为同一实例
    first_conn = connections[0]
    for conn in connections[1:]:
        assert conn is first_conn, "All connections should be the same instance"
    
    print("✅ Neo4j connection thread safety test passed")

if __name__ == "__main__":
    print("Running code quality tests...")
    
    try:
        test_logger_initialization()
        test_health_portrait_processor() 
        test_neo4j_connection()
        print("\n🎉 All tests passed! Code quality issues have been fixed.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")