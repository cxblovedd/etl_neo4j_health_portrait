#!/usr/bin/env python3
"""
SQL Server连接诊断脚本
用于测试不同的连接方式和配置
"""

import pyodbc
import sys
from config.settings import Config

def test_connection():
    """测试SQL Server连接"""
    
    print("=== SQL Server连接诊断 ===")
    print(f"目标服务器: {Config.SQL_HOST}:{Config.SQL_PORT}")
    print(f"数据库: {Config.SQL_DATABASE}")
    print(f"用户名: {Config.SQL_USER}")
    print()
    
    # 测试不同的连接配置
    test_configs = [
        {
            "name": "SQL Server - 不指定数据库（测试连接）",
            "connection_string": (
                f"DRIVER={{SQL Server}};"
                f"SERVER={Config.SQL_HOST};"
                f"UID={Config.SQL_USER};"
                f"PWD={Config.SQL_PASSWORD};"
            )
        },
        {
            "name": "SQL Server - 连接到master数据库",
            "connection_string": (
                f"DRIVER={{SQL Server}};"
                f"SERVER={Config.SQL_HOST};"
                f"DATABASE=master;"
                f"UID={Config.SQL_USER};"
                f"PWD={Config.SQL_PASSWORD};"
            )
        },
        {
            "name": "ODBC Driver 11 - 连接到master",
            "connection_string": (
                f"DRIVER={{ODBC Driver 11 for SQL Server}};"
                f"SERVER={Config.SQL_HOST};"
                f"DATABASE=master;"
                f"UID={Config.SQL_USER};"
                f"PWD={Config.SQL_PASSWORD};"
                f"Encrypt=no;"
            )
        },
        {
            "name": "ODBC Driver 11 - 禁用加密",
            "connection_string": (
                f"DRIVER={{ODBC Driver 11 for SQL Server}};"
                f"SERVER={Config.SQL_HOST};"
                f"DATABASE={Config.SQL_DATABASE};"
                f"UID={Config.SQL_USER};"
                f"PWD={Config.SQL_PASSWORD};"
                f"Encrypt=no;"
            )
        },
        {
            "name": "SQL Server Native Client 11.0",
            "connection_string": (
                f"DRIVER={{SQL Server Native Client 11.0}};"
                f"SERVER={Config.SQL_HOST};"
                f"DATABASE={Config.SQL_DATABASE};"
                f"UID={Config.SQL_USER};"
                f"PWD={Config.SQL_PASSWORD};"
            )
        }
    ]
    
    success_count = 0
    
    for i, config in enumerate(test_configs, 1):
        print(f"\n测试配置 {i}: {config['name']}")
        print("-" * 50)
        
        try:
            print("正在连接...")
            conn = pyodbc.connect(config['connection_string'], timeout=5)
            
            # 测试查询
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            
            print("✅ 连接成功!")
            print(f"服务器版本: {version[:100]}...")
            
            # 尝试查询目标表
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {Config.SQL_AI_PATIENTS_TABLE}")
                count = cursor.fetchone()[0]
                print(f"✅ 表 {Config.SQL_AI_PATIENTS_TABLE} 存在，包含 {count} 条记录")
            except Exception as e:
                print(f"⚠️ 无法访问表 {Config.SQL_AI_PATIENTS_TABLE}: {e}")
            
            cursor.close()
            conn.close()
            success_count += 1
            break  # 找到可用配置就停止
            
        except pyodbc.Error as e:
            error_msg = str(e)
            if len(error_msg) > 200:
                error_msg = error_msg[:200] + "..."
            print(f"❌ 连接失败: {error_msg}")
        except Exception as e:
            print(f"❌ 未知错误: {e}")
    
    print("\n" + "=" * 60)
    if success_count > 0:
        print("✅ 找到可用的连接配置!")
    else:
        print("❌ 所有连接配置都失败了")
        print("\n可能的解决方案:")
        print("1. 检查SQL Server是否启用了TCP/IP协议")
        print("2. 确认防火墙设置允许1433端口")
        print("3. 验证用户名和密码是否正确")
        print("4. 确认SQL Server实例名称")
        print("5. 检查SQL Server是否支持远程连接")

if __name__ == "__main__":
    test_connection()