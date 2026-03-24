#!/usr/bin/env python3
"""
简化的配置验证脚本
验证ETL项目配置的基本有效性
"""

import sys
import os
import traceback

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def validate_basic_config():
    """验证基本配置项"""
    try:
        from core.config import Config
        
        print("🔍 验证配置项...")
        errors = Config.validate_config()
        
        if errors:
            print("❌ 配置验证失败：")
            for error in errors:
                print(f"   - {error}")
            return False
        else:
            print("✅ 配置项验证通过")
            return True
            
    except Exception as e:
        print(f"❌ 配置验证异常：{e}")
        return False

def test_imports():
    """测试关键模块导入"""
    try:
        print("🔍 测试模块导入...")
        
        # 测试核心模块
        from core.config import Config
        from core.logger import setup_logger
        from scheduler.job_manager import JobManager
        
        print("✅ 核心模块导入成功")
        return True
        
    except ImportError as e:
        print(f"❌ 模块导入失败：{e}")
        return False
    except Exception as e:
        print(f"❌ 模块导入异常：{e}")
        traceback.print_exc()
        return False

def main():
    """主验证流程"""
    print("🚀 开始ETL项目配置验证...")
    print("=" * 50)
    
    tests = [
        ("基本配置验证", validate_basic_config),
        ("模块导入测试", test_imports),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"📋 {len(results)+1}. {test_name}...")
        result = test_func()
        results.append((test_name, result))
        print()
    
    # 总结结果
    print("=" * 50)
    print("📊 验证结果总结：")
    
    passed = 0
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    success_rate = (passed / len(results)) * 100
    
    if success_rate == 100:
        print("🎉 所有测试通过！ETL项目配置正常。")
        return True
    else:
        print(f"❌ 部分测试失败（{success_rate:.0f}%），请检查配置。")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"💥 验证异常：{e}")
        sys.exit(1)