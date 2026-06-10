#!/usr/bin/env python3
import sys
import os
import json

# Add project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config import Config
from core.db import Neo4jConnection
from etl.extractors.sqlserver import SQLServerConnection

def audit():
    print("🚀 开始比对 SQL Server 和 Neo4j 中的患者数据...")
    print("=" * 60)
    
    # 1. 从 SQL Server 加载患者 ID
    sql_patient_ids = set()
    sql_conn = None
    try:
        sql_conn = SQLServerConnection()
        ids = sql_conn.load_patient_ids() # 加载全部
        sql_patient_ids = set(ids)
        print(f"漏 SQL Server (源表) 中患者总数: {len(sql_patient_ids)}")
    except Exception as e:
        print(f"❌ 读取 SQL Server 发生错误: {e}")
        return
    finally:
        if sql_conn:
            sql_conn.close()

    # 2. 从 Neo4j 加载已同步的患者 ID
    neo4j_patient_ids = set()
    patients_in_neo4j = {}
    neo4j_conn = None
    try:
        neo4j_conn = Neo4jConnection()
        with neo4j_conn.get_session() as session:
            query = "MATCH (p:Patient) RETURN p.patientId AS patientId, p.name AS name"
            result = session.run(query)
            for record in result:
                pid = record["patientId"]
                name = record["name"]
                if pid:
                    patients_in_neo4j[str(pid)] = name
            neo4j_patient_ids = set(patients_in_neo4j.keys())
        print(f"漏 Neo4j (图数据库) 中已存在的患者总数: {len(neo4j_patient_ids)}")
    except Exception as e:
        print(f"❌ 读取 Neo4j 发生错误: {e}")
        return
    finally:
        if neo4j_conn:
            neo4j_conn.close()

    # 3. 数据差异比对
    synced_ids = sql_patient_ids & neo4j_patient_ids
    unsynced_ids = sql_patient_ids - neo4j_patient_ids
    orphan_ids = neo4j_patient_ids - sql_patient_ids

    print("=" * 60)
    print("📊 比对结果分析：")
    print(f"   ✅ 已同步成功 (两边均有): {len(synced_ids)}")
    print(f"   ⏳ 待同步/同步失败 (SQL有，Neo4j无): {len(unsynced_ids)}")
    print(f"   ⚠️ 孤立患者 (Neo4j有，SQL无): {len(orphan_ids)}")
    print("=" * 60)

    # 打印详细列表
    if unsynced_ids:
        print(f"\n⏳ 待同步/同步失败的患者 ID 列表 (前50个):")
        print(sorted(list(unsynced_ids))[:50])
    
    if orphan_ids:
        print(f"\n⚠️ 仅在 Neo4j 中存在的患者 ID 列表 (前50个，可能为家属节点合并产生或手动录入):")
        orphan_list = []
        for pid in sorted(list(orphan_ids))[:50]:
            name = patients_in_neo4j.get(pid, "未知")
            orphan_list.append(f"{pid}({name})")
        print(orphan_list)

    # 4. 保存报告
    report = {
        "summary": {
            "sql_server_total": len(sql_patient_ids),
            "neo4j_total": len(neo4j_patient_ids),
            "synced_count": len(synced_ids),
            "unsynced_count": len(unsynced_ids),
            "orphan_count": len(orphan_ids)
        },
        "unsynced_patient_ids": sorted(list(unsynced_ids)),
        "orphan_patients": [{"patientId": pid, "name": patients_in_neo4j.get(pid, "未知")} for pid in sorted(list(orphan_ids))]
    }
    
    report_dir = os.path.join(Config.PROJECT_ROOT, "data", "state")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "audit_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n💾 详细对比报告已保存至: {report_path}")

if __name__ == "__main__":
    audit()
