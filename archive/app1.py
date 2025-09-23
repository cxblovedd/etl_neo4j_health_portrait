# app.py

import os
from flask import Flask, jsonify, request
from neo4j import GraphDatabase, basic_auth
from config.settings import Config

# --- 1. 应用和数据库配置 ---

app = Flask(__name__)

# 使用从Config类加载的配置
NEO4J_URI = Config.NEO4J_URI
NEO4J_USER = Config.NEO4J_USER
NEO4J_PASSWORD = Config.NEO4J_PASSWORD
NEO4J_DATABASE = Config.NEO4J_DATABASE  # 获取数据库名称

# 全局的Neo4j驱动实例，由应用共享，提高效率
driver = None
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    print("成功连接到Neo4j数据库。")
except Exception as e:
    print(f"连接Neo4j数据库失败: {e}")


# --- 2. API 端点定义 ---

@app.route('/api/v1/patients/<string:patient_id>/family-graph', methods=['GET'])
def get_family_graph(patient_id):
    """
    提供家族关系图谱数据的API端点。
    支持 `depth` 查询参数来控制图谱的广度。
    """
    if not driver:
        return jsonify({"error": "数据库连接不可用。"}), 503

    # 从URL查询参数中获取深度，提供默认值和范围限制
    try:
        depth = int(request.args.get('depth', 2))
        if not 1 <= depth <= 5: # 限制深度，防止查询过大导致性能问题
            raise ValueError()
    except ValueError:
        return jsonify({"error": "查询参数 'depth' 必须是1到5之间的整数。"}), 400

    try:
        # 调用核心函数来获取并格式化图谱数据
        graph_data = fetch_and_format_graph_data(driver, patient_id, depth)
        
        # 如果找不到起始患者，返回404
        if not any(node['id'] == patient_id for node in graph_data['nodes']):
            return jsonify({"error": f"未找到ID为 '{patient_id}' 的患者。"}), 404
            
        return jsonify({
            "code": 0,
            "msg": "成功",
            "data": graph_data
        })
    except Exception as e:
        app.logger.error(f"为患者 {patient_id} 获取图谱时发生错误: {e}")
        return jsonify({"error": "服务器内部错误。"}), 500

# --- 3. 数据查询与格式化核心逻辑 ---

def fetch_and_format_graph_data(db_driver, patient_id, depth):
    """
    执行 Cypher 查询并格式化数据为前端需要的 "nodes" 和 "edges" 结构。
    优化：避免 edges 中出现 source 或 target 为 null 的情况。
    """
    nodes = {}
    edges = {}

    with db_driver.session(database=NEO4J_DATABASE) as session:
        query = """
            MATCH path = (p:Patient {patientId: $patientId})-[_:PARENT_OF|SPOUSE_OF|HAS_REPORTED_RELATIONSHIP*..%d]-(relative:Patient)
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, startNode(r) AS startNode, endNode(r) AS endNode
        """ % depth

        results = session.run(query, patientId=patient_id)

        for record in results:
            edge_record = record['r']
            start_node = record['startNode']
            end_node = record['endNode']

            start_node_id = start_node.get('patientId')
            end_node_id = end_node.get('patientId')
            if not start_node_id or not end_node_id:
                continue

            # 添加起点
            if start_node_id not in nodes:
                nodes[start_node_id] = {
                    "id": start_node_id,
                    "label": start_node.get('name', f"患者 {start_node_id}"),
                    "type": 'MainPatient' if start_node_id == patient_id else 'Relative',
                    "properties": {
                        "gender": start_node.get('gender'),
                        "birthDate": start_node.get('birthDate'),
                        "idType": start_node.get('idType'),
                        "idValue": start_node.get('idValue')
                    }
                }

            # 添加终点
            if end_node_id not in nodes:
                nodes[end_node_id] = {
                    "id": end_node_id,
                    "label": end_node.get('name', f"患者 {end_node_id}"),
                    "type": 'MainPatient' if end_node_id == patient_id else 'Relative',
                    "properties": {
                        "gender": end_node.get('gender'),
                        "birthDate": end_node.get('birthDate'),
                        "idType": end_node.get('idType'),
                        "idValue": end_node.get('idValue')
                    }
                }

            # 添加边
            edge_id = edge_record.element_id
            if edge_id not in edges:
                edge_type = type(edge_record).__name__
                style_type = 'PRECISE' if edge_type in ['PARENT_OF', 'SPOUSE_OF'] else 'REPORTED'

                edges[edge_id] = {
                    "id": edge_id,
                    "source": start_node_id,
                    "target": end_node_id,
                    "label": edge_record.get('relationshipName', edge_record.get('type', edge_type)),
                    "type": style_type
                }

    return {
        "nodes": list(nodes.values()),
        "edges": list(edges.values())
    }


# --- 4. 应用启动入口 ---

@app.route('/')
def index():
    return "家族图谱API服务正在运行。请使用 /api/v1/patients/&lt;patientId&gt;/family-graph 端点获取数据。"

if __name__ == '__main__':
    # 使用`flask run`命令或在生产环境中使用Gunicorn/uWSGI来运行
    # 这里为了方便直接演示，使用app.run()
    # host='0.0.0.0' 让服务可以被外部访问
    app.run(host='0.0.0.0', port=5000, debug=True)