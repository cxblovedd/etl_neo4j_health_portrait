# app.py

import os
import json
from flask import Flask, jsonify, request, abort, render_template
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import datetime
import logging
from functools import wraps

# --- Configuration ---
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j.haxm.local:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "Weohgust_2025!") # 假设这是您正确的密码
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

# --- Flask App Initialization ---
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Driver Initialization ---
driver = None
try:
    if NEO4J_PASSWORD == "your_password":
         logging.warning("Using default Neo4j password 'your_password'. Please change or set the NEO4J_PASSWORD environment variable.")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    logging.info(f"Successfully connected to Neo4j at {NEO4J_URI} on startup.")
except Exception as e:
    logging.error(f"Fatal: Failed to connect to Neo4j on startup: {e}")
    driver = None

# --- Decorator for Neo4j Session Handling ---
def neo4j_session(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not driver:
            logging.error("Neo4j driver not available.")
            return jsonify({"error": "Database connection error"}), 503
        with driver.session(database=NEO4J_DATABASE) as session:
            try:
                return f(session, *args, **kwargs)
            except Neo4jError as e:
                logging.error(f"Neo4j query error: {e}", exc_info=True)
                if "ConstraintValidationFailed" in str(e):
                     return jsonify({"error": "Data conflict or constraint violation"}), 409
                return jsonify({"error": "Database query failed"}), 500
            except Exception as e:
                logging.error(f"API error in {f.__name__}: {e}", exc_info=True)
                return jsonify({"error": "An internal server error occurred"}), 500
    return decorated_function

# --- Helper Functions ---
def calculate_age(birth_date):
    if not birth_date: return None
    today = datetime.date.today()
    if isinstance(birth_date, datetime.datetime): birth_date = birth_date.date()
    if hasattr(birth_date, 'to_native'): birth_date = birth_date.to_native()
    try:
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except (TypeError, AttributeError):
        logging.warning(f"Could not calculate age from invalid birth_date type: {type(birth_date)}")
        return None

def serialize_value(value):
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)): return value.isoformat()
    if hasattr(value, 'isoformat'):
         try: return value.isoformat()
         except Exception: return str(value)
    if hasattr(value, 'properties'): return {k: serialize_value(v) for k, v in value.items()}
    if isinstance(value, list): return [serialize_value(item) for item in value]
    if isinstance(value, dict): return {k: serialize_value(v) for k, v in value.items()}
    return value

def serialize_record(record):
    return {key: serialize_value(value) for key, value in record.items()}

# ---
# 图谱展示页面路由 (带连接测试)
# ---
@app.route('/graph/patient/<string:patient_id>')
def show_patient_graph_wrapper(patient_id):
    """
    这是一个包装函数，用于在渲染前进行连接性测试。
    """
    logging.info("--- [DEBUG] 收到 /graph/patient 的请求 ---")
    
    # 【新增的连接测试】
    test_driver = None
    try:
        logging.info(f"--- [DEBUG] 准备使用以下信息测试连接: URI={NEO4J_URI}, User={NEO4J_USER}")
        test_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        test_driver.verify_connectivity()
        logging.info("--- [DEBUG] 连接性验证成功！后端配置无误。 ---")
    except Neo4jError as e:
        # 如果这里报错，说明就是Python后端的配置问题
        logging.error(f"--- [DEBUG] 后端连接测试失败！错误类型: {type(e)}, 错误信息: {e} ---", exc_info=True)
        return f"<h1>后端数据库连接失败</h1><p>错误信息: {e}</p><p>请检查app.py中的NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD配置。</p>", 500
    finally:
        if test_driver:
            test_driver.close()

    # 如果测试通过，再调用原来的逻辑来渲染页面
    # 为了避免与装饰器冲突，我们调用一个内部函数
    return show_patient_graph_internal(patient_id)


@neo4j_session
def show_patient_graph_internal(session, patient_id):
    """实际渲染页面的函数"""
    patient_name = "未知"
    result = session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $id}) RETURN p.name AS name", id=patient_id).single())
    if result and result["name"]:
        patient_name = result["name"]

    logging.info(f"--- [DEBUG] 准备渲染模板，传递的密码是: {'*' * len(NEO4J_PASSWORD)} ---")
    return render_template(
        'graph_display.html',
        patient_id=patient_id,
        patient_name=patient_name,
        neo4j_uri=NEO4J_URI,
        neo4j_user=NEO4J_USER,
        neo4j_password=NEO4J_PASSWORD
    )


# --- API Endpoints ---
@app.route('/api/docs')
def api_docs():
    """API接口清单"""
    api_routes = []
    for rule in app.url_map.iter_rules():
        try:
            if rule.rule.startswith('/api/') and rule.endpoint != 'static' and rule.rule != '/api/docs':
                if rule.endpoint in app.view_functions:
                    view_func = app.view_functions[rule.endpoint]
                    description = view_func.__doc__ or ''
                    if description:
                        description = description.strip().split('\n')[0]
                    api_routes.append({
                        'name': description,
                        'url': rule.rule,
                        'methods': ', '.join(rule.methods - {'HEAD', 'OPTIONS'})
                    })
        except Exception as e:
            logging.warning(f"无法为规则 {rule.endpoint} 生成文档: {e}")
    api_routes.sort(key=lambda x: x['url'])
    table_rows = ""
    for route in api_routes:
        table_rows += f"<tr><td>{route['name']}</td><td>{route['url']}</td><td>{route['methods']}</td></tr>"
    html = f"""
    <html><head><title>API接口清单</title><style>body {{ font-family: Arial, sans-serif; margin: 40px; }} h1 {{ color: #333; }} table {{ border-collapse: collapse; width: 100%; }} th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }} th {{ background-color: #f5f5f5; }}</style></head>
    <body><h1>API接口清单</h1><table><tr><th>接口名称</th><th>接口地址</th><th>请求方法</th></tr>{table_rows}</table></body></html>
    """
    return html


@app.route('/api/patients/<string:patient_id>/dashboard', methods=['GET'])
@neo4j_session
def get_patient_dashboard(session, patient_id):
    """获取患者仪表盘概览信息"""
    query_basic_vitals = "MATCH (p:Patient {patientId: $patientId}) OPTIONAL MATCH (p)-[:HAS_VITALSIGN_RECORD]->(vs:VitalSign) WITH p, vs ORDER BY vs.timestamp DESC WITH p, collect(vs) AS vs_list RETURN p.name AS name, p.birthDate AS birthDate, vs_list[0] AS latest_vs, head([v IN vs_list WHERE v.type = 'BloodPressure' | v]) AS latest_bp, p.weight AS latest_weight, p.height AS latest_height"
    query_conditions = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_CHRONIC_CONDITION]->(c:Condition) WHERE r.status = 'Active' RETURN c.name AS conditionName, c.type AS conditionType ORDER BY c.type DESC, c.name"
    query_abnormal_labs = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->()-[:RECORDED_LAB_RESULT]->(lr:LabResult) WITH lr ORDER BY lr.timestamp DESC LIMIT 5 MATCH (lr)-[:RESULT_OF]->(lt:LabTest) RETURN lr.timestamp AS timestamp, lt.name AS testName, lr.value AS value, lr.unit AS unit, lr.interpretation AS interpretation ORDER BY timestamp DESC"
    query_abnormal_count = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->()-[:RECORDED_LAB_RESULT]->(lr:LabResult) RETURN count(lr) AS abnormalCount"
    result_basic = session.execute_read(lambda tx: tx.run(query_basic_vitals, patientId=patient_id).single())
    if not result_basic or not result_basic["name"]:
        if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
            abort(404, description="Patient not found")
        else:
             logging.error(f"Dashboard query returned unexpected nulls for patient {patient_id}")
             abort(500, description="Failed to retrieve complete dashboard data")
    result_conditions = session.execute_read(lambda tx: list(tx.run(query_conditions, patientId=patient_id)))
    result_abnormal_labs = session.execute_read(lambda tx: list(tx.run(query_abnormal_labs, patientId=patient_id)))
    result_abnormal_count = session.execute_read(lambda tx: tx.run(query_abnormal_count, patientId=patient_id).single())
    dashboard_data = {
        "patientId": patient_id, "name": result_basic["name"], "latest_weight":result_basic["latest_weight"],
        "latest_height":result_basic["latest_height"], "age": calculate_age(result_basic["birthDate"]), "latestVitals": {},
        "keyConditions": [serialize_record(r) for r in result_conditions],
        "recentAbnormalIndicators": [serialize_record(r) for r in result_abnormal_labs],
        "recentAbnormalIndicatorCount": result_abnormal_count["abnormalCount"] if result_abnormal_count else 0
    }
    latest_bp_node = result_basic.get('latest_bp')
    if latest_bp_node:
         dashboard_data["latestVitals"]["bloodPressure"] = {k: serialize_value(v) for k, v in latest_bp_node.items()}
    return jsonify(dashboard_data)

# ... 省略其他API路由以保持简洁，您的文件中应保留它们 ...
@app.route('/api/patients/<string:patient_id>/findings', methods=['GET'])
@neo4j_session
def get_findings(session, patient_id):
    """获取患者病症/发现列表"""
    query = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->()-[:PERFORMED]->(ex:Examination) MATCH (ex)-[:REVEALED]->(ef:ExaminationFinding) MATCH (ef)-[:SUGGESTS_CONDITION]->(c:Condition) RETURN ex.reportId AS reportid,ef.finding AS finding, ef.details AS details, ef.locationName AS location,c.icdCode AS icdcode ,c.name AS suggestname,c.type AS suggesttype ORDER BY reportid DESC"
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    if not results and not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/bodypart/<string:part_name>/conditions', methods=['GET'])
@neo4j_session
def get_bodypart_conditions(session, patient_id, part_name):
    """获取与特定身体部位（含子部位）相关联的患者病症/发现"""
    query = "MATCH (p:Patient {patientId: $patientId}) OPTIONAL MATCH (rootPart:BodyPart {name: $partName}) WHERE rootPart IS NOT NULL CALL { WITH rootPart OPTIONAL MATCH (rootPart)<-[:LOCATED_IN*0..]-(affectedPart:BodyPart) RETURN collect(DISTINCT coalesce(affectedPart, rootPart)) AS relevantParts } MATCH (p)-[:HAD_ENCOUNTER]->()-[:PERFORMED]->(ex:Examination)-[:REVEALED]->(ef:ExaminationFinding)-[:LOCATED_IN]->(bp:BodyPart) WHERE bp IN relevantParts MATCH (ef)-[:SUGGESTS_CONDITION]->(c:Condition) RETURN DISTINCT c.name AS conditionName, c.type AS conditionType, ef.finding AS findingDescription, ef.details AS findingDetails, bp.name AS specificLocation, ex.type AS examType, ex.timestamp AS examTimestamp ORDER BY examTimestamp DESC"
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id, partName=part_name)))
    if not results and not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/labtest/<string:test_code>/history', methods=['GET'])
@neo4j_session
def get_labtest_history(session, patient_id, test_code):
    """获取特定检验项目的历史结果"""
    query = "MATCH (lt:LabTest {code: $testCode}) MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->()-[:RECORDED_LAB_RESULT]->(lr:LabResult)-[:RESULT_OF]->(lt) RETURN lr.labTestName AS labTestName,lr.timestamp AS timestamp, lr.value AS value, lr.unit AS unit, lr.interpretation AS interpretation, lr.referenceRange as referenceRange ORDER BY lr.timestamp DESC"
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id, testCode=test_code)))
    if not results and not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/encounters', methods=['GET'])
@neo4j_session
def get_encounters(session, patient_id):
    """获取患者就诊记录列表（分页）"""
    try:
        page = int(request.args.get('page', 1)); limit = int(request.args.get('limit', 10))
        if page < 1: page = 1
        if limit < 1: limit = 10
        skip = (page - 1) * limit
    except ValueError: return jsonify({"error": "Invalid page or limit parameter"}), 400
    query_data = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->(e:Encounter)-[:RECORDED_DIAGNOSIS]->(c:Condition) RETURN e.encounterId AS encounterId, e.type AS type, e.encounterDate AS encounterDate, e.department as department, c.sourceRecordId as sourceRecordId,c.icdCode as icdCode,c.name as conditionName ORDER BY e.encounterDate DESC SKIP $skip LIMIT $limit"
    query_count = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->(e:Encounter) RETURN count(e) AS totalCount"
    results = session.execute_read(lambda tx: list(tx.run(query_data, patientId=patient_id, skip=skip, limit=limit)))
    count_result = session.execute_read(lambda tx: tx.run(query_count, patientId=patient_id).single())
    total_count = count_result['totalCount'] if count_result else 0
    if total_count == 0 and not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    return jsonify({
        "currentPage": page, "pageSize": limit, "totalCount": total_count,
        "totalPages": (total_count + limit - 1) // limit if limit > 0 else 0,
        "encounters": [serialize_record(r) for r in results]
    })

@app.route('/api/patients/<string:patient_id>/history/medical', methods=['GET'])
@neo4j_session
def get_medical_history(session, patient_id):
    """获取患者既往医疗史事件列表"""
    query = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_HISTORY_EVENT]->(mh:MedicalHistoryEvent) RETURN mh.type as type, mh.description as description, mh.date as date, r.sourceRecordId as sourceRecordId ORDER BY mh.date DESC"
    if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/history/personal', methods=['GET'])
@neo4j_session
def get_personal_history(session, patient_id):
    """获取患者个人史条目列表 (包括来源信息)"""
    query = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_PERSONAL_HISTORY]->(ph:PersonalHistoryItem) RETURN ph.type as type, r.status as status, r.details as details, r.sourceRecordId as sourceRecordId, r.recordedDate as recordedDate, r.updatedAt as lastUpdatedAt ORDER BY ph.type, r.recordedDate DESC"
    if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/history/family', methods=['GET'])
@neo4j_session
def get_family_history(session, patient_id):
    """获取患者家族史条目列表 (包括来源信息)"""
    query = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_FAMILY_HISTORY]->(fh:FamilyHistory) RETURN fh.relative as relative, fh.conditionName as conditionName, r.details as details, r.sourceRecordId as sourceRecordId, r.recordedDate as recordedDate, r.updatedAt as lastUpdatedAt ORDER BY fh.relative, fh.conditionName, r.recordedDate DESC"
    if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/allergies', methods=['GET'])
@neo4j_session
def get_allergies(session, patient_id):
    """获取患者过敏史列表 (包括来源信息)"""
    query = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_ALLERGY]->(a:Allergy) RETURN a.allergen as allergen, r.reaction as reaction, r.severity as severity, r.sourceRecordId as sourceRecordId, r.recordedDate as recordedDate, r.updatedAt as lastUpdatedAt ORDER BY a.allergen, r.recordedDate DESC"
    if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/marital_info', methods=['GET'])
@neo4j_session
def get_marital_info(session, patient_id):
    """获取患者婚育史信息"""
    query = "MATCH (p:Patient {patientId: $patientId})-[r:HAS_MARITAL_INFO]->(ms:MaritalStatus) RETURN ms.status as status, ms.marriageCount as marriageCount, ms.childrenCount as childrenCount, r.sourceRecordId as sourceRecordId, r.recordedDate as recordedDate, r.updatedAt as lastUpdatedAt LIMIT 1"
    if not session.execute_read(lambda tx: tx.run("MATCH (p:Patient {patientId: $patientId}) RETURN p LIMIT 1", patientId=patient_id).single()):
        abort(404, description="Patient not found")
    result = session.execute_read(lambda tx: tx.run(query, patientId=patient_id).single())
    if not result:
        return jsonify({})
    return jsonify(serialize_record(result))


# --- Error Handlers ---
@app.errorhandler(404)
def not_found(error):
    message = getattr(error, 'description', 'Resource not found')
    return jsonify({"error": message}), 404

@app.errorhandler(500)
def internal_error(error):
    logging.exception("An internal server error occurred")
    return jsonify({"error": "Internal Server Error"}), 500

@app.errorhandler(503)
def service_unavailable(error):
    message = getattr(error, 'description', 'Service Unavailable')
    return jsonify({"error": message}), 503


# --- Main Execution ---
if __name__ == '__main__':
    if not driver:
        logging.critical("!!! Neo4j driver failed to initialize. API cannot start. !!!")
    else:
        is_debug = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
        app.run(host='0.0.0.0', port=5000, debug=is_debug)
