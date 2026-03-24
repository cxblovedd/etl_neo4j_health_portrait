from flask import jsonify, request, abort
import datetime
import logging
from functools import wraps
from neo4j.exceptions import Neo4jError
from api import app
from core.db import Neo4jConnection

def neo4j_session(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        conn = Neo4jConnection()
        try:
            with conn.get_session() as session:
                return f(session, *args, **kwargs)
        except RuntimeError:
            return jsonify({"error": "数据库连接错误"}), 503
        except Neo4jError as e:
            logging.error(f"Neo4j查询错误: {e}", exc_info=True)
            return jsonify({"error": "数据库查询失败"}), 500
        except Exception as e:
            logging.error(f"API在 {f.__name__} 中出错: {e}", exc_info=True)
            return jsonify({"error": "服务器内部错误"}), 500
    return decorated_function

def calculate_age(birth_date_str):
    if not birth_date_str: return None
    try:
        birth_date = datetime.datetime.strptime(birth_date_str, "%Y-%m-%d").date()
        today = datetime.date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except (TypeError, ValueError, AttributeError):
        return None

def serialize_value(value):
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if hasattr(value, 'properties'):
        return {k: serialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    return value

def serialize_record(record):
    return {key: serialize_value(value) for key, value in record.items()}

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

@app.route('/api/patients/<string:patient_id>/family-graph', methods=['GET'])
@neo4j_session
def get_family_graph(session, patient_id):
    """获取患者家族关系图谱"""
    try:
        depth = int(request.args.get('depth', 2))
        if not 1 <= depth <= 5: raise ValueError()
    except ValueError: return jsonify({"error": "查询参数 'depth' 必须是1到5之间的整数。"}), 400
    pass

@app.route('/api/patients/<string:patient_id>/dashboard', methods=['GET'])
@neo4j_session
def get_patient_dashboard(session, patient_id):
    """获取患者仪表盘概览信息 (已适配)"""
    basic_info_query = """
    MATCH (p:Patient {patientId: $patientId})
    RETURN p.name AS name, p.birthDate AS birthDate, p.gender AS gender
    """
    basic_result = session.execute_read(lambda tx: tx.run(basic_info_query, patientId=patient_id).single())
    if not basic_result:
        abort(404, description="未找到患者")

    latest_condition_query = """
    MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->(e:Encounter)
    WHERE e.visitStartTime IS NOT NULL
    WITH e ORDER BY e.visitStartTime DESC LIMIT 1
    MATCH (e)-[:RECORDED_DIAGNOSIS]->(c:Condition)
    RETURN c.name AS conditionName, e.visitStartTime as date
    LIMIT 5
    """
    conditions = session.execute_read(lambda tx: list(tx.run(latest_condition_query, patientId=patient_id)))

    abnormal_labs_query = """
    MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->()-[:HAD_LAB_TEST]->(ltr:LabTestReport)-[r:HAS_ITEM]->(li:LabTestItem)
    WHERE r.interpretation IS NOT NULL AND r.interpretation <> '正常'
    WITH r, li ORDER BY r.timestamp DESC LIMIT 5
    RETURN r.timestamp AS timestamp, li.name as testName, r.value as value, r.unit as unit, r.interpretation as interpretation
    """
    abnormal_labs = session.execute_read(lambda tx: list(tx.run(abnormal_labs_query, patientId=patient_id)))

    dashboard_data = {
        "patientId": patient_id,
        "name": basic_result["name"],
        "age": calculate_age(basic_result["birthDate"]),
        "gender": basic_result["gender"],
        "keyConditions": [serialize_record(r) for r in conditions],
        "recentAbnormalIndicators": [serialize_record(r) for r in abnormal_labs],
        "recentAbnormalIndicatorCount": len(abnormal_labs)
    }
    return jsonify(dashboard_data)

@app.route('/api/patients/<string:patient_id>/encounters', methods=['GET'])
@neo4j_session
def get_encounters(session, patient_id):
    """获取患者就诊记录列表 (已适配)"""
    try:
        page = int(request.args.get('page', 1)); limit = int(request.args.get('limit', 10))
        skip = (page - 1) * limit
    except ValueError: return jsonify({"error": "无效的分页参数"}), 400
    
    query_data = """
    MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->(e:Encounter)
    OPTIONAL MATCH (e)-[:AT_HOSPITAL]->(h:Hospital)
    OPTIONAL MATCH (e)-[:IN_DEPARTMENT]->(d:Department)
    OPTIONAL MATCH (e)-[:RECORDED_DIAGNOSIS]->(c:Condition)
    WITH p, e, h, d, collect(c.name) AS diagnoses
    ORDER BY e.visitStartTime DESC
    SKIP $skip LIMIT $limit
    RETURN
        e.encounterId AS encounterId,
        e.typeName AS encounterType,
        e.visitStartTime AS encounterDate,
        h.name AS hospitalName,
        d.name AS departmentName,
        diagnoses
    """
    query_count = "MATCH (p:Patient {patientId: $patientId})-[:HAD_ENCOUNTER]->(e:Encounter) RETURN count(e) AS totalCount"
    
    results = session.execute_read(lambda tx: list(tx.run(query_data, patientId=patient_id, skip=skip, limit=limit)))
    count_result = session.execute_read(lambda tx: tx.run(query_count, patientId=patient_id).single())
    total_count = count_result['totalCount'] if count_result else 0

    return jsonify({
        "currentPage": page, "pageSize": limit, "totalCount": total_count,
        "totalPages": (total_count + limit - 1) // limit,
        "encounters": [serialize_record(r) for r in results]
    })
    
@app.route('/api/patients/<string:patient_id>/history/medical', methods=['GET'])
@neo4j_session
def get_medical_history(session, patient_id):
    """获取患者既往医疗史事件列表 (已适配)"""
    query = """
    MATCH (p:Patient {patientId: $patientId})-[]->(e:PastMedicalEvent)
    RETURN 
        [lbl IN labels(e) WHERE lbl <> 'PastMedicalEvent'][0] AS type,
        e.name AS description,
        e.date AS date
    ORDER BY date DESC
    """
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/history/personal', methods=['GET'])
@neo4j_session
def get_personal_history(session, patient_id):
    """获取患者个人史条目列表 (已适配)"""
    query = "MATCH (p:Patient {patientId: $patientId})-[:HAS_LIFESTYLE_FACT]->(lf:LifestyleFact) RETURN lf"
    results = session.execute_read(lambda tx: [r['lf'] for r in tx.run(query, patientId=patient_id)])
    return jsonify([serialize_value(r) for r in results])

@app.route('/api/patients/<string:patient_id>/history/family', methods=['GET'])
@neo4j_session
def get_family_history(session, patient_id):
    """获取患者家族史条目列表 (已适配)"""
    query = """
    MATCH (p:Patient {patientId: $patientId})-[r:HAS_FAMILY_HISTORY]->(c:Condition)
    RETURN c.name as conditionName, r.relationship as relative, r.onsetAge as onsetAge, r.recordedAt as recordedDate
    ORDER BY relative, conditionName
    """
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])
    
@app.route('/api/patients/<string:patient_id>/allergies', methods=['GET'])
@neo4j_session
def get_allergies(session, patient_id):
    """获取患者过敏史列表 (已适配)"""
    query = """
    MATCH (p:Patient {patientId: $patientId})-[r:HAS_ALLERGY_TO]->(a:Allergen)
    RETURN a.name AS allergen, r.reaction as reaction, r.severity as severity, r.recordedAt as recordedDate
    ORDER BY allergen
    """
    results = session.execute_read(lambda tx: list(tx.run(query, patientId=patient_id)))
    return jsonify([serialize_record(r) for r in results])

@app.route('/api/patients/<string:patient_id>/marital_info', methods=['GET'])
@neo4j_session
def get_marital_info(session, patient_id):
    """获取患者婚育史信息 (已适配)"""
    query = "MATCH (p:Patient {patientId: $patientId}) RETURN p.maritalStatus as status"
    result = session.execute_read(lambda tx: tx.run(query, patientId=patient_id).single())
    return jsonify(serialize_record(result)) if result else jsonify({})

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": getattr(error, 'description', '未找到资源')}), 404
