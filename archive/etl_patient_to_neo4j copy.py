import json
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import datetime
import logging

# --- Configuration ---
NEO4J_URI = "bolt://localhost:7687"  # Replace with your Neo4j URI
NEO4J_USER = "neo4j"              # Replace with your Neo4j Username
NEO4J_PASSWORD = "abc123456"    # Replace with your Neo4j Password
NEO4J_DATABASE = "neo4j"           # Replace with your target database name if not default

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Neo4j Connection ---
driver = None
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    logging.info(f"Successfully connected to Neo4j at {NEO4J_URI}")
except Neo4jError as e:
    logging.error(f"Failed to connect to Neo4j: {e}")
    exit() # Exit if connection fails
except Exception as e:
    logging.error(f"An unexpected error occurred during connection: {e}")
    exit()

# --- Helper Functions ---
def parse_datetime(dt_str):
    """Safely parse various ISO-like datetime formats."""
    if not dt_str:
        return None
    try:
        # Try standard ISO format with Z for UTC
        if dt_str.endswith('Z'):
            return datetime.datetime.fromisoformat(dt_str[:-1] + '+00:00')
        # Try standard ISO format (might have timezone offset or be naive)
        return datetime.datetime.fromisoformat(dt_str)
    except ValueError:
        try:
            # Try just date
            return datetime.datetime.combine(datetime.date.fromisoformat(dt_str), datetime.time.min)
        except ValueError:
             logging.warning(f"Could not parse datetime string: {dt_str}. Returning None.")
             return None

def parse_date(d_str):
    """Safely parse ISO date format."""
    if not d_str:
        return None
    try:
        return datetime.date.fromisoformat(d_str)
    except ValueError:
        logging.warning(f"Could not parse date string: {d_str}. Returning None.")
        return None

def get_condition_name_from_finding(finding_details):
    """ Helper to potentially create a condition name from finding details.
        Adjust this logic based on how you want to link findings to conditions.
        This example uses the finding description and details.
    """
    finding_desc = finding_details.get('finding', 'Unknown Finding')
    details = finding_details.get('details', '')
    if details:
        return f"{finding_desc} ({details})"
    return finding_desc


# --- Data Processing Functions ---

def import_patient_core(tx, patient_data):
    """Imports the core Patient node."""
    query = """
    MERGE (p:Patient {patientId: $patientId})
    ON CREATE SET
      p.idCard = $idCard,
      p.name = $name,
      p.birthDate = $birthDate,
      p.gender = $gender,
      p.updateAt = datetime()
    ON MATCH SET // Update potentially changeable info on match
      p.idCard = $idCard, // Ensure idCard is set even if node exists
      p.name = $name,
      p.birthDate = $birthDate,
      p.gender = $gender,
      p.updateAt = datetime() // Update the updateAt timestamp whenever the node is updated
    RETURN p.patientId AS patientId
    """
    result = tx.run(query,
                    patientId=patient_data.get("patientId"),
                    idCard=patient_data.get("idCard"),
                    name=patient_data.get("name"),
                    birthDate=parse_date(patient_data.get("birthDate")),
                    gender=patient_data.get("gender"))
    record = result.single()
    if record:
        logging.info(f"Imported/Updated Patient with patientId: {record['patientId']}")
    else:
        logging.warning(f"Failed to import/update patient with patientId: {patient_data.get('patientId')}")


def import_vital_signs(tx, patient_id, vital_signs):
    """Imports VitalSign nodes and relationships."""
    if not vital_signs: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // 使用MERGE而不是CREATE，基于类型、值和单位做唯一性约束
    MERGE (vs:VitalSign {type: $type, value: $value, unit: $unit, patientId: $patientId, timestamp: $timestamp})
    // 将timestamp添加到节点属性中，确保唯一性
    MERGE (p)-[:HAS_VITAL_SIGN {timestamp: $timestamp}]->(vs)
    """
    for vital in vital_signs:
        ts = parse_datetime(vital.get("timestamp")) or datetime.datetime.now() # Default to now if missing
        tx.run(query,
               patientId=patient_id,
               type=vital.get("type"),
               value=vital.get("value"),
               unit=vital.get("unit"),
               timestamp=ts)

def import_lab_results(tx, patient_id, lab_results):
    """
    Imports LabResult nodes and relationships, matching LabTest primarily by testCode.
    If testCode is missing or doesn't match, it falls back to testName.
    If LabTest doesn't exist, it creates it using available info (code and name).
    """
    if not lab_results:
        return

    # 使用 elementId() 代替 id()
    ensure_labtest_query = """
    MERGE (lt:LabTest {code: $testCode})
    ON CREATE SET lt.name = $testName
    ON MATCH SET lt.name = coalesce($testName, lt.name)
    WITH lt
    WHERE $testName IS NOT NULL AND lt.name IS NULL
    SET lt.name = $testName
    RETURN elementId(lt) AS labTestId, lt.code AS code, lt.name AS name
    """
    
    ensure_labtest_by_name_query = """
    MERGE (lt:LabTest {name: $testName})
    RETURN elementId(lt) AS labTestId, lt.name AS name
    """

    # 修改为使用MERGE来避免重复的实验室结果
    import_query = """
    MATCH (p:Patient {patientId: $patientId})
    MATCH (lt:LabTest) WHERE elementId(lt) = $labTestId
    // 使用MERGE代替CREATE，基于检测值、单位、参考范围、解释和时间戳作为唯一性约束
    MERGE (lr:LabResult {
        value: $value,
        unit: $unit,
        referenceRange: $referenceRange,
        interpretation: $interpretation,
        timestamp: $timestamp,
        patientId: $patientId, 
        labTestCode: lt.code,
        labTestName: lt.name
    })
    MERGE (p)-[:HAS_LAB_RESULT {timestamp: $timestamp}]->(lr)
    MERGE (lr)-[:RESULT_OF]->(lt)

    // Optional: Link high Triglycerides result to Hyperlipidemia risk
    WITH lr, p, lt
    WHERE (lt.code = 'CHEM0025' OR lt.name = '甘油三酯') AND lr.interpretation = '偏高'
    MERGE (c_hyperlipidemia:Condition {name: '高血脂风险', type: 'Risk'})
    MERGE (lr)-[:INDICATES_CONDITION]->(c_hyperlipidemia)
    MERGE (p)-[:HAS_CONDITION {status: 'Risk Identified', detectedDate: date()}]->(c_hyperlipidemia)
    """

    for lab in lab_results:
        ts = parse_datetime(lab.get("timestamp")) or datetime.datetime.now()
        test_code = lab.get("testCode")
        test_name = lab.get("testName")

        lab_test_id = None

        if test_code:
            result = tx.run(ensure_labtest_query, testCode=test_code, testName=test_name)
            record = result.single()
            if record:
                lab_test_id = record['labTestId']
            else:
                logging.warning(f"Could not MERGE LabTest using code {test_code} for patient {patient_id}")
                if test_name:
                    result_name = tx.run(ensure_labtest_by_name_query, testName=test_name)
                    record_name = result_name.single()
                    if record_name:
                        lab_test_id = record_name['labTestId']
                    else:
                        logging.error(f"Failed to MERGE LabTest using name {test_name} either for patient {patient_id}")
                        continue
        elif test_name:
            result = tx.run(ensure_labtest_by_name_query, testName=test_name)
            record = result.single()
            if record:
                lab_test_id = record['labTestId']
            else:
                logging.error(f"Failed to MERGE LabTest using name {test_name} (no code provided) for patient {patient_id}")
                continue
        else:
            logging.error(f"Skipping lab result due to missing testCode and testName for patient {patient_id}. Result data: {lab}")
            continue

        if lab_test_id:
            tx.run(import_query,
                   patientId=patient_id,
                   labTestId=lab_test_id,
                   value=lab.get("value"),
                   unit=lab.get("unit"),
                   referenceRange=lab.get("referenceRange"),
                   interpretation=lab.get("interpretation"),
                   timestamp=ts)

def import_conditions(tx, patient_id, conditions):
    """Imports Condition nodes and relationships."""
    if not conditions: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (c:Condition {name: $conditionName, type: $type}) // Merge based on name and type
    ON CREATE SET c.description = $description // Set description only on create
    MERGE (p)-[r:HAS_CONDITION]->(c) // Use MERGE for relationship
    SET r.status = $status,
        r.detectedDate = $detectedDate,
        r.diagnosedDate = $diagnosedDate // Use detectedDate or diagnosedDate as appropriate
    """
    for cond in conditions:
        detected_date = parse_date(cond.get("detectedDate") or cond.get("diagnosedDate"))
        tx.run(query,
               patientId=patient_id,
               conditionName=cond.get("conditionName"),
               type=cond.get("type"),
               description=cond.get("description"), # Will be null if not present
               status=cond.get("status"),
               detectedDate=detected_date,
               diagnosedDate=detected_date # Use the same parsed date for simplicity here
               )

def import_personal_history(tx, patient_id, history_items):
    """Imports PersonalHistoryItem nodes and relationships."""
    if not history_items: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (ph:PersonalHistoryItem {type: $type, patientId: $patientId}) // Include patient ID for uniqueness if needed
    ON CREATE SET ph.status = $status, ph.details = $details
    ON MATCH SET ph.status = $status, ph.details = $details // Update status/details on match
    MERGE (p)-[:HAS_PERSONAL_HISTORY]->(ph)

    // Optional: Link risk factors
    WITH ph, p, $type AS historyType
    WHERE historyType = 'Smoking'
    MERGE (c_ht:Condition {name: '高血压', type: 'Risk'}) // Ensure risk node exists
    MERGE (ph)-[:RISK_FACTOR_FOR]->(c_ht)

    WITH ph, p, $type AS historyType
    WHERE historyType = 'Alcohol'
    MERGE (c_hu:Condition {name: '高尿酸血症', type: 'Risk'}) // Ensure risk node exists
    MERGE (ph)-[:RISK_FACTOR_FOR]->(c_hu)
    """
    for item in history_items:
        tx.run(query,
               patientId=patient_id,
               type=item.get("type"),
               status=item.get("status"),
               details=item.get("details"))

def import_examinations(tx, patient_id, examinations):
    """Imports Examination and nested ExaminationFinding nodes."""
    if not examinations: return

    exam_query = """
    MATCH (p:Patient {patientId: $patientId})
    
    // 匹配对应的BodyPart和BodySystem
    MATCH (bp:BodyPart {name: $bodyPartExamined})
    MATCH (bs:BodySystem {name: $bodySystem})
    
    // 创建Examination节点并关联相关实体
    MERGE (ex:Examination {
        type: $examType,
        bodyPartExamined: $bodyPartExamined,
        bodySystem: $bodySystem,
        timestamp: $timestamp,
        patientId: $patientId
    })
    MERGE (p)-[:UNDERWENT_EXAMINATION {timestamp: $timestamp}]->(ex)
    MERGE (ex)-[:EXAMINED_BODY_PART]->(bp)
    MERGE (ex)-[:EXAMINED_BODY_SYSTEM]->(bs)
    
    RETURN elementId(ex) AS examNodeId, $findings AS findings
    """
    
    finding_query = """
    MATCH (ex) WHERE elementId(ex) = $examNodeId
    
    WITH ex, $findingData AS findingData
    
    // 创建或合并对应的Condition节点
    CALL {
        WITH findingData
        MERGE (c:Condition {name: CASE 
            WHEN findingData.finding = '甲状腺结节' THEN "甲状腺结节4类"
            WHEN findingData.finding = '肾结石' THEN "肾结石" 
            ELSE findingData.finding + " Finding"
        END, type: CASE 
            WHEN findingData.finding = '甲状腺结节' THEN "Finding"
            WHEN findingData.finding = '肾结石' THEN "Diagnosis"
            ELSE "Finding"
        END})
        RETURN c
    }
    
    // 创建ExaminationFinding节点
    MERGE (ef:ExaminationFinding {
        finding: findingData.finding,
        details: findingData.details,
        examinationId: elementId(ex)
    })
    MERGE (ex)-[:REVEALED_FINDING]->(ef)
    MERGE (ef)-[:ASSOCIATED_CONDITION]->(c)
    
    // 直接关联到检查涉及的BodyPart和BodySystem
    WITH ef, c, ex
    MATCH (ex)-[:EXAMINED_BODY_PART]->(bp)
    MATCH (ex)-[:EXAMINED_BODY_SYSTEM]->(bs)
    MERGE (c)-[:AFFECTS_BODY_PART]->(bp)
    MERGE (c)-[:PART_OF_SYSTEM]->(bs)
    """

    for exam in examinations:
        ts = parse_datetime(exam.get("timestamp")) or datetime.datetime.now()
        exam_result = tx.run(exam_query,
                           patientId=patient_id,
                           examType=exam.get("examType"),
                           bodyPartExamined=exam.get("bodyPartExamined"),
                           bodySystem=exam.get("bodySystem"),
                           timestamp=ts,
                           findings=exam.get("findings", []))

        record = exam_result.single()
        if record:
            exam_node_id = record['examNodeId']
            findings_list = record['findings']

            for finding in findings_list:
                tx.run(finding_query,
                     examNodeId=exam_node_id,
                     findingData=finding)
        else:
            logging.warning(f"Failed to create Examination node for type: {exam.get('examType')}")

def import_medical_history(tx, patient_id, history_events):
    """Imports MedicalHistoryEvent nodes and relationships."""
    if not history_events: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE based on patient, type, and date to avoid duplicates for same event on same day
    MERGE (mh:MedicalHistoryEvent {type: $type, description: $description, date: $date, patientId: $patientId})
    MERGE (p)-[:HAS_HISTORY_EVENT {date: $date}]->(mh)
    """
    for event in history_events:
        event_date = parse_date(event.get("date"))
        tx.run(query,
               patientId=patient_id,
               type=event.get("type"),
               description=event.get("description"),
               date=event_date)

def import_allergies(tx, patient_id, allergies):
    """Imports Allergy nodes and relationships."""
    if not allergies: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (a:Allergy {allergen: $allergen, patientId: $patientId}) // Unique per patient and allergen
    ON CREATE SET a.reaction = $reaction, a.severity = $severity
    ON MATCH SET a.reaction = $reaction, a.severity = $severity // Update details on match
    MERGE (p)-[:HAS_ALLERGY]->(a)
    """
    for allergy in allergies:
        tx.run(query,
               patientId=patient_id,
               allergen=allergy.get("allergen"),
               reaction=allergy.get("reaction"),
               severity=allergy.get("severity"))

def import_family_history(tx, patient_id, family_history):
    """Imports FamilyHistory nodes and relationships."""
    if not family_history: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE based on patient, relative, and condition to represent one fact
    MERGE (fh:FamilyHistory {
        relative: $relative,
        conditionName: $conditionName,
        patientId: $patientId
        })
    ON CREATE SET fh.details = $details
    ON MATCH SET fh.details = $details // Update details if needed
    MERGE (p)-[:HAS_FAMILY_HISTORY]->(fh)
    """
    for item in family_history:
        tx.run(query,
               patientId=patient_id,
               relative=item.get("relative"),
               conditionName=item.get("conditionName"),
               details=item.get("details"))

def import_marital_status(tx, patient_id, marital_status):
    """Imports MaritalStatus node and relationship."""
    if not marital_status: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (ms:MaritalStatus {patientId: $patientId}) // Assume one marital status node per patient
    SET ms.status = $status,
        ms.spouseHealth = $spouseHealth,
        ms.childrenCount = $childrenCount,
        ms.childrenHealth = $childrenHealth
    MERGE (p)-[:HAS_MARITAL_INFO]->(ms)
    """
    tx.run(query,
           patientId=patient_id,
           status=marital_status.get("status"),
           spouseHealth=marital_status.get("spouseHealth"),
           childrenCount=marital_status.get("childrenCount"),
           childrenHealth=marital_status.get("childrenHealth"))


# --- Main Import Function ---
def process_patient_json(patient_data):
    """Processes a single patient's JSON data and imports it into Neo4j."""
    if not patient_data or not patient_data.get("patientId"):
        logging.error("Patient data is missing or lacks a patientId.")
        return

    patient_id = patient_data["patientId"]
    logging.info(f"Processing patient with patientId: {patient_id}")

    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            # Execute imports within a single transaction for atomicity per patient
            session.execute_write(import_patient_core, patient_data)
            session.execute_write(import_vital_signs, patient_id, patient_data.get("vitalSigns"))
            session.execute_write(import_lab_results, patient_id, patient_data.get("labResults"))
            session.execute_write(import_conditions, patient_id, patient_data.get("conditions"))
            session.execute_write(import_personal_history, patient_id, patient_data.get("personalHistory"))
            session.execute_write(import_examinations, patient_id, patient_data.get("examinations"))
            session.execute_write(import_medical_history, patient_id, patient_data.get("medicalHistory"))
            session.execute_write(import_allergies, patient_id, patient_data.get("allergies"))
            session.execute_write(import_family_history, patient_id, patient_data.get("familyHistory"))
            session.execute_write(import_marital_status, patient_id, patient_data.get("maritalStatus"))
            logging.info(f"Successfully processed patient with patientId: {patient_id}")

    except Neo4jError as e:
        logging.error(f"Neo4j error while processing patient {patient_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing patient {patient_id}: {e}")


# --- Placeholder for API Fetching ---
def get_patient_data_from_api(patient_id):
    """
    Placeholder function to simulate fetching patient data.
    Replace this with your actual API call logic.
    """
    logging.info(f"Simulating API call for patientId: {patient_id}")
    # In a real scenario, make HTTP request, handle errors, parse JSON
    # For this example, we just return the sample JSON if the ID matches
    if patient_id == "P12345":
         # Use the sample JSON defined earlier
         return sample_patient_json
    else:
        logging.warning(f"No data found for patientId: {patient_id}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    if driver:
        # 导入示例数据
        from sample_patient_elderly import sample_patient_elderly

        # --- Example: Import a single patient ---
        patient_id_to_import = sample_patient_elderly["patientId"]  # 直接从示例数据中获取ID
        patient_json_data = sample_patient_elderly

        if patient_json_data:
            process_patient_json(patient_json_data)
        else:
            logging.warning(f"Could not retrieve data for patient {patient_id_to_import}. Skipping import.")

        # --- Example: Import multiple patients (if your API supports fetching multiple IDs) ---
        # patient_ids = ["P12345", "P67890"]
        # for pid in patient_ids:
        #     data = get_patient_data_from_api(pid)
        #     if data:
        #         process_patient_json(data)
        #     else:
        #         logging.warning(f"Could not retrieve data for patient {pid}. Skipping import.")

        # Close the driver when done
        driver.close()
        logging.info("Neo4j driver closed.")
    else:
        logging.error("Neo4j driver not initialized. Exiting.")