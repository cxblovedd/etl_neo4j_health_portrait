# --- START OF FILE etl_patient_to_neo4j_rel_sourceid.py ---

import json
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import datetime
import logging

# --- Configuration ---
NEO4J_URI = "bolt://10.55.108.31:7687"  # Replace with your Neo4j URI
NEO4J_USER = "neo4j"              # Replace with your Neo4j Username
NEO4J_PASSWORD = "hayymoni2018"    # Replace with your Neo4j Password
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
        if dt_str.endswith('Z'):
            dt = datetime.datetime.fromisoformat(dt_str[:-1])
            return dt.replace(tzinfo=datetime.timezone.utc)
        return datetime.datetime.fromisoformat(dt_str)
    except ValueError:
        try:
            dt = datetime.datetime.combine(datetime.date.fromisoformat(dt_str), datetime.time.min)
            return dt.replace(tzinfo=datetime.timezone.utc)
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
      p.height = $height,
      p.weight = $weight,
      p.occupation = $occupation,
      p.createdAt = datetime(),
      p.updatedAt = datetime()
    ON MATCH SET
      p.occupation = coalesce($occupation, p.occupation),
      p.updatedAt = datetime(),
      p.height = coalesce($height, p.height),
      p.weight = coalesce($weight, p.weight)
    RETURN p.patientId AS patientId
    """
    # ... (rest of the function remains the same, including contact info update) ...
    result = tx.run(query,
                    patientId=patient_data.get("patientId"),
                    idCard=patient_data.get("idCard"),
                    name=patient_data.get("name"),
                    birthDate=parse_date(patient_data.get("birthDate")),
                    gender=patient_data.get("gender"),
                    height=patient_data.get("height"),
                    weight=patient_data.get("weight"),
                    occupation=patient_data.get("occupation"))
    record = result.single()
    if record:
        logging.info(f"Imported/Updated Patient with patientId: {record['patientId']}")
    else:
        logging.warning(f"Failed to MERGE patient with patientId: {patient_data.get('patientId')}")
    if patient_data.get("contactInfo"):
        contact_query = """
        MATCH (p:Patient {patientId: $patientId})
        SET p.phone = $phone,
            p.address = $address,
            p.updatedAt = datetime()
        """
        tx.run(contact_query,
               patientId=patient_data.get("patientId"),
               phone=patient_data.get("contactInfo", {}).get("phone"),
               address=patient_data.get("contactInfo", {}).get("address"))


def import_marital_status(tx, patient_id, marital_data):
    """Imports MaritalStatus node, links to Patient, stores sourceRecordId on the relationship."""
    if not marital_data: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE the status node, representing the concept for this patient
    MERGE (ms:MaritalStatus {patientId: $patientId})
    // SET the properties on the node representing the current/latest known status
    SET ms.status = $status,
        ms.spouseHealth = $spouseHealth,
        ms.hasChildren = $hasChildren,
        ms.numberOfChildren = $numberOfChildren,
        ms.parentingSituation = $parentingSituation,
        ms.childrenHealth = $childrenHealth
    // MERGE the relationship and store source-specific info on it
    MERGE (p)-[r:HAS_MARITAL_INFO]->(ms)
    SET r.sourceRecordId = $sourceRecordId, // Store source ID on the relationship
        r.recordedDate = datetime(), // Use current time as recorded date for this update
        r.updatedAt = datetime()
    """
    tx.run(query,
           patientId=patient_id,
           status=marital_data.get("maritalStatus"),
           spouseHealth=marital_data.get("spouseHealthStatus"),
           hasChildren=marital_data.get("hasChildren"),
           numberOfChildren=marital_data.get("numberOfChildren"),
           parentingSituation=marital_data.get("parentingSituation"),
           childrenHealth=marital_data.get("childrenHealthStatus"),
           sourceRecordId=marital_data.get("sourceRecordId") # Get source ID
           )

def import_chronic_conditions(tx, patient_id, conditions):
    """
    Imports chronic Condition nodes (MERGE primarily by icdCode)
    and :HAS_CHRONIC_CONDITION relationships.
    """
    if not conditions: return

    # Cypher query to MERGE Condition node based on its code (icdCode)
    # It sets name and type on creation, and optionally updates them on match.
    query_merge_condition_by_code = """
    MERGE (c:Condition {code: $icdCode}) // Use 'code' property for the standard code
    ON CREATE SET
      c.name = $conditionName, // Set name from source on creation
      c.type = $type,         // Set type from source on creation
      c.icdCode = $icdCode,   // Store the original icdCode as a property too if needed
      c.createdAt = datetime()
    ON MATCH SET
      // Update name/type only if they are provided in the current data and different?
      // Or simply ensure they exist using coalesce if they might be missing initially.
      c.name = coalesce($conditionName, c.name),
      c.type = coalesce($type, c.type),
      // c.icdCode = $icdCode, // Code shouldn't change if matched by code
      c.updatedAt = datetime()
    RETURN elementId(c) AS conditionElementId // Return the internal ID
    """

    # Cypher query to link the Patient to the Condition node found/created above
    query_link_patient_to_condition = """
    MATCH (p:Patient {patientId: $patientId})
    MATCH (c) WHERE elementId(c) = $conditionElementId // Find the specific Condition node
    // MERGE the relationship between Patient and Condition
    MERGE (p)-[r:HAS_CHRONIC_CONDITION]->(c)
    // Set/Update properties on the relationship based on the current record
    SET r.status = $status,
        r.firstReportedDate = $firstReportedDate,
        // r.sourceRecordId = $sourceRecordId, // Uncomment if you have source ID for this fact
        r.updatedAt = datetime()
    """

    # Fallback Query (Use with caution if code is often missing)
    query_merge_condition_by_name_type = """
    MERGE (c:Condition {name: $conditionName, type: $type}) // Merge based on name and type
    ON CREATE SET
        c.icdCode = $icdCode, // Try to set ICD code if available even when merging by name
        c.createdAt = datetime()
    ON MATCH SET
        c.icdCode = coalesce(c.icdCode, $icdCode), // Attempt to fill in missing code on match
        c.updatedAt = datetime()
    RETURN elementId(c) AS conditionElementId
    """


    for cond in conditions:
        condition_code = cond.get("icdCode") # Assuming 'icdCode' is the field for the unique code
        condition_name = cond.get("conditionName")
        condition_type = cond.get("type")
        condition_element_id = None

        # --- Strategy: Prioritize Code ---
        if condition_code:
            # Try to MERGE using the code
            try:
                result = tx.run(query_merge_condition_by_code,
                                icdCode=condition_code,
                                conditionName=condition_name, # Pass name/type for CREATE/UPDATE
                                type=condition_type)
                record = result.single()
                if record:
                    condition_element_id = record['conditionElementId']
                else:
                    # This case should ideally not happen with MERGE unless there's a DB error
                    logging.error(f"MERGE Condition by code {condition_code} returned no record unexpectedly for patient {patient_id}.")
                    continue # Skip this condition if node merge failed
            except Exception as e:
                 logging.error(f"Error during MERGE Condition by code {condition_code} for patient {patient_id}: {e}")
                 continue # Skip this condition

        # --- Fallback Strategy (Optional - uncomment and use with caution) ---
        # elif condition_name and condition_type:
        #     logging.warning(f"Condition code missing for '{condition_name}', attempting MERGE by name and type for patient {patient_id}.")
        #     try:
        #         result = tx.run(query_merge_condition_by_name_type,
        #                         conditionName=condition_name,
        #                         type=condition_type,
        #                         icdCode=condition_code) # Pass code even if null, might be set on match
        #         record = result.single()
        #         if record:
        #             condition_element_id = record['conditionElementId']
        #         else:
        #             logging.error(f"Failed to MERGE Condition by name/type '{condition_name}' for patient {patient_id}")
        #             continue
        #     except Exception as e:
        #         logging.error(f"Error during MERGE Condition by name/type '{condition_name}' for patient {patient_id}: {e}")
        #         continue

        # --- Strict Mode: If no code (and fallback not used), skip ---
        else:
            logging.warning(f"Skipping chronic condition '{condition_name}' link due to missing 'icdCode' for patient {patient_id}.")
            continue # Skip if no code and no fallback

        # --- Link Patient to Condition ---
        if condition_element_id:
            try:
                tx.run(query_link_patient_to_condition,
                       patientId=patient_id,
                       conditionElementId=condition_element_id,
                       status=cond.get("status"),
                       firstReportedDate=parse_date(cond.get("firstReportedDate"))
                       # sourceRecordId=cond.get("sourceRecordId")
                       )
            except Exception as e:
                logging.error(f"Error linking patient {patient_id} to condition node {condition_element_id}: {e}")

def import_medical_history(tx, patient_id, history_events):
    """Imports MedicalHistoryEvent nodes, links to Patient, stores sourceRecordId on relationship."""
    if not history_events: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE event node based on patient, type, description, date
    MERGE (mh:MedicalHistoryEvent {
        patientId: $patientId,
        type: $type,
        description: $description,
        date: $date
        })
    ON CREATE SET mh.eventId = $eventId
    // MERGE the relationship and store source info there
    MERGE (p)-[r:HAS_HISTORY_EVENT]->(mh) // Removed date from relationship merge key
    SET r.sourceRecordId = $sourceRecordId,
        r.recordedDate = $date, // Event date is the recording date here
        r.updatedAt = datetime()
    """
    for event in history_events:
        event_date = parse_date(event.get("date"))
        if event_date:
            tx.run(query,
                   patientId=patient_id,
                   eventId=event.get("eventId"),
                   type=event.get("type"),
                   description=event.get("description"),
                   date=event_date,
                   sourceRecordId=event.get("sourceRecordId")
                   )

def import_personal_history(tx, patient_id, history_items):
    """Imports PersonalHistoryItem nodes, links to Patient, stores details/status/sourceRecordId on relationship."""
    if not history_items: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE history item node representing the TYPE for this patient
    MERGE (ph:PersonalHistoryItem {type: $type})
    // MERGE the relationship representing THIS RECORD of the history item
    // Use sourceRecordId in MERGE key if a single source should only create one relationship instance for this type
    MERGE (p)-[r:HAS_PERSONAL_HISTORY {sourceRecordId: $sourceRecordId}]->(ph)
    // Set the details FROM THIS RECORD onto the relationship
    ON CREATE SET // Set details only when this specific source record relationship is first created
      r.status = $status,
      r.details = $details,
      r.recordedDate = datetime(), // Or use a date from source if available
      r.updatedAt = datetime()
    ON MATCH SET // Update details if the same source record is processed again
      r.status = $status,
      r.details = $details,
      r.recordedDate = coalesce(r.recordedDate, datetime()), // Keep original recorded date if exists
      r.updatedAt = datetime()

    // // Alternative: If you want a relationship for EACH record, even from same source, use CREATE instead of MERGE for the relationship:
    // CREATE (p)-[r:HAS_PERSONAL_HISTORY {
    //     status: $status,
    //     details: $details,
    //     sourceRecordId: $sourceRecordId,
    //     recordedDate: datetime(),
    //     updatedAt: datetime()
    // }]->(ph)
    """
    # Removed RISK_FACTOR_FOR creation
    for item in history_items:
        # Basic validation
        if item.get("type") and item.get("sourceRecordId"):
             tx.run(query,
                    patientId=patient_id,
                    type=item.get("type"),
                    status=item.get("status"),
                    details=item.get("details"),
                    sourceRecordId=item.get("sourceRecordId")
                    )
        else:
            logging.warning(f"Skipping personal history item due to missing type or sourceRecordId for patient {patient_id}. Data: {item}")


def import_family_history(tx, patient_id, family_history):
    """Imports FamilyHistory nodes, links to Patient, stores details/sourceRecordId on relationship."""
    if not family_history: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE family history fact node
    MERGE (fh:FamilyHistory {
        patientId: $patientId,
        relative: $relative,
        conditionName: $conditionName
        })
    // MERGE the relationship representing THIS RECORD of the family history fact
    MERGE (p)-[r:HAS_FAMILY_HISTORY {sourceRecordId: $sourceRecordId}]->(fh)
    // Set the details FROM THIS RECORD onto the relationship
    ON CREATE SET
      r.details = $details,
      r.recordedDate = datetime(),
      r.updatedAt = datetime()
    ON MATCH SET
      r.details = $details, // Always update details from source
      r.recordedDate = coalesce(r.recordedDate, datetime()),
      r.updatedAt = datetime()
    """
    for item in family_history:
        if item.get("relative") and item.get("conditionName") and item.get("sourceRecordId"):
            tx.run(query,
                   patientId=patient_id,
                   relative=item.get("relative"),
                   conditionName=item.get("conditionName"),
                   details=item.get("details"),
                   sourceRecordId=item.get("sourceRecordId")
                   )
        else:
             logging.warning(f"Skipping family history item due to missing relative, conditionName, or sourceRecordId for patient {patient_id}. Data: {item}")


def import_allergies(tx, patient_id, allergies):
    """Imports Allergy nodes, links to Patient, stores reaction/severity/sourceRecordId on relationship."""
    if not allergies: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    // MERGE allergy concept node for this patient
    MERGE (a:Allergy {patientId: $patientId, allergen: $allergen})
    // MERGE the relationship representing THIS RECORD of the allergy
    MERGE (p)-[r:HAS_ALLERGY {sourceRecordId: $sourceRecordId}]->(a)
    // Set the details FROM THIS RECORD onto the relationship
    ON CREATE SET
      r.reaction = $reaction,
      r.severity = $severity,
      r.recordedDate = datetime(),
      r.updatedAt = datetime()
    ON MATCH SET
      r.reaction = $reaction, // Always update details from source
      r.severity = $severity,
      r.recordedDate = coalesce(r.recordedDate, datetime()),
      r.updatedAt = datetime()
    """
    for allergy in allergies:
         if allergy.get("allergen") and allergy.get("sourceRecordId"):
            tx.run(query,
                   patientId=patient_id,
                   allergen=allergy.get("allergen"),
                   reaction=allergy.get("reaction"),
                   severity=allergy.get("severity"),
                   sourceRecordId=allergy.get("sourceRecordId")
                   )
         else:
             logging.warning(f"Skipping allergy item due to missing allergen or sourceRecordId for patient {patient_id}. Data: {allergy}")


# --- Encounter related functions (import_encounter_data, import_encounter_diagnoses, etc.) ---
# These functions remain largely the same as the previous version, as they primarily deal with
# instance data (LabResult, VitalSign, Examination, Finding) linked to an Encounter.
# Source IDs for these instances (like reportId, examId, findingId) can reasonably
# be properties *of the instance nodes themselves*.

def import_encounter_diagnoses(tx, encounter_id, diagnoses_recorded):
    """从就诊记录的诊断生成或匹配Condition节点，优先使用icdCode匹配"""
    if not diagnoses_recorded: return
    query = """
    MATCH (e:Encounter {encounterId: $encounterId})
    
    // 优先通过icdCode匹配Condition节点
    MERGE (c:Condition {icdCode: $icdCode})
    ON CREATE SET 
        c.name = $conditionName,
        c.type = $type,
        c.createdAt = datetime()
    ON MATCH SET 
        c.name = coalesce($conditionName, c.name),
        c.type = coalesce($type, c.type),
        c.updatedAt = datetime()
    
    // 创建就诊到诊断的关系
    MERGE (e)-[r:RECORDED_DIAGNOSIS]->(c)
    SET r.status = $status,
        r.updatedAt = datetime()
    """

    # 对于没有icdCode的诊断，使用name+type匹配
    query_no_icd = """
    MATCH (e:Encounter {encounterId: $encounterId})
    
    MERGE (c:Condition {name: $conditionName, type: $type})
    ON CREATE SET 
        c.createdAt = datetime()
    ON MATCH SET 
        c.updatedAt = datetime()
    
    MERGE (e)-[r:RECORDED_DIAGNOSIS]->(c)
    SET r.status = $status,
        r.updatedAt = datetime()
    """
    
    for diag in diagnoses_recorded:
        if diag.get("icdCode"):  # 有ICD编码的情况
            tx.run(query,
                   encounterId=encounter_id,
                   icdCode=diag.get("icdCode"),
                   conditionName=diag.get("conditionName"),
                   type=diag.get("type"),
                   status=diag.get("status"))
        else:  # 没有ICD编码的情况
            tx.run(query_no_icd,
                   encounterId=encounter_id,
                   conditionName=diag.get("conditionName"),
                   type=diag.get("type"),
                   status=diag.get("status"))

def import_encounter_vitals(tx, encounter_id, patient_id, vitals_recorded):
    """Imports vital signs recorded during a specific encounter."""
    # This function structure is okay - VitalSign node represents the specific measurement instance
    if not vitals_recorded: return
    query = """
    MATCH (p:Patient {patientId: $patientId})
    MATCH (e:Encounter {encounterId: $encounterId})
    MERGE (vs:VitalSign {
        patientId: $patientId,
        type: $type,
        timestamp: $timestamp
        })
    ON CREATE SET
        vs.vitalId = $vitalId // Source ID for this specific vital reading
    SET vs.value = $value,
        vs.unit = $unit,
        vs.systolic = $systolic,
        vs.diastolic = $diastolic,
        vs.updatedAt = datetime()
    MERGE (p)-[:HAS_VITALSIGN_RECORD]->(vs) // General link to patient
    MERGE (vs)-[:MEASURED_DURING]->(e) // Link to the specific encounter
    """
    for vital in vitals_recorded:
        ts = parse_datetime(vital.get("timestamp"))
        if ts:
            tx.run(query,
                   patientId=patient_id,
                   encounterId=encounter_id,
                   vitalId=vital.get("vitalId"),
                   type=vital.get("type"),
                   value=vital.get("value"),
                   unit=vital.get("unit"),
                   systolic=vital.get("systolic"),
                   diastolic=vital.get("diastolic"),
                   timestamp=ts
                   )

def import_encounter_labs(tx, encounter_id, patient_id, labs_recorded):
    """Imports lab results recorded during a specific encounter."""
    # This function structure is okay - LabResult node represents the specific result instance
    if not labs_recorded: return

    ensure_labtest_query = """
    MERGE (lt:LabTest {code: $testCode})
    ON CREATE SET lt.name = $testName
    ON MATCH SET lt.name = coalesce($testName, lt.name)
    RETURN elementId(lt) AS labTestElementId, lt.code as labTestCode, lt.name as labTestName
    """
    ensure_labtest_by_name_query = """
    MERGE (lt:LabTest {name: $testName})
    RETURN elementId(lt) AS labTestElementId, lt.name as labTestName
    """
    import_query = """
    MATCH (e:Encounter {encounterId: $encounterId})
    MATCH (lt) WHERE elementId(lt) = $labTestElementId
    // MERGE LabResult node based on encounter, test code, and timestamp
    MERGE (lr:LabResult {
        encounterId: $encounterId, // Link to encounter
        labTestCode: $labTestCode,
        timestamp: $timestamp,
        patientId: $patientId // Store patientId for easier direct queries if needed
        })
    ON CREATE SET
        lr.reportId = $reportId // Use reportId from JSON if available on create
    SET lr.value = $value,
        lr.textValue = $textValue,
        lr.unit = $unit,
        lr.referenceRange = $referenceRange,
        lr.interpretation = $interpretation,
        lr.labTestName = $labTestName,
        lr.updatedAt = datetime()
    MERGE (e)-[:RECORDED_LAB_RESULT]->(lr) // Link encounter to result
    MERGE (lr)-[:RESULT_OF]->(lt) // Link result to test type
    """

    for lab in labs_recorded:
        ts = parse_datetime(lab.get("timestamp"))
        test_code = lab.get("testCode")
        test_name = lab.get("testName")
        lab_test_element_id = None
        actual_test_code = None
        actual_test_name = test_name

        if not ts:
            logging.warning(f"Skipping lab result due to missing/invalid timestamp for patient {patient_id}, encounter {encounter_id}. Data: {lab}")
            continue

        # Find or Create LabTest Node
        if test_code:
            result = tx.run(ensure_labtest_query, testCode=test_code, testName=test_name)
            record = result.single()
            if record:
                lab_test_element_id = record['labTestElementId']
                actual_test_code = record['labTestCode']
                actual_test_name = record['labTestName'] # Use name from DB if available
            else:
                logging.error(f"Failed to MERGE LabTest with code {test_code}")
                continue
        elif test_name:
            result = tx.run(ensure_labtest_by_name_query, testName=test_name)
            record = result.single()
            if record:
                lab_test_element_id = record['labTestElementId']
                actual_test_name = record['labTestName']
            else:
                logging.error(f"Failed to MERGE LabTest with name {test_name}")
                continue
        else:
            logging.warning(f"Skipping lab result due to missing testCode and testName for patient {patient_id}, encounter {encounter_id}. Data: {lab}")
            continue

        # Import LabResult and Link
        tx.run(import_query,
               encounterId=encounter_id,
               patientId=patient_id,
               labTestElementId=lab_test_element_id,
               labTestCode=actual_test_code,
               labTestName=actual_test_name,
               reportId=lab.get("reportId") or lab.get("resultId"),
               value=lab.get("value"),
               textValue=lab.get("textValue"),
               unit=lab.get("unit"),
               referenceRange=lab.get("referenceRange"),
               interpretation=lab.get("interpretation"),
               timestamp=ts)

def import_encounter_exams(tx, encounter_id, patient_id, exams_performed):
    """Imports examinations and findings recorded during a specific encounter."""
    if not exams_performed: return

    ensure_bp_query = "MERGE (:BodyPart {name: $location})"
    for exam in exams_performed:
        for finding in exam.get("findings", []):
            if finding.get("location"):
                tx.run(ensure_bp_query, location=finding.get("location"))

    exam_query = """
    MATCH (e:Encounter {encounterId: $encounterId})
    MERGE (ex:Examination {
        encounterId: $encounterId,
        type: $type,
        timestamp: $timestamp
        })
    ON CREATE SET
        ex.examId = $examId,
        ex.reportId = $reportId,
        ex.patientId = $patientId,
        ex.updatedAt = datetime()
    ON MATCH SET
        ex.reportId = coalesce($reportId, ex.reportId),
        ex.patientId = $patientId,
        ex.updatedAt = datetime()
    MERGE (e)-[:PERFORMED]->(ex)
    RETURN elementId(ex) AS examElementId, $findings AS findings
    """

    finding_query = """
    MATCH (ex) WHERE elementId(ex) = $examElementId
    
    // 通过icdCode匹配Condition节点
    MERGE (c:Condition {icdCode: $icdCode})
    ON CREATE SET 
        c.name = $suggestedConditionName,
        c.type = $suggestedConditionType,
        c.createdAt = datetime()
    ON MATCH SET 
        c.name = coalesce($suggestedConditionName, c.name),
        c.type = coalesce($suggestedConditionType, c.type),
        c.updatedAt = datetime()
        
    MERGE (bp:BodyPart {name: $location})
    MERGE (ef:ExaminationFinding {findingId: $findingId, examinationElementId: $examElementId})
    ON CREATE SET
        ef.finding = $finding,
        ef.details = $details,
        ef.locationName = $location,
        ef.updatedAt = datetime()
    ON MATCH SET
        ef.finding = coalesce($finding, ef.finding),
        ef.details = coalesce($details, ef.details),
        ef.locationName = coalesce($location, ef.locationName),
        ef.updatedAt = datetime()
    MERGE (ex)-[:REVEALED]->(ef)
    MERGE (ef)-[:LOCATED_IN]->(bp)
    MERGE (ef)-[r:SUGGESTS_CONDITION]->(c)
    SET r.suggestType = $suggestType,
        r.updatedAt = datetime()
    """

    for exam in exams_performed:
        ts = parse_datetime(exam.get("timestamp"))
        if not ts:
            logging.warning(f"Skipping exam due to missing/invalid timestamp for patient {patient_id}, encounter {encounter_id}. Data: {exam}")
            continue

        exam_result = tx.run(exam_query,
                           encounterId=encounter_id,
                           patientId=patient_id,
                           examId=exam.get("examId"),
                           type=exam.get("type"),
                           timestamp=ts,
                           reportId=exam.get("reportId"),
                           findings=exam.get("findings", []))

        record = exam_result.single()
        if record:
            exam_element_id = record['examElementId']
            findings_list = record['findings']

            for finding in findings_list:
                if (finding.get("location") and 
                    finding.get("icdCode") and 
                    finding.get("suggestedConditionName") and 
                    finding.get("suggestedConditionType") and 
                    finding.get("findingId")):
                    
                    tx.run(finding_query,
                           examElementId=exam_element_id,
                           findingId=finding.get("findingId"),
                           finding=finding.get("finding"),
                           location=finding.get("location"),
                           details=finding.get("details"),
                           icdCode=finding.get("icdCode"),
                           suggestedConditionName=finding.get("suggestedConditionName"),
                           suggestedConditionType=finding.get("suggestedConditionType"),
                           suggestType=finding.get("suggestType", "CONFIRMED"))
                else:
                    logging.warning(f"Skipping finding due to missing required fields (including icdCode) for exam {exam_element_id}. Data: {finding}")

# --- Encounter Data Orchestrator ---
def import_encounter_data(tx, patient_id, encounter):
    """Imports all data associated with a single encounter."""
    encounter_id = encounter.get("encounterId")
    if not encounter_id:
        logging.warning(f"Skipping encounter due to missing encounterId for patient {patient_id}. Data: {encounter}")
        return

    encounter_query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (e:Encounter {encounterId: $encounterId})
    ON CREATE SET
        e.type = $type,
        e.encounterDate = $encounterDate,
        e.dischargeDate = $dischargeDate,
        e.department = $department,
        e.attendingProviderId = $attendingProviderId,
        e.createdAt = datetime(),
        e.updatedAt = datetime()
    ON MATCH SET
        e.type = coalesce($type, e.type),
        e.encounterDate = coalesce($encounterDate, e.encounterDate),
        e.dischargeDate = coalesce($dischargeDate, e.dischargeDate),
        e.department = coalesce($department, e.department),
        e.attendingProviderId = coalesce($attendingProviderId, e.attendingProviderId),
        e.updatedAt = datetime()
    MERGE (p)-[:HAD_ENCOUNTER]->(e)
    """
    tx.run(encounter_query,
           patientId=patient_id,
           encounterId=encounter_id,
           type=encounter.get("type"),
           encounterDate=parse_date(encounter.get("encounterDate")),
           dischargeDate=parse_date(encounter.get("dischargeDate")),
           department=encounter.get("department"),
           attendingProviderId=encounter.get("attendingProviderId"))

    logging.info(f"Processing data for encounter {encounter_id}")
    import_encounter_diagnoses(tx, encounter_id, encounter.get("diagnosesRecorded"))
    import_encounter_vitals(tx, encounter_id, patient_id, encounter.get("vitalSignsRecorded"))
    import_encounter_labs(tx, encounter_id, patient_id, encounter.get("labResultsRecorded"))
    import_encounter_exams(tx, encounter_id, patient_id, encounter.get("examinationsPerformed"))


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
            def import_all_data_tx(tx, p_data, p_id):
                import_patient_core(tx, p_data)
                import_marital_status(tx, p_id, p_data.get("maritalAndReproductiveHistory"))
                # import_chronic_conditions(tx, p_id, p_data.get("Conditions")) # Use correct key "Conditions"
                import_medical_history(tx, p_id, p_data.get("medicalHistory"))
                import_personal_history(tx, p_id, p_data.get("personalHistory"))
                import_family_history(tx, p_id, p_data.get("familyHistory"))
                import_allergies(tx, p_id, p_data.get("allergies"))

                for encounter in p_data.get("encounters", []):
                    import_encounter_data(tx, p_id, encounter)

            session.execute_write(import_all_data_tx, patient_data, patient_id)
            logging.info(f"Successfully processed patient with patientId: {patient_id}")

    except Neo4jError as e:
        logging.error(f"Neo4j error while processing patient {patient_id}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing patient {patient_id}: {e}", exc_info=True)

# --- Main Execution ---
if __name__ == "__main__":
    if driver:
        try:
            # Make sure 'patient.json' exists and contains the JSON data
            with open("patient2.json", 'r', encoding='utf-8') as f:
                sample_patient_json = json.load(f)
        except FileNotFoundError:
            logging.error("Error: patient.json not found.")
            sample_patient_json = None
        except json.JSONDecodeError as e:
            logging.error(f"Error: Could not decode patient.json: {e}")
            sample_patient_json = None

        if sample_patient_json:
            process_patient_json(sample_patient_json)
        else:
            logging.warning("No patient data loaded from JSON file.")

        driver.close()
        logging.info("Neo4j driver closed.")
    else:
        logging.error("Neo4j driver not initialized. Exiting.")
# --- END OF FILE etl_patient_to_neo4j_rel_sourceid.py ---