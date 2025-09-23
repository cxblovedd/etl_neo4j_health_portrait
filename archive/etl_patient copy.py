# --- START OF REFACTORED FILE etl_patient.py ---

import datetime
import logging
from collections import defaultdict
from etl.utils.logger import setup_logger

logger = setup_logger('etl_core')

# --- Helper Functions (No changes needed) ---
def parse_datetime(dt_str):
    """Safely parse various ISO-like datetime formats."""
    if not dt_str:
        return None
    try:
        # Handle formats like "2023-10-24 09:04:12" or "2025-05-18T17:30:18.813"
        return datetime.datetime.fromisoformat(dt_str.replace(' ', 'T'))
    except ValueError:
        try:
            # Fallback for date only
            dt = datetime.datetime.combine(datetime.date.fromisoformat(dt_str), datetime.time.min)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
             logging.warning(f"Could not parse datetime string: {dt_str}. Returning None.")
             return None

def parse_date(d_str):
    """Safely parse various date formats."""
    if not d_str:
        return None
    dt = parse_datetime(d_str)
    return dt.date() if dt else None

# --- Data Processing Functions (Refactored) ---

def import_patient_core(tx, patient_data):
    """Imports the core Patient node, extracting height/weight from physicalTraitsList."""
    query = """
    MERGE (p:Patient {patientId: $patientId})
    ON CREATE SET
      p.birthDate = $birthDate,
      p.gender = $gender,
      p.createdAt = datetime()
    ON MATCH SET
      p.gender = $gender, // Gender might be corrected
      p.updatedAt = datetime()
    // Height and Weight are updated separately if present
    """
    tx.run(query,
           patientId=patient_data.get("patientId"),
           birthDate=parse_date(patient_data.get("birthDate")),
           gender=patient_data.get("gender"))
    
    # Process physical traits for height and weight
    traits = patient_data.get("physicalTraitsList", [])
    if traits:
        # Find the most recent trait record
        latest_trait = max(traits, key=lambda t: parse_datetime(t.get("recordedAt", "1900-01-01")), default=None)
        if latest_trait:
            height = latest_trait.get("heightCm")
            weight = latest_trait.get("weightKg")
            bmi = latest_trait.get("bmi")
            
            update_hw_query = """
            MATCH (p:Patient {patientId: $patientId})
            SET p.height = $height, p.weight = $weight, p.bmi = $bmi, p.updatedAt = datetime()
            """
            tx.run(update_hw_query, patientId=patient_data.get("patientId"), height=height, weight=weight, bmi=bmi)
    
    logging.info(f"Imported/Updated Patient with patientId: {patient_data.get('patientId')}")


def import_marital_status(tx, patient_id, marital_list):
    """Imports MaritalStatus from maritalReproductiveList by taking the latest record."""
    if not marital_list: 
        # 如果列表为空，直接返回，逻辑保留
        return
    
    # 从列表中找到最新的一条记录
    latest_marital = max(marital_list, key=lambda m: parse_datetime(m.get("recordedAt", "1900-01-01")), default=None)
    
    # 智能过滤：如果最新记录本身没有有效信息（如sourceRecordId），则跳过
    if not latest_marital or not latest_marital.get("sourceRecordedId"):
        logging.warning(f"Skipping marital status for patient {patient_id} due to missing data or sourceRecordId in latest record.")
        return

    query = """
    MATCH (p:Patient {patientId: $patientId})
    // For simplicity, store marital info directly on the patient node.
    // An alternative is creating a related node if history tracking is critical.
    SET p.maritalStatus = $status,
        p.spouseHealth = $spouseHealth,
        p.numberOfChildren = $numberOfChildren,
        p.maritalSourceRecordId = $sourceRecordId, // Track the source of this info
        p.updatedAt = datetime()
    """
    tx.run(query,
           patientId=patient_id,
           status=latest_marital.get("maritalStatus"),
           spouseHealth=latest_marital.get("spouseHealthStatus"),
           numberOfChildren=latest_marital.get("childrenCount"),
           sourceRecordId=latest_marital.get("sourceRecordedId")
          )

def import_diagnosis_history(tx, patient_id, diagnoses_list):
    """Imports all diagnoses from the flat diagnosesList."""
    if not diagnoses_list: return
    
    query = """
    UNWIND $diagnoses as diagnosis
    // Skip if no disease code
    WITH diagnosis WHERE diagnosis.diseaseCode IS NOT NULL
    
    MATCH (p:Patient {patientId: $patientId})
    
    // Merge the canonical Condition node
    MERGE (c:Condition {code: diagnosis.diseaseCode})
    ON CREATE SET
        c.name = diagnosis.diseaseName,
        c.createdAt = datetime()
    ON MATCH SET
        c.name = coalesce(diagnosis.diseaseName, c.name) // Update name if provided

    // Merge the relationship representing this diagnosis instance for the patient
    MERGE (p)-[r:HAS_DIAGNOSIS {diagnosisId: diagnosis.diagnosisId}]->(c)
    ON CREATE SET
        r.diagnosisDate = $dateParser(diagnosis.diagnosisDate),
        r.visitType = diagnosis.visitType,
        r.isChronic = (diagnosis.chronicRefractoryDiseaseFlag = '1'),
        r.createdAt = datetime()
    """
    # Filter out diagnoses with no code before passing to Cypher
    valid_diagnoses = [d for d in diagnoses_list if d.get("diseaseCode")]
    tx.run(query, patientId=patient_id, diagnoses=valid_diagnoses, dateParser=parse_datetime)


def import_allergies(tx, patient_id, allergies_list):
    """Imports allergies, creating canonical Allergen nodes."""
    if not allergies_list: return

    query = """
    UNWIND $allergies as allergy
    WITH allergy WHERE allergy.allergen IS NOT NULL AND allergy.allergen <> '未知'

    MATCH (p:Patient {patientId: $patientId})

    // MERGE the canonical Allergen concept node
    MERGE (a:Allergen {name: allergy.allergen})
    ON CREATE SET a.type = allergy.allergenType

    // MERGE the relationship with sourceId to ensure idempotency
    MERGE (p)-[r:HAS_ALLERGY {sourceRecordId: allergy.allergyId}]->(a)
    SET r.reaction = allergy.reaction,
        r.recordedAt = $dateParser(allergy.recordedAt),
        r.updatedAt = datetime()
    """
    tx.run(query, patientId=patient_id, allergies=allergies_list, dateParser=parse_datetime)


def import_family_history(tx, patient_id, family_history_list):
    """Imports family history, skipping 'unknown' entries."""
    if not family_history_list: return

    query = """
    UNWIND $items as item
    // Filter out meaningless data
    WITH item WHERE item.relativeRelationship <> '不详' AND item.relativeDisease <> '不详'

    MATCH (p:Patient {patientId: $patientId})
    
    // Create a node for the specific family history fact
    MERGE (fh:FamilyHistoryFact {
        relative: item.relativeRelationship, 
        conditionName: item.relativeDisease
    })

    // Link patient to this fact
    MERGE (p)-[r:HAS_FAMILY_HISTORY {sourceRecordId: item.familyHistoryId}]->(fh)
    SET r.recordedAt = $dateParser(item.recordedAt),
        r.updatedAt = datetime()
    """
    tx.run(query, patientId=patient_id, items=family_history_list, dateParser=parse_datetime)


def import_medical_events_history(tx, patient_id, surgeries, traumas, blood_transfusions):
    """Imports surgeries, traumas, and blood transfusions as MedicalHistoryEvent nodes."""
    # 只有当所有列表都为空时才返回
    if not surgeries and not traumas and not blood_transfusions: 
        return

    query = """
    UNWIND $events as event
    MATCH (p:Patient {patientId: $patientId})
    
    // Create a distinct event node for each history item
    MERGE (e:MedicalHistoryEvent {sourceId: event.id})
    ON CREATE SET
        e.type = event.type,
        e.name = event.name,
        e.date = $dateParser(event.date),
        e.bodySite = event.bodySite,
        e.details = event.details,
        e.createdAt = datetime()

    MERGE (p)-[:HAS_HISTORY_EVENT]->(e)
    """
    
    events = []
    # (1) 处理手术史 (逻辑不变)
    for s in surgeries:
        if s.get("surgeryName"):
            events.append({
                "id": f"surgery-{s.get('pastSurgeriestId')}",
                "type": "Surgery", "name": s.get("surgeryName"),
                "date": s.get("surgeryDate"), "bodySite": s.get("bodySite"),
                "details": s.get("notes")
            })
            
    # (2) 处理外伤史 (逻辑不变)
    for t in traumas:
        if t.get("traumaType"):
             events.append({
                "id": f"trauma-{t.get('pastTraumasId')}",
                "type": "Trauma", "name": t.get("traumaType"),
                "date": t.get("traumasDate"), "bodySite": t.get("bodySite"),
                "details": f"Severity: {t.get('severity')}, Healed: {t.get('healed')}"
            })

    # (3) 新增：处理输血史
    for bt in blood_transfusions:
        # **智能过滤**: 只有当存在关键信息（如输血日期）时，才创建记录
        if bt.get("bloodTransfusionsDate"):
            events.append({
                "id": f"transfusion-{bt.get('pastBloodTransfusionsId')}",
                "type": "BloodTransfusion",
                "name": "输血史", # 给予一个标准名称
                "date": bt.get("bloodTransfusionsDate"),
                "bodySite": None,
                "details": f"Volume: {bt.get('volumeMl', 'N/A')} ml"
            })
            
    if events:
        tx.run(query, patientId=patient_id, events=events, dateParser=parse_date)
def import_lifestyle_history(tx, patient_id, patient_data):
    """Consolidates various personal/lifestyle history lists into PersonalHistoryItem nodes."""
    
    query = """
    UNWIND $items as item
    MATCH (p:Patient {patientId: $patientId})

    // Merge a canonical node for the lifestyle fact
    MERGE (phi:PersonalHistoryItem {category: item.category, type: item.type})

    // Link patient to it, using sourceRecordId for idempotency
    MERGE (p)-[r:HAS_LIFESTYLE_INFO {sourceRecordId: item.sourceRecordId}]->(phi)
    SET r.details = item.details,
        r.recordedAt = $dateParser(item.recordedAt),
        r.updatedAt = datetime()
    """
    
    items = []
    # Smoking
    for h in patient_data.get("personalSmokingHistoryList", []):
        items.append({
            "category": "Smoking", "type": h.get("status"),
            "details": h.get("historyDetails"), "sourceRecordId": f"smoke-{h.get('personalSmokingHistoryId')}",
            "recordedAt": h.get("createdAt")
        })
    # Alcohol
    for h in patient_data.get("personalAlcoholHistoryList", []):
        items.append({
            "category": "Alcohol", "type": h.get("frequency"),
            "details": h.get("historyDetails"), "sourceRecordId": f"alcohol-{h.get('personalAlcoholHistoryId')}",
            "recordedAt": h.get("historyDate")
        })
    # Diet
    for h in patient_data.get("dietHabitsList", []):
        items.append({
            "category": "Diet", "type": h.get("dietType"),
            "details": f"Flavor: {h.get('flavorType')}", "sourceRecordId": h.get("sourceRecordedId"),
            "recordedAt": h.get("recordedAt")
        })
    # Sleep
    for h in patient_data.get("sleepAssessmentList", []):
         items.append({
            "category": "Sleep", "type": h.get("sleepQuality"),
            "details": f"Duration: {h.get('sleepDuration')}", "sourceRecordId": h.get("sourceRecordedId"),
            "recordedAt": h.get("recordedAt")
        })

    if items:
        valid_items = [i for i in items if i.get("type") and i.get("sourceRecordId")]
        if valid_items:
            tx.run(query, patientId=patient_id, items=valid_items, dateParser=parse_datetime)


def import_clinical_events(tx, patient_id, jc_list, jy_list):
    """Groups Lab (jy) and Exam (jc) results by date into 'implicit' Encounters."""
    if not jc_list and not jy_list: return

    # Group events by date in Python first
    events_by_date = defaultdict(lambda: {'labs': [], 'exams': []})
    
    for lab in jy_list:
        event_date = parse_date(lab.get("bgfbsj"))
        if event_date:
            events_by_date[event_date]['labs'].append(lab)

    for exam in jc_list:
        event_date = parse_date(exam.get("bgfbsj"))
        if event_date:
            events_by_date[event_date]['exams'].append(exam)
            
    # Cypher queries
    encounter_query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (e:Encounter {patientId: $patientId, encounterDate: $date})
    MERGE (p)-[:HAD_ENCOUNTER]->(e)
    RETURN elementId(e) as encounterElementId
    """
    
    lab_query = """
    UNWIND $labs as lab
    // Find the encounter for this batch
    MATCH (e) WHERE elementId(e) = $encounterElementId
    
    // Merge canonical LabTest node
    MERGE (lt:LabTest {code: lab.jyxmdm})
    ON CREATE SET lt.name = lab.jyxmmc
    
    // Create the specific LabResult node
    // Use a unique ID from the source if possible, otherwise composite key
    MERGE (lr:LabResult {
        encounterId: e.encounterId, 
        testCode: lab.jyxmdm,
        timestamp: $dateParser(lab.bgfbsj)
    })
    SET lr.patientId = $patientId,
        lr.value = lab.jyjg,
        lr.unit = lab.jyjgdw,
        lr.referenceRange = lab.jyzcfw
        
    MERGE (e)-[:RECORDED_LAB_RESULT]->(lr)
    MERGE (lr)-[:RESULT_OF]->(lt)
    """

    exam_query = """
    UNWIND $exams as exam
    MATCH (e) WHERE elementId(e) = $encounterElementId
    
    // Create Examination node for each report
    MERGE (ex:Examination {
        encounterId: e.encounterId,
        name: exam.jcxmmc,
        timestamp: $dateParser(exam.bgfbsj)
    })
    SET ex.patientId = $patientId,
        ex.bodyPart = exam.jcbw,
        ex.findings = exam.jcjg, // Store text as properties
        ex.description = exam.jcsj
        
    MERGE (e)-[:PERFORMED]->(ex)
    """
    
    # Process each day's events
    for date, events in events_by_date.items():
        result = tx.run(encounter_query, patientId=patient_id, date=date)
        record = result.single()
        if not record:
            logging.error(f"Failed to create implicit encounter for patient {patient_id} on date {date}")
            continue
        
        encounter_element_id = record['encounterElementId']
        
        if events['labs']:
            tx.run(lab_query, encounterElementId=encounter_element_id, patientId=patient_id, labs=events['labs'], dateParser=parse_datetime)
            
        if events['exams']:
            tx.run(exam_query, encounterElementId=encounter_element_id, patientId=patient_id, exams=events['exams'], dateParser=parse_datetime)


# --- Main Transaction Orchestrator ---
def import_patient_data_from_json(tx, patient_json_data):
    """
    Main transaction function to process the new flat JSON structure.
    """
    if patient_json_data.get("code") != 0 or not patient_json_data.get("data"):
        logging.error("Invalid JSON data format or error code.")
        return

    p_data = patient_json_data["data"]
    p_id = p_data.get("patientId")
    if not p_id:
        logging.error("Patient data is missing 'patientId'.")
        return

    # 1. Core Patient Info
    import_patient_core(tx, p_data)

    # 2. Directly Linked Patient Attributes and History
    import_marital_status(tx, p_id, p_data.get("maritalReproductiveList"))
    import_diagnosis_history(tx, p_id, p_data.get("diagnosesList"))
    import_allergies(tx, p_id, p_data.get("allergyProfilesList"))
    import_family_history(tx, p_id, p_data.get("familyHistoryList"))
    
    # 3. Consolidated Medical and Lifestyle History
    import_medical_events_history(
        tx, p_id, 
        p_data.get("pastSurgeriesList", []), 
        p_data.get("pastTraumasList", []),
        p_data.get("pastBloodTransfusionsList", []) # <--- 新增传递此列表
    )
    import_lifestyle_history(tx, p_id, p_data)

    # 4. Process Clinical Events (Labs/Exams) by grouping them into implicit encounters
    import_clinical_events(tx, p_id, p_data.get("jcList", []), p_data.get("jyList", []))
    
    logging.info(f"Successfully processed all data for patient {p_id}")