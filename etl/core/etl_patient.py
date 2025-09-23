# etl/core/etl_patient.py

import json
from datetime import datetime
from etl.utils.logger import setup_logger

logger = setup_logger('etl_patient_core') 


def parse_datetime(dt_str):
    """Safely parse datetime strings, trying multiple formats."""
    if not dt_str or not isinstance(dt_str, str):
        return None
    formats_to_try = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ"
    ]
    for fmt in formats_to_try:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    logger.warning(f"Could not parse date string: {dt_str} with any known format.")
    return None

def import_patient_data_from_json(tx, patient_json_data):
    """
    Main function to orchestrate the import of all parts of a patient's health portrait.
    """
    # data = patient_json_data.get('data', {})
    # if not data:
    #     logger.error("No 'data' field in patient JSON. Aborting import.")
    #     return
    data = patient_json_data

    patient_id = data.get('patientId')
    if not patient_id:
        logger.error("Missing 'patientId' in data. Aborting import.")
        return

    import_patient_core(tx, patient_id, data)
    # import_encounters(tx, patient_id, data.get('encounters', []))
    # import_allergies(tx, patient_id, data.get('allergyProfilesList', []))
    # import_family_history(tx, patient_id, data.get('familyHistoryList', []))
    # import_past_blood_transfusions(tx, patient_id, data.get('pastBloodTransfusionsList', []))
    # import_past_surgeries(tx, patient_id, data.get('pastSurgeriesList', []))
    # import_past_traumas(tx, patient_id, data.get('pastTraumasList', []))
    # import_past_vaccinations(tx, patient_id, data.get('pastVaccinationsList', []))
        # 对每个可能被迭代的列表进行None值检查
    encounters = data.get('encounters', [])
    if encounters is not None:
        import_encounters(tx, patient_id, encounters)

    allergies = data.get('allergyProfilesList', [])
    if allergies is not None:
        import_allergies(tx, patient_id, allergies)

    family_history = data.get('familyHistoryList', [])
    if family_history is not None:
        import_family_history(tx, patient_id, family_history)

    blood_transfusions = data.get('pastBloodTransfusionsList', [])
    if blood_transfusions is not None:
        import_past_blood_transfusions(tx, patient_id, blood_transfusions)

    surgeries = data.get('pastSurgeriesList', [])
    if surgeries is not None:
        import_past_surgeries(tx, patient_id, surgeries)

    traumas = data.get('pastTraumasList', [])
    if traumas is not None:
        import_past_traumas(tx, patient_id, traumas)

    vaccinations = data.get('pastVaccinationsList', [])
    if vaccinations is not None:
        import_past_vaccinations(tx, patient_id, vaccinations)
        
    family_members = data.get('familyMembers', [])
    if family_members:
        import_family_members(tx, patient_id, family_members)    
        
    import_personal_history(tx, patient_id, data)
    
    


def import_patient_core(tx, patient_id, data):
    """
    Imports the main patient node, ensuring it can merge with pre-built nodes from family members.
    """
    id_type = data.get('idType')
    id_value = data.get('idValue')

    # 步骤 1: "认领"查询
    # 如果存在一个通过证件信息创建的、但还没有patientId的预建节点，
    # 就把当前 ETL 的 patient_id 赋予它。
    # 这一步是连接预建节点和正式节点的关键。
    if id_type and id_value:
        claim_query = """
        MATCH (p:Patient {idType: $idType, idValue: $idValue})
        WHERE p.patientId IS NULL
        SET p.patientId = $patientId
        """
        tx.run(claim_query, 
               idType=id_type, 
               idValue=id_value, 
               patientId=patient_id)

    # 步骤 2: 主查询 (合并与更新)
    # 经过步骤1，现在可以安全地通过 patientId 来合并节点，不会产生重复。
    # 如果节点是预建的，它现在已经被“认领”了；如果是全新的，则会在这里被创建。
    query = """
    MERGE (p:Patient {patientId: $patientId})
    ON CREATE SET
        p.name = $name,
        p.empi = $empi,
        p.birthDate = $birthDate,
        p.gender = $gender,
        p.idValue = $idValue,
        p.idType = $idType,
        p.maritalStatus = $maritalStatus,
        p.createdAt = $createdAt
    ON MATCH SET
        p.name = $name,
        p.empi = $empi,
        p.birthDate = $birthDate,
        p.gender = $gender,
        p.idValue = $idValue,
        p.idType = $idType,
        p.maritalStatus = $maritalStatus,
        p.updateTime = $updateTime
    """
    tx.run(query,
           patientId=patient_id,
           name=data.get('name'),
           # 注意：根据您之前的JSON, 'empiNo' 已更正为 'empi'
           empi=data.get('empi'),
           birthDate=data.get('birthDate'),
           gender=data.get('gender'),
           idValue=id_value,
           idType=id_type,
           maritalStatus=data.get('maritalStatus'),
           # 假设 parse_datetime 函数存在
           createdAt=parse_datetime(data.get('createdAt')),
           updateTime=parse_datetime(data.get('updateTime'))
          )

def import_encounters(tx, patient_id, encounters_list):
    """Imports all encounters and their nested details, creating distinct nodes for hospitals, departments, and providers."""
    # 【新增】定义就诊类型的映射
    encounter_type_map = {
        '1': '门诊',
        '2': '住院',
        '3': '体检'
    }

    for encounter in encounters_list:
        encounter_id = encounter.get('encounterId')
        if not encounter_id:
            logger.debug(f"Skipping encounter for patient {patient_id} due to missing encounterId. Record: {encounter}")
            continue

        encounter_type_code = encounter.get('encounterType')
        # 【新增】获取可读的类型名称，如果找不到则使用"未知类型"
        encounter_type_name = encounter_type_map.get(str(encounter_type_code), '未知类型')

        # 1. 创建或合并 Encounter 节点本身，并加入类型名称
        encounter_query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (e:Encounter {encounterId: $encounterId})
        ON CREATE SET
            e.encounterType = $encounterType,
            e.typeName = $typeName,
            e.visitStartTime = $visitStartTime,
            e.visitEndTime = $visitEndTime
        ON MATCH SET
            e.encounterType = $encounterType,
            e.typeName = $typeName,
            e.visitStartTime = $visitStartTime,
            e.visitEndTime = $visitEndTime
        MERGE (p)-[:HAD_ENCOUNTER]->(e)
        """
        tx.run(encounter_query,
               patientId=patient_id,
               encounterId=encounter_id,
               encounterType=encounter_type_code,
               typeName=encounter_type_name, # 【新增】传入类型名称参数
               visitStartTime=parse_datetime(encounter.get('visitStartTime')),
               visitEndTime=parse_datetime(encounter.get('visitEndTime'))
              )
        
        # 2. 创建或合并 Hospital 节点，并建立关系 (已有逻辑，无需改动)
        hospital_id = encounter.get('hospitalId')
        if hospital_id:
            hospital_query = """
            MATCH (e:Encounter {encounterId: $encounterId})
            MERGE (h:Hospital {hospitalId: $hospitalId})
            ON CREATE SET h.name = $hospitalName
            ON MATCH SET h.name = $hospitalName
            MERGE (e)-[:AT_HOSPITAL]->(h)
            """
            tx.run(hospital_query,
                   encounterId=encounter_id,
                   hospitalId=hospital_id,
                   hospitalName=encounter.get('hospitalName')
                  )

        # 3. 创建或合并 Department 节点，并建立关系 (已有逻辑，无需改动)
        department_id = encounter.get('departmentId')
        if department_id:
            department_query = """
            MATCH (e:Encounter {encounterId: $encounterId})
            MERGE (d:Department {departmentId: $departmentId})
            ON CREATE SET d.name = $departmentName
            ON MATCH SET d.name = $departmentName
            MERGE (e)-[:IN_DEPARTMENT]->(d)
            """
            if hospital_id:
                department_query += " WITH d MATCH (h:Hospital {hospitalId: $hospitalId}) MERGE (h)-[:HAS_DEPARTMENT]->(d)"
            
            tx.run(department_query,
                   encounterId=encounter_id,
                   departmentId=department_id,
                   departmentName=encounter.get('departmentName'),
                   hospitalId=hospital_id
                  )
        
        # 4. 创建或合并 Provider (医生) 节点，并建立关系 (已有逻辑，无需改动)
        provider_id = encounter.get('attendingProviderId')
        if provider_id:
            provider_query = """
            MATCH (e:Encounter {encounterId: $encounterId})
            MERGE (doc:Provider {providerId: $providerId})
            ON CREATE SET doc.name = $providerName
            ON MATCH SET doc.name = $providerName
            MERGE (e)-[:TREATED_BY]->(doc)
            """
            tx.run(provider_query,
                   encounterId=encounter_id,
                   providerId=provider_id,
                   providerName=encounter.get('attendingProviderName')
                  )
        
        # 5. 导入该次就诊下的其他嵌套数据
        diagnoses = encounter.get('diagnoses', [])
        if diagnoses is not None:
            import_diagnoses_from_encounter(tx, encounter_id, diagnoses)
        
        examinations = encounter.get('examinations', [])
        if examinations is not None:
            import_examinations_from_encounter(tx, encounter_id, examinations)

        lab_tests = encounter.get('labTests', [])
        if lab_tests is not None:
            import_lab_tests_from_encounter(tx, encounter_id, lab_tests)

def import_diagnoses_from_encounter(tx, encounter_id, diagnoses_list):
    """
    导入诊断信息 (已修正 Cypher 语法错误)。
    """
    for diagnosis in diagnoses_list:
        disease_name = diagnosis.get('diagnosisName')
        disease_code = diagnosis.get('diagnosisNo')

        if not disease_name and not disease_code:
            logger.debug(f"Skipping diagnosis for encounter {encounter_id} due to missing name and code.")
            continue

        # Cypher 查询已修正
        query = """
        WITH $encounterId AS encounterId, $diseaseName AS dName, $diseaseCode AS dCode

        OPTIONAL MATCH (c1:Condition {code: dCode}) WHERE dCode IS NOT NULL
        OPTIONAL MATCH (c2:Condition {name: dName})
        WITH encounterId, dName, dCode, COALESCE(c1, c2) as existingCondition
        
        FOREACH(ignored IN CASE WHEN existingCondition IS NOT NULL THEN [] ELSE [1] END |
            CREATE (c:Condition)
                SET c.code = dCode, c.name = dName
        )
        
        WITH encounterId, dName, dCode
        MATCH (c:Condition) WHERE (dCode IS NOT NULL AND c.code = dCode) OR (dCode IS NULL AND c.name = dName)
        
        SET c.name = dName
        
        // --- 错误修正：在这里添加 WITH 子句 ---
        // 将变量 c 和 encounterId 传递给后续的 MATCH
        WITH c, encounterId
        
        MATCH (e:Encounter {encounterId: encounterId})
        
        MERGE (e)-[:RECORDED_DIAGNOSIS]->(c)
        """
        
        tx.run(query,
               encounterId=encounter_id,
               diseaseName=disease_name,
               diseaseCode=disease_code)

def import_examinations_from_encounter(tx, encounter_id, examinations_list):
    """Imports examination reports and findings for an encounter."""
    for exam in examinations_list:
        report_id = exam.get('reportId')
        if not report_id:
            logger.debug(f"Skipping examination for encounter {encounter_id} due to missing reportId. Record: {exam}")
            continue
            
        exam_query = """
        MATCH (e:Encounter {encounterId: $encounterId})
        MERGE (ex:Examination {reportId: $reportId})
        ON CREATE SET 
            ex.timestamp = $timestamp,
            ex.fullReport = $fullReport
        MERGE (e)-[:HAD_EXAMINATION]->(ex)
        """
        tx.run(exam_query,
               encounterId=encounter_id,
               reportId=report_id,
               timestamp=parse_datetime(exam.get('timestamp')),
               fullReport=exam.get('fullReport'))
        
        for finding in exam.get('findings', []):
            finding_result = finding.get('diagnosisResult')
            if not finding_result:
                logger.debug(f"Skipping examination finding for report {report_id} due to missing diagnosisResult. Record: {finding}")
                continue
                
            finding_query = """
            MATCH (ex:Examination {reportId: $reportId})
            MERGE (c:Condition {name: $findingResult})
            ON CREATE SET c.code = $findingCode
            MERGE (ex)-[r:HAS_FINDING]->(c)
            ON CREATE SET
                r.bodyPart = $bodyPart,
                r.diagnosisId = $diagnosisId
            """
            tx.run(finding_query,
                   reportId=report_id,
                   findingResult=finding_result,
                   findingCode=finding.get('diagnosisCode'),
                   bodyPart=finding.get('bodyPart'),
                   diagnosisId=finding.get('diagnosisId'))

def import_lab_tests_from_encounter(tx, encounter_id, lab_tests_list):
    """Imports lab test reports and items for an encounter."""
    for lab_test in lab_tests_list:
        report_id = lab_test.get('reportId')
        if not report_id:
            logger.debug(f"Skipping lab test for encounter {encounter_id} due to missing reportId. Record: {lab_test}")
            continue

        report_query = """
        MATCH (e:Encounter {encounterId: $encounterId})
        MERGE (ltr:LabTestReport {reportId: $reportId})
        MERGE (e)-[:HAD_LAB_TEST]->(ltr)
        """
        tx.run(report_query, encounterId=encounter_id, reportId=report_id)

        for item in lab_test.get('items', []):
            item_name = item.get('labtestIndexName', '').strip()
            test_id = item.get('testId')
            if not item_name or not test_id:
                logger.debug(f"Skipping lab test item for report {report_id} due to missing labtestIndexName or testId. Record: {item}")
                continue
            
            item_query = """
            MATCH (ltr:LabTestReport {reportId: $reportId})
            MERGE (li:LabTestItem {name: $itemName})
            ON CREATE SET li.code = $itemCode
            MERGE (ltr)-[r:HAS_ITEM {testId: $testId}]->(li)
            SET
                r.value = $value,
                r.textValue = $textValue,
                r.unit = $unit,
                r.referenceRange = $referenceRange,
                r.interpretation = $interpretation,
                r.timestamp = $timestamp
            """
            tx.run(item_query,
                   reportId=report_id,
                   itemName=item_name,
                   itemCode=item.get('labtestIndexCode', '').strip(),
                   testId=test_id,
                   value=item.get('value'),
                   textValue=item.get('textValue'),
                   unit=item.get('unit'),
                   referenceRange=item.get('referenceRange'),
                   interpretation=item.get('interpretation'),
                   timestamp=parse_datetime(item.get('timestamp'))
                  )

def import_allergies(tx, patient_id, allergy_list):
    """Imports allergy information for a patient."""
    for item in allergy_list:
        allergen_name = item.get('allergen')
        if not allergen_name or allergen_name == '无':
            logger.debug(f"Skipping allergy for patient {patient_id} due to missing or '无' allergen. Record: {item}")
            continue

        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (a:Allergen {name: $allergen})
        MERGE (p)-[r:HAS_ALLERGY_TO]->(a)
        ON CREATE SET
            r.allergyId = $allergyId,
            r.allergenType = $allergenType,
            r.reaction = $reaction,
            r.reactionType = $reactionType,
            r.recordedAt = $recordedAt
        """
        tx.run(query,
               patientId=patient_id,
               allergyId=item.get('allergyId'),
               allergen=allergen_name,
               allergenType=item.get('allergenType'),
               reaction=item.get('reaction'),
               reactionType=item.get('reactionType'),
               recordedAt=parse_datetime(item.get('recordedAt')))

def import_family_history(tx, patient_id, family_history_list):
    """Imports family medical history for a patient."""
    for item in family_history_list:
        relative_disease = item.get('relativeDisease')
        if not relative_disease or relative_disease == '不详':
            logger.debug(f"Skipping family history for patient {patient_id}: relativeDisease is missing or '不详'. Record: {item}")
            continue

        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (c:Condition {name: $relativeDisease})
        MERGE (p)-[r:HAS_FAMILY_HISTORY]->(c)
        ON CREATE SET
            r.relationship = $relationship,
            r.onsetAge = $onsetAge,
            r.recordedAt = $recordedAt
        """
        tx.run(query,
               patientId=patient_id,
               relativeDisease=relative_disease,
               relationship=item.get('relativeRelationship'),
               onsetAge=item.get('onsetAge'),
               recordedAt=parse_datetime(item.get('recordedAt')))

def import_past_surgeries(tx, patient_id, past_surgeries_list):
    for item in past_surgeries_list:
        surgery_name = item.get('surgeryName')
        if not surgery_name:
            logger.debug(f"Skipping surgery record for patient {patient_id} due to missing surgeryName. Record: {item}")
            continue
        
        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (e:PastMedicalEvent:Surgery {name: $name})
        ON CREATE SET 
            e.date = $date,
            e.bodySite = $bodySite,
            e.code = $code
        MERGE (p)-[:HAD_SURGERY]->(e)
        """
        tx.run(query, 
               patientId=patient_id,
               name=surgery_name,
               date=parse_datetime(item.get('surgeryDate')),
               bodySite=item.get('bodySite'),
               code=item.get('surgeryCode'))

def import_past_traumas(tx, patient_id, past_traumas_list):
    for item in past_traumas_list:
        body_site = item.get('bodySite')
        trauma_type = item.get('traumaType')
        if not body_site or not trauma_type:
            logger.debug(f"Skipping trauma record for patient {patient_id} due to missing bodySite or traumaType. Record: {item}")
            continue
        
        name = f"{body_site} {trauma_type}"
        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (e:PastMedicalEvent:Trauma {name: $name})
        ON CREATE SET
            e.date = $date,
            e.severity = $severity,
            e.healed = $healed,
            e.traumaId = $traumaId
        MERGE (p)-[:HAD_TRAUMA]->(e)
        """
        tx.run(query,
               patientId=patient_id,
               name=name,
               date=parse_datetime(item.get('traumasDate')),
               severity=item.get('severity'),
               healed=item.get('healed'),
               traumaId=item.get('pastTraumasId'))

def import_past_blood_transfusions(tx, patient_id, past_blood_transfusions_list):
    for item in past_blood_transfusions_list:
        transfusion_date = item.get('bloodTransfusionsDate')
        volume = item.get('volumeMl')
        if not transfusion_date and not volume:
            logger.debug(f"Skipping empty blood transfusion record for patient {patient_id}. Record: {item}")
            continue
        
        name = f"输血 {volume or ''}ml"
        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (e:PastMedicalEvent:BloodTransfusion {name: $name, date: $date})
        ON CREATE SET
            e.volumeMl = $volume,
            e.address = $address,
            e.transfusionId = $transfusionId
        MERGE (p)-[:HAD_BLOOD_TRANSFUSION]->(e)
        """
        tx.run(query,
               patientId=patient_id,
               name=name,
               date=parse_datetime(transfusion_date),
               volume=volume,
               address=item.get('bloodTransfusionsAddress'),
               transfusionId=item.get('pastBloodTransfusionsId'))

def import_past_vaccinations(tx, patient_id, past_vaccinations_list):
    for item in past_vaccinations_list:
        vaccine_name = item.get('vaccineName')
        if not vaccine_name:
            logger.debug(f"Skipping vaccination record for patient {patient_id} due to missing vaccineName. Record: {item}")
            continue

        vaccine_date = item.get('vaccineDate')
        parsed_date = parse_datetime(vaccine_date)
        
        # 创建一个组合唯一标识符，包含所有重要信息
        # 使用疫苗名称 + 日期字符串 + 剂次 + 患者ID 确保唯一性
        dose_number = item.get('doseNumber') or ''
        unique_id = f"{patient_id}_{vaccine_name}_{vaccine_date or 'no_date'}_{dose_number}"
        
        query = """
        MATCH (p:Patient {patientId: $patientId})
        MERGE (e:PastMedicalEvent:Vaccination {uniqueId: $uniqueId})
        ON CREATE SET
            e.name = $name,
            e.date = $date,
            e.doseNumber = $doseNumber,
            e.manufacturer = $manufacturer,
            e.lotNumber = $lotNumber,
            e.vaccineCode = $vaccineCode
        ON MATCH SET
            e.name = $name,
            e.date = $date,
            e.doseNumber = $doseNumber,
            e.manufacturer = $manufacturer,
            e.lotNumber = $lotNumber,
            e.vaccineCode = $vaccineCode
        MERGE (p)-[:HAD_VACCINATION]->(e)
        """
        tx.run(query,
               patientId=patient_id,
               uniqueId=unique_id,
               name=vaccine_name,
               date=parsed_date,
               doseNumber=dose_number,
               manufacturer=item.get('manufacturer'),
               lotNumber=item.get('lotNumber'),
               vaccineCode=item.get('vaccineCode'))

def import_personal_history(tx, patient_id, data):
    # Smoking History
    smoking_history = data.get('personalSmokingHistoryList', [])
    if smoking_history:
        status = smoking_history[0].get('status')
        if status:
            import_lifestyle_fact(tx, patient_id, 'SmokingStatus', status, 'personalSmokingHistory', smoking_history[0])

    # Alcohol History
    alcohol_history = data.get('personalAlcoholHistoryList', [])
    if alcohol_history:
        frequency = alcohol_history[0].get('frequency')
        if frequency:
            import_lifestyle_fact(tx, patient_id, 'AlcoholFrequency', frequency, 'personalAlcoholHistory', alcohol_history[0])

    # Physical Traits
    physical_traits = data.get('physicalTraitsList', [])
    if physical_traits:
        bmi = physical_traits[0].get('bmi')
        if bmi:
            import_lifestyle_fact(tx, patient_id, 'BMI', bmi, 'physicalTraits', physical_traits[0])

    # Diet Habits
    diet_habits = data.get('dietHabitsList', [])
    if diet_habits:
        diet_type = diet_habits[0].get('dietType')
        flavor_type = diet_habits[0].get('flavorType')
        if diet_type:
            import_lifestyle_fact(tx, patient_id, 'DietType', diet_type, 'dietHabits', diet_habits[0])
        if flavor_type:
            import_lifestyle_fact(tx, patient_id, 'FlavorPreference', flavor_type, 'dietHabits', diet_habits[0])
            
    # Sleep Assessment
    sleep_assessment = data.get('sleepAssessmentList', [])
    if sleep_assessment:
        duration = sleep_assessment[0].get('sleepDuration')
        quality = sleep_assessment[0].get('sleepQuality')
        if duration:
             import_lifestyle_fact(tx, patient_id, 'SleepDuration', duration, 'sleepAssessment', sleep_assessment[0])
        if quality:
             import_lifestyle_fact(tx, patient_id, 'SleepQuality', quality, 'sleepAssessment', sleep_assessment[0])


def import_lifestyle_fact(tx, patient_id, fact_type, fact_value, source, record_data):
    """Generic function to import a single lifestyle fact."""
    if not fact_value:
        logger.debug(f"Skipping lifestyle fact '{fact_type}' for patient {patient_id} due to empty value.")
        return

    query = """
    MATCH (p:Patient {patientId: $patientId})
    MERGE (f:LifestyleFact {type: $type, value: $value})
    MERGE (p)-[r:HAS_LIFESTYLE_FACT]->(f)
    ON CREATE SET
        r.recordedAt = $recordedAt,
        r.source = $source
    """
    
    tx.run(query,
           patientId=patient_id,
           type=fact_type,
           value=str(fact_value),
           recordedAt=parse_datetime(record_data.get('createdAt')),
           source=source,
          )

def import_family_members(tx, main_patient_id, family_members_list):
    """
    导入家族成员信息 (V2 - 使用idType和idValue作为唯一标识)。
    【警告】: 此版本仍会将“亲生父母”和“岳父母”不加区分地统一处理为 PARENT_OF 关系。
    """
    # 关系代码到标准化类型的映射字典 (白名单)
    RELATIONSHIP_MAP = {
        '1': 'SPOUSE',
        '2': 'CHILD',
        '4': 'PARENT',
    }

    # 性别代码映射
    GENDER_MAP = { '1': 'Male', '2': 'Female' }

    for member in family_members_list:
        rel_code = str(member.get('relationship'))
        # 核心改动：使用idType和idValue作为主要标识符
        id_type = member.get('idType')
        id_value = member.get('idValue')

        # 如果唯一标识(证件类型+证件号)缺失，则跳过此记录
        if not id_type or not id_value:
            # 可以在这里添加日志 logger.debug(...)
            continue

        if rel_code not in RELATIONSHIP_MAP:
            continue
            
        rel_type = RELATIONSHIP_MAP[rel_code]
        
        # 准备要设置到节点上的所有属性
        properties_to_set = {
            "name": member.get("name"),
            "gender": GENDER_MAP.get(str(member.get("gender"))),
            "birthDate": member.get("birthDate"),
            "patientId": member.get("patientId") # 包含patientId，即使它可能为null
        }
        # 过滤掉值为None的属性，避免覆盖已有数据为null
        properties_to_set = {k: v for k, v in properties_to_set.items() if v is not None}
        
        # 统一的Cypher查询，先处理节点，再处理关系
        # 核心改动：MERGE (relative:Patient {idType: $idType, idValue: $idValue})
        query = """
        // 1. 使用 idType 和 idValue 查找或创建家族成员节点
        MERGE (relative:Patient {idType: $idType, idValue: $idValue})
        // 2. 无论创建还是匹配，都用最新的信息更新其属性
        SET relative += $properties

        // 3. 查找主患者节点
        WITH relative
        MATCH (main:Patient {patientId: $mainPatientId})

        // 4. 根据关系类型，创建对应的关系
        """
        
        # 根据关系类型附加关系创建的Cypher子句
        relationship_cypher = ""
        if rel_type == 'SPOUSE':
            relationship_cypher = """
                MERGE (main)-[r:SPOUSE_OF]-(relative)
                SET r.relationshipName = $relName
            """
        elif rel_type == 'CHILD':
            relationship_cypher = """
                MERGE (main)-[r:PARENT_OF]->(relative)
                SET r.relationshipName = $relName
            """
        elif rel_type == 'PARENT':
            relationship_cypher = """
                MERGE (main)<-[r:PARENT_OF]-(relative)
                SET r.relationshipName = $relName
            """
        
        # 只有在关系类型有效时才执行查询
        if relationship_cypher:
            final_query = query + relationship_cypher
            params = {
                "mainPatientId": str(main_patient_id),
                "idType": id_type,
                "idValue": id_value,
                "properties": properties_to_set,
                "relName": member.get("relationshipName", rel_type)
            }
            tx.run(final_query, **params)