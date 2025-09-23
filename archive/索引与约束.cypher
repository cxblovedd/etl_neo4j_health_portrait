// 1. 患者节点约束和索引
// patientId必须唯一
CREATE CONSTRAINT patient_id_unique IF NOT EXISTS
FOR (p:Patient) REQUIRE p.patientId IS UNIQUE;
// 为name创建索引以加快查询
CREATE INDEX patient_name IF NOT EXISTS
FOR (p:Patient) ON (p.name);

// 2. 疾病节点约束和索引
// icdCode应该唯一（如果存在）
CREATE CONSTRAINT condition_icd_unique IF NOT EXISTS
FOR (c:Condition) REQUIRE c.icdCode IS UNIQUE;
// 为name和type组合创建索引
CREATE INDEX condition_name_type IF NOT EXISTS
FOR (c:Condition) ON (c.name, c.type);

// 3. 就诊记录约束
CREATE CONSTRAINT encounter_id_unique IF NOT EXISTS
FOR (e:Encounter) REQUIRE e.encounterId IS UNIQUE;
// 为encounterDate创建索引以支持时间范围查询
CREATE INDEX encounter_date IF NOT EXISTS
FOR (e:Encounter) ON (e.encounterDate);

// 4. 实验室检查结果约束
CREATE CONSTRAINT lab_result_id_unique IF NOT EXISTS
FOR (lr:LabResult) REQUIRE lr.resultId IS UNIQUE;
// 为testCode创建索引
CREATE INDEX lab_test_code IF NOT EXISTS
FOR (lr:LabResult) ON (lr.testCode);

// 5. 生命体征记录约束
CREATE CONSTRAINT vital_sign_id_unique IF NOT EXISTS
FOR (vs:VitalSign) REQUIRE vs.vitalId IS UNIQUE;
// 为type和timestamp创建组合索引
CREATE INDEX vital_sign_type_time IF NOT EXISTS
FOR (vs:VitalSign) ON (vs.type, vs.timestamp);

// 6. 检查记录约束
CREATE CONSTRAINT examination_id_unique IF NOT EXISTS
FOR (ex:Examination) REQUIRE ex.examId IS UNIQUE;

// 7. 检查发现约束
CREATE CONSTRAINT exam_finding_id_unique IF NOT EXISTS
FOR (ef:ExaminationFinding) REQUIRE ef.findingId IS UNIQUE;

// 8. 个人史记录索引
CREATE INDEX personal_history_type IF NOT EXISTS
FOR (ph:PersonalHistoryItem) ON (ph.type);

// 9. 家族史记录索引
CREATE INDEX family_history_relative IF NOT EXISTS
FOR (fh:FamilyHistory) ON (fh.relative);

// 10. 过敏记录索引
CREATE INDEX allergy_allergen IF NOT EXISTS
FOR (a:Allergy) ON (a.allergen);