// 1. 找到一个“夫妻”对 (parent1 和 parent2)
MATCH (parent1:Patient)-[:SPOUSE_OF]-(parent2:Patient)

// 2. 找到其中一方(parent1)已知的子女(child)
MATCH (parent1)-[:PARENT_OF]->(child:Patient)

// 3. 关键条件：确保另一方(parent2)与该子女的亲子关系尚不存在
WHERE NOT EXISTS((parent2)-[:PARENT_OF]->(child))

// 4. 创建这条缺失的、推断出来的亲子关系
MERGE (parent2)-[r:PARENT_OF]->(child)
  // 5. 【重要】在新创建的关系上打上“推断”标签，以区分原始数据和推断数据
  ON CREATE SET 
    r.inferred = true, 
    r.relationshipName = '父母(推断)'

// 6. 返回本次操作新建了多少条关系，以便确认
RETURN count(r) AS newRelationshipsCreated