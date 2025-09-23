// 匹配张大爷的Patient节点
MATCH (p:Patient {name: '张大爷'})

// 删除所有关联节点及其关系
CALL {
  WITH p
  
  // 收集所有与患者直接相关的节点
  OPTIONAL MATCH (p)-[r1]-(n1)
  
  // 收集与这些节点相关的二级节点
  OPTIONAL MATCH (n1)-[r2]-(n2)
  WHERE n2 <> p
  
  // 收集所有需要删除的节点和关系
  WITH p, collect(DISTINCT n1) AS firstLevelNodes, collect(DISTINCT n2) AS secondLevelNodes
  
  // 删除二级关系和节点
  FOREACH (node IN secondLevelNodes | DETACH DELETE node)
  
  // 删除一级节点
  FOREACH (node IN firstLevelNodes | DETACH DELETE node)
  
  // 最后删除患者节点
  DELETE p
  
  RETURN count(p) AS deletedPatient
}

RETURN 'Successfully deleted patient data' AS result;