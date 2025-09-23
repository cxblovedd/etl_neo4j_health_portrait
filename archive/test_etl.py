import unittest
import json
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from etl_patient_to_neo4j import process_patient_json, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE

class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up Neo4j connection and test data."""
        cls.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        # Test data for a basic patient
        cls.test_patient = {
            "idCard": "TEST123456",
            "patientId": "P123",
            "name": "测试患者",
            "birthDate": "1990-01-01",
            "gender": "男",
            "labResults": [
                {
                    "testCode": "CHEM0025",
                    "testName": "甘油三酯",
                    "value": "2.5",
                    "unit": "mmol/L",
                    "referenceRange": "0.45-1.70",
                    "interpretation": "偏高",
                    "timestamp": "2023-01-01T10:00:00Z"
                }
            ],
            "conditions": [
                {
                    "conditionName": "高血压",
                    "type": "Diagnosis",
                    "status": "Active",
                    "diagnosedDate": "2023-01-01"
                }
            ]
        }

    @classmethod
    def tearDownClass(cls):
        """Clean up test data and close connection."""
        with cls.driver.session(database=NEO4J_DATABASE) as session:
            # Clean up test data
            session.run("MATCH (p:Patient {idCard: $idCard}) DETACH DELETE p", 
                       idCard=cls.test_patient["idCard"])
            session.run("MATCH (lt:LabTest {code: $code}) DETACH DELETE lt",
                       code="CHEM0025")
            session.run("MATCH (c:Condition {name: $name}) DETACH DELETE c",
                       name="高血压")
        cls.driver.close()

    def setUp(self):
        """Set up for each test."""
        # Clean any existing test data before each test
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run("MATCH (p:Patient {idCard: $idCard}) DETACH DELETE p", 
                       idCard=self.test_patient["idCard"])

    def test_patient_core_import(self):
        """Test basic patient information import."""
        process_patient_json(self.test_patient)
        
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(
                "MATCH (p:Patient {idCard: $idCard}) RETURN p",
                idCard=self.test_patient["idCard"]
            )
            patient = result.single()
            
            self.assertIsNotNone(patient, "Patient node should be created")
            self.assertEqual(
                patient["p"]["name"], 
                self.test_patient["name"],
                "Patient name should match"
            )

    def test_lab_results_import(self):
        """Test lab results import and relationship creation."""
        process_patient_json(self.test_patient)
        
        with self.driver.session(database=NEO4J_DATABASE) as session:
            # Check LabTest node
            result = session.run("""
                MATCH (lt:LabTest {code: $testCode})
                RETURN lt
                """,
                testCode=self.test_patient["labResults"][0]["testCode"]
            )
            lab_test = result.single()
            self.assertIsNotNone(lab_test, "LabTest node should be created")
            
            # Check LabResult node and relationships
            result = session.run("""
                MATCH (p:Patient {idCard: $idCard})-[r:HAS_LAB_RESULT]->(lr:LabResult)-[:RESULT_OF]->(lt:LabTest)
                WHERE lt.code = $testCode
                RETURN lr, r
                """,
                idCard=self.test_patient["idCard"],
                testCode=self.test_patient["labResults"][0]["testCode"]
            )
            record = result.single()
            self.assertIsNotNone(record, "LabResult and relationships should exist")
            
            # Check high triglycerides condition link
            result = session.run("""
                MATCH (p:Patient {idCard: $idCard})-[:HAS_CONDITION]->(c:Condition {name: '高血脂风险'})
                RETURN c
                """,
                idCard=self.test_patient["idCard"]
            )
            condition = result.single()
            self.assertIsNotNone(condition, "High lipids risk condition should be created")

    def test_conditions_import(self):
        """Test conditions import and relationship creation."""
        process_patient_json(self.test_patient)
        
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run("""
                MATCH (p:Patient {idCard: $idCard})-[r:HAS_CONDITION]->(c:Condition)
                WHERE c.name = $conditionName
                RETURN c, r
                """,
                idCard=self.test_patient["idCard"],
                conditionName=self.test_patient["conditions"][0]["conditionName"]
            )
            record = result.single()
            self.assertIsNotNone(record, "Condition and relationship should exist")
            self.assertEqual(
                record["c"]["type"],
                self.test_patient["conditions"][0]["type"],
                "Condition type should match"
            )

    def test_error_handling(self):
        """Test error handling for invalid data."""
        invalid_patient = {
            "idCard": "TEST123456",
            "labResults": [{
                "testCode": None,
                "testName": None,
                "value": "invalid"
            }]
        }
        
        # Should not raise exception but log error
        process_patient_json(invalid_patient)
        
        with self.driver.session(database=NEO4J_DATABASE) as session:
            # Verify no lab result was created
            result = session.run("""
                MATCH (p:Patient {idCard: $idCard})-[:HAS_LAB_RESULT]->(lr:LabResult)
                RETURN count(lr) as count
                """,
                idCard=invalid_patient["idCard"]
            )
            count = result.single()["count"]
            self.assertEqual(count, 0, "No lab results should be created for invalid data")

if __name__ == '__main__':
    unittest.main()