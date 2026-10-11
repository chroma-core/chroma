import importlib.util
import os
import sys
import unittest

# Load module directly
file_path = os.path.join(
    os.path.dirname(__file__),
    "../telemetry/production_debt.py",
)
spec = importlib.util.spec_from_file_location("chroma_production_debt", file_path)
production_debt_mod = importlib.util.module_from_spec(spec)
sys.modules["chroma_production_debt"] = production_debt_mod
spec.loader.exec_module(production_debt_mod)

ProductionDebtEvaluator = production_debt_mod.ProductionDebtEvaluator
TechnicalDueDiligenceLedger = production_debt_mod.TechnicalDueDiligenceLedger
GENESIS_HASH = production_debt_mod.GENESIS_HASH


class TestProductionDebtEvaluator(unittest.TestCase):
    def setUp(self) -> None:
        self.evaluator = ProductionDebtEvaluator(
            never_equate_intent_to_approval=True,
            max_acceptable_rdi=10.0,
        )

    def test_clean_vector_collection_passes_readiness(self) -> None:
        report = self.evaluator.evaluate_query_and_index(
            collection_name="enterprise_kb",
            allocated_memory_mb=100.0,
            utilized_memory_mb=98.0,
            latency_ms=18.5,
            vector_drift_score=0.01,
            un_gated_mutations=0,
        )
        self.assertTrue(report.is_production_ready)
        self.assertLessEqual(report.rdi_score, 10.0)
        self.assertEqual(len(report.critical_smells), 0)
        self.assertTrue(bool(report.receipt_hash))

    def test_degraded_vector_collection_fails_rdi(self) -> None:
        report = self.evaluator.evaluate_query_and_index(
            collection_name="legacy_rag_index",
            allocated_memory_mb=500.0,  # High memory inflation
            utilized_memory_mb=150.0,
            latency_ms=180.0,  # High latency
            vector_drift_score=0.45,  # High vector drift
            un_gated_mutations=3,  # Un-gated collection mutations
        )
        self.assertFalse(report.is_production_ready)
        self.assertGreater(report.rdi_score, 50.0)
        self.assertIn("HIGH_MEMORY_INDEX_INFLATION_3.33X", report.critical_smells)
        self.assertIn("HIGH_RETRIEVAL_LATENCY_180.0MS", report.critical_smells)
        self.assertIn("DETECTED_HIGH_VECTOR_DRIFT_0.450", report.critical_smells)
        self.assertIn("DETECTED_3_UNGATED_VECTOR_MUTATIONS", report.critical_smells)

    def test_cryptographic_ledger_integrity(self) -> None:
        self.evaluator.evaluate_query_and_index("col-1")
        self.evaluator.evaluate_query_and_index("col-2")
        self.evaluator.evaluate_query_and_index("col-3")

        entries = self.evaluator.ledger.get_ledger_entries()
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["prev_hash"], GENESIS_HASH)
        self.assertEqual(entries[1]["prev_hash"], entries[0]["curr_hash"])
        self.assertEqual(entries[2]["prev_hash"], entries[1]["curr_hash"])
        self.assertTrue(self.evaluator.ledger.verify_ledger_integrity())


if __name__ == "__main__":
    unittest.main()
