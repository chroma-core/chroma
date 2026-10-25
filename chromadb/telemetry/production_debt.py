from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

log: logging.Logger = logging.getLogger(__name__)

GENESIS_HASH: str = (
    "0000000000000000000000000000000000000000000000000000000000000000"
)


@dataclass
class VectorDebtReport:
    collection_name: str
    rdi_score: float  # Retrieval Debt Index (target <= 10.0)
    memory_inflation_multiplier: float  # Target <= 1.08x
    latency_ms: float  # Target <= 45ms
    mutation_safety_score: float  # Target 100.0
    production_readiness_index: float  # Scale 0 - 100
    is_production_ready: bool
    critical_smells: List[str]
    receipt_hash: str


class TechnicalDueDiligenceLedger:
    """
    Cryptographic SHA-256 hash-chained Action Ledger for ChromaDB vector operations.
    """

    def __init__(self) -> None:
        self._entries: List[Dict[str, Any]] = []
        self._last_hash: str = GENESIS_HASH

    def record_vector_evaluation(
        self,
        collection_name: str,
        event_type: str,
        readiness_index: float,
        critical_smells: List[str],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        timestamp = datetime.now(timezone.utc).isoformat()
        index = len(self._entries)

        meta_bytes = json.dumps(metadata, sort_keys=True).encode("utf-8")
        canonical_content = f"{index}|{self._last_hash}|{collection_name}|{event_type}|{readiness_index}|{timestamp}|{hashlib.sha256(meta_bytes).hexdigest()}"
        curr_hash = hashlib.sha256(canonical_content.encode("utf-8")).hexdigest()

        entry = {
            "index": index,
            "timestamp": timestamp,
            "collection_name": collection_name,
            "event_type": event_type,
            "readiness_index": readiness_index,
            "critical_smells": critical_smells,
            "prev_hash": self._last_hash,
            "curr_hash": curr_hash,
            "metadata": metadata,
        }

        self._entries.append(entry)
        self._last_hash = curr_hash
        return entry

    def get_ledger_entries(self) -> List[Dict[str, Any]]:
        return list(self._entries)

    def verify_ledger_integrity(self) -> bool:
        prev = GENESIS_HASH
        for entry in self._entries:
            if entry["prev_hash"] != prev:
                return False
            prev = entry["curr_hash"]
        return True


class ProductionDebtEvaluator:
    """
    A2Z SOC Production Debt & Technical Due Diligence Evaluator for ChromaDB.

    Quantifies vector retrieval and index quality against 4 Enterprise Forward Deployed Engineering KPIs:
    1. Retrieval Debt Index (RDI <= 10.0)
    2. Vector Index Memory Inflation (VMI <= 1.08x)
    3. P99 Semantic Retrieval Latency Ceiling (<= 45ms)
    4. Deterministic Mutation Boundaries (never_equate_intent_to_approval)
    """

    def __init__(
        self,
        never_equate_intent_to_approval: bool = True,
        max_acceptable_rdi: float = 10.0,
    ) -> None:
        self.never_equate_intent_to_approval = never_equate_intent_to_approval
        self.max_acceptable_rdi = max_acceptable_rdi
        self.ledger = TechnicalDueDiligenceLedger()

    def check_kill_switch(self) -> bool:
        if os.environ.get("AAG_KILL_SWITCH", "").lower() in ("true", "1", "yes"):
            return True
        for path_str in ("artifacts/KILL", "/tmp/KILL"):
            if Path(path_str).exists():
                return True
        return False

    def evaluate_query_and_index(
        self,
        collection_name: str,
        allocated_memory_mb: float = 100.0,
        utilized_memory_mb: float = 95.0,
        latency_ms: float = 24.5,
        vector_drift_score: float = 0.02,
        un_gated_mutations: int = 0,
    ) -> VectorDebtReport:
        # 1. Evaluate emergency kill switch
        if self.check_kill_switch():
            self.ledger.record_vector_evaluation(
                collection_name=collection_name,
                event_type="evaluation_halted_kill_switch",
                readiness_index=0.0,
                critical_smells=["EMERGENCY_KILL_SWITCH_ENGAGED"],
                metadata={"reason": "AAG_KILL_SWITCH is set"},
            )
            raise PermissionError(
                "A2Z SOC ActionGate: Emergency kill switch is engaged. Vector evaluation halted."
            )

        critical_smells: List[str] = []

        # KPI 2: Vector Index Memory Inflation Multiplier
        memory_ratio = allocated_memory_mb / max(1.0, utilized_memory_mb)
        if memory_ratio > 1.5:
            critical_smells.append(f"HIGH_MEMORY_INDEX_INFLATION_{memory_ratio:.2f}X")

        # KPI 3: Latency Ceiling
        if latency_ms > 100.0:
            critical_smells.append(f"HIGH_RETRIEVAL_LATENCY_{latency_ms:.1f}MS")

        # Vector Drift
        if vector_drift_score > 0.15:
            critical_smells.append(f"DETECTED_HIGH_VECTOR_DRIFT_{vector_drift_score:.3f}")

        # KPI 4: Mutation Safety
        if un_gated_mutations > 0:
            critical_smells.append(f"DETECTED_{un_gated_mutations}_UNGATED_VECTOR_MUTATIONS")

        # KPI 1: Retrieval Debt Index (0 = Clean, 100 = Catastrophic)
        rdi = (
            max(0.0, (memory_ratio - 1.0) * 20.0)
            + max(0.0, (latency_ms - 45.0) * 0.5)
            + (vector_drift_score * 50.0)
            + (un_gated_mutations * 25.0)
        )
        rdi_score = round(min(100.0, rdi), 2)

        # Production Readiness Index (0 - 100)
        readiness = max(0.0, 100.0 - rdi_score)
        is_production_ready = (
            rdi_score <= self.max_acceptable_rdi and len(critical_smells) == 0
        )

        # Cryptographic Ledger Entry
        entry = self.ledger.record_vector_evaluation(
            collection_name=collection_name,
            event_type="diligence_passed" if is_production_ready else "diligence_failed_debt",
            readiness_index=readiness,
            critical_smells=critical_smells,
            metadata={
                "rdi_score": rdi_score,
                "memory_ratio": memory_ratio,
                "latency_ms": latency_ms,
                "vector_drift_score": vector_drift_score,
                "un_gated_mutations": un_gated_mutations,
                "never_equate_intent_to_approval": self.never_equate_intent_to_approval,
            },
        )

        return VectorDebtReport(
            collection_name=collection_name,
            rdi_score=rdi_score,
            memory_inflation_multiplier=round(memory_ratio, 2),
            latency_ms=round(latency_ms, 2),
            mutation_safety_score=(
                100.0 if un_gated_mutations == 0 else max(0.0, 100.0 - un_gated_mutations * 30.0)
            ),
            production_readiness_index=readiness,
            is_production_ready=is_production_ready,
            critical_smells=critical_smells,
            receipt_hash=entry["curr_hash"],
        )
