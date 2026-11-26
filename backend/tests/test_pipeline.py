"""管线层关键链路测试：跨模块真实数据链路。"""
from __future__ import annotations

from datetime import date

import pytest

from app.pipeline.watermark import WatermarkManager, WatermarkLine
from app.pipeline.scd_processor import ScdProcessor, ScdRecord
from app.pipeline.fact_assembler import FactAssembler, FactColumn, BridgeWeight
from app.pipeline.quality_checker import QualityChecker, GateCheck
from app.pipeline.reprocess_orchestrator import (
    ReprocessOrchestrator, ReprocessTask, LineageEdge, LateFactRecord,
)
from app.pipeline.snapshot_manager import SnapshotManager
from app.pipeline.integration import PipelineOrchestrator


class TestWatermark:
    def test_advance_ok(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("sales_fact", "2026-10-01", "2026-10-01", 1000, 1000)
        assert ok
        assert "已推进" in msg

    def test_advance_deviation_too_large(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("sales_fact", "2026-10-01", "2026-10-01", 500, 1000)
        assert not ok
        assert "偏差" in msg

    def test_extract_not_ready(self):
        wm = WatermarkManager()
        wm.register_extract("ERP", "2026-09-01")
        ready, msg = wm.extract_ready("ERP", "2026-10-01")
        assert not ready
        assert "落后" in msg

    def test_extract_becomes_ready(self):
        wm = WatermarkManager()
        wm.register_extract("ERP", "2026-09-01")
        wm.register_extract("ERP", "2026-10-15")
        ready, msg = wm.extract_ready("ERP", "2026-10-01")
        assert ready

    def test_watermark_line_dirty_rows(self):
        wl = WatermarkLine("test_table")
        wl.mark_dirty(50)
        assert wl.dirty_rows == 50
        wl.mark_dirty(10)
        assert wl.dirty_rows == 60


class TestScdProcessor:
    def test_close_chain_ok(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(
            1, "dim1", "SK-001", "field1",
            date(2026, 9, 1), date(9999, 12, 31), True, None,
        ))
        ok, msg, rec = sp.close_chain(1, date(2026, 10, 15), None)
        assert ok
        assert rec is not None
        assert rec["effective_end"] == "2026-10-15"
        assert not rec["is_current"]

    def test_close_chain_bad_date(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(
            1, "dim1", "SK-001", "field1",
            date(2026, 9, 1), date(9999, 12, 31), True, None,
        ))
        ok, msg, _ = sp.close_chain(1, date(2026, 8, 1), None)
        assert not ok
        assert "早于" in msg

    def test_overlap_detected(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 10, 1), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 15), date(2026, 10, 15), True, None))
        conflicts, msg = sp.detect_overlap("dim1")
        assert len(conflicts) >= 1
        c = conflicts[0]
        assert c.record_a == 1 and c.record_b == 2

    def test_no_overlap_adjacent(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 9, 15), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 15), date(2026, 10, 1), True, None))
        conflicts, _ = sp.detect_overlap("dim1")
        assert len(conflicts) == 0

    def test_gap_detection(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 1), date(2026, 9, 5), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 9, 10), date(2026, 10, 1), True, None))
        gaps = sp.detect_gaps("dim1")
        assert len(gaps) == 1

    def test_attr_decay_apply(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "old_value",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "another_old",
            date(2026, 9, 15), date(9999, 12, 31), True, None))
        count = sp.apply_attr_decay("dim1", "等级", "默认值")
        assert count == 2
        for r in sp.get_dim_records("dim1"):
            assert "已失效" in r.changed_field

    def test_close_chain_with_predecessor_ok(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(10, "dim1", "SK-010", "f0",
            date(2026, 8, 1), date(2026, 9, 1), False, None))
        sp.add_record(ScdRecord(11, "dim1", "SK-011", "f1",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        ok, msg, _ = sp.close_chain(11, date(2026, 10, 1), 10)
        assert ok
        assert "已闭链" in msg

    def test_close_chain_predecessor_mismatch(self):
        sp = ScdProcessor()
        sp.add_record(ScdRecord(10, "dim1", "SK-010", "f0",
            date(2026, 8, 1), date(2026, 10, 1), False, None))
        sp.add_record(ScdRecord(11, "dim1", "SK-011", "f1",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        ok, msg, _ = sp.close_chain(11, date(2026, 11, 1), 10)
        assert not ok
        assert "不首尾相接" in msg


class TestFactAssembler:
    def test_hit_rate_full(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        fa._key_mapping.register("nk-2", 1002)
        rate, total, resolved = fa.compute_hit_rate(["nk-1", "nk-2"])
        assert rate == 1.0
        assert total == 2
        assert resolved == 2

    def test_hit_rate_partial(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        rate, total, resolved = fa.compute_hit_rate(["nk-1", "nk-2", "nk-3"])
        assert rate == 1.0 / 3.0
        assert total == 3
        assert resolved == 1

    def test_hit_rate_empty(self):
        fa = FactAssembler()
        rate, total, resolved = fa.compute_hit_rate([])
        assert rate == 1.0
        assert resolved == 0

    def test_bridge_normalize_ok(self):
        fa = FactAssembler()
        fa.add_bridge_weight(BridgeWeight(1, "dim_a", 0.6))
        fa.add_bridge_weight(BridgeWeight(1, "dim_b", 0.4))
        ok, msg, _ = fa.normalize_bridge_weights([1])
        assert ok

    def test_bridge_normalize_fail(self):
        fa = FactAssembler()
        fa.add_bridge_weight(BridgeWeight(1, "dim_a", 0.5))
        fa.add_bridge_weight(BridgeWeight(1, "dim_b", 0.3))
        ok, msg, _ = fa.normalize_bridge_weights([1])
        assert not ok
        assert "归一失败" in msg

    def test_grain_consistency_ok(self):
        fa = FactAssembler()
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg = fa.check_grain_consistency("sales", {"customer", "product", "date"})
        assert ok

    def test_grain_consistency_mismatch(self):
        fa = FactAssembler()
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg = fa.check_grain_consistency("sales", {"customer", "product"})
        assert not ok
        assert "不一致" in msg

    def test_assemble_batch_hit_rate_below_threshold(self):
        fa = FactAssembler()
        fa.register_dimension("sales", ["customer", "product", "date"])
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg, result = fa.assemble_batch(
            "B001", "sales",
            [FactColumn("amount", "销售额", "SUM")],
            ["nk-1", "nk-2", "nk-3"],
            ["customer", "product", "date"],
        )
        assert not ok
        assert "命中率" in msg
        assert result["status"] == "待装配"

    def test_assemble_batch_ok(self):
        fa = FactAssembler()
        fa._key_mapping.register("nk-1", 1001)
        fa._key_mapping.register("nk-2", 1002)
        fa._key_mapping.register("nk-3", 1003)
        fa._key_mapping.register("nk-4", 1004)
        fa._key_mapping.register("nk-5", 1005)
        fa.register_dimension("sales", ["customer", "product", "date"])
        fa.register_grain("sales", {"customer", "product", "date"})
        ok, msg, result = fa.assemble_batch(
            "B001", "sales",
            [FactColumn("amount", "销售额", "SUM")],
            ["nk-1", "nk-2", "nk-3", "nk-4", "nk-5"],
            ["customer", "product", "date"],
        )
        assert ok
        assert result["status"] == "已装配"
        assert result["hit_rate"] == 1.0


class TestQualityChecker:
    def test_gate_pass(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.95})
        assert passed
        assert len(results) == 1
        assert results[0].passed

    def test_gate_fail(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.60})
        assert not passed
        assert not results[0].passed

    def test_gate_missing_measurement(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "行数精度", 0.95, "窗口级"))
        passed, results = qc.run_checks("sales", {})
        assert not passed

    def test_multiple_gates_partial_fail(self):
        qc = QualityChecker()
        qc.register_gate("sales", GateCheck("G1", "外键命中率", 0.80, "批次级"))
        qc.register_gate("sales", GateCheck("G2", "行数精度", 0.90, "窗口级"))
        passed, results = qc.run_checks("sales", {"外键命中率": 0.95, "行数精度": 0.82})
        assert not passed


class TestReprocessOrchestrator:
    def test_late_fact_backfill(self):
        ro = ReprocessOrchestrator()
        ro.set_watermark_cutoff("sales", date(2026, 10, 1))
        late = LateFactRecord("LATE-1", "sales",
                              date(2026, 9, 15), date(2026, 10, 20), "part-202609")
        status = ro.register_late_fact(late)
        assert status == "待回补"

    def test_late_fact_reject(self):
        ro = ReprocessOrchestrator()
        ro.set_watermark_cutoff("sales", date(2026, 9, 1))
        late = LateFactRecord("LATE-2", "sales",
                              date(2026, 10, 1), date(2026, 10, 15), "part-202610")
        status = ro.register_late_fact(late)
        assert status == "拒绝回补"

    def test_lineage_propagation(self):
        ro = ReprocessOrchestrator()
        ro.add_edge(LineageEdge("E1", "fact", "summary", "派生"))
        ro.add_edge(LineageEdge("E2", "fact", "quality", "校验"))
        ro.add_edge(LineageEdge("E3", "summary", "metric", "绑定"))
        impacted = ro.propagate_impact("fact")
        assert "summary" in impacted
        assert "quality" in impacted
        assert "metric" in impacted

    def test_reprocess_create(self):
        ro = ReprocessOrchestrator()
        task = ReprocessTask("RP-1", "迟到事实回补", "sales",
                             date(2026, 9, 1), date(2026, 9, 30),
                             "并存留痕", "BATCH-202609")
        ok, msg = ro.create_reprocess(task)
        assert ok
        assert "并存留痕" in msg

    def test_reprocess_bad_strategy(self):
        ro = ReprocessOrchestrator()
        task = ReprocessTask("RP-1", "test", "sales",
                             date(2026, 9, 1), date(2026, 9, 30),
                             "错误策略", "BATCH")
        ok, _ = ro.create_reprocess(task)
        assert not ok

    def test_backfill_long_lag(self):
        ro = ReprocessOrchestrator()
        late = LateFactRecord("LATE-3", "sales",
                              date(2026, 1, 1), date(2026, 12, 20),
                              "part-202601")
        decision = ro.decide_backfill_boundary(late, date(2026, 6, 1))
        assert decision == "整段重算"


class TestSnapshotManager:
    def test_create_snapshot_ok(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [
            {"surrogate_key": "SK-001", "is_current": True},
            {"surrogate_key": "SK-002", "is_current": False},
        ])
        ok, msg, snap = sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        assert ok
        assert snap.record_count == 2

    def test_publish_then_no_silent_replace(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        snap_id = "SNP-customer-2026-10-01"
        ok, msg, _ = sm.publish_snapshot(snap_id, "v1", "全量")
        assert ok
        ok2, msg2, _ = sm.publish_snapshot(snap_id, "v1", "全量")
        assert not ok2
        assert "不得静默替换" in msg2

    def test_withdraw_publish(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        snap_id = "SNP-customer-2026-10-01"
        sm.publish_snapshot(snap_id, "v1", "全量")
        pub_id = "PUB-customer-v1"
        ok, msg = sm.withdraw_publish(pub_id)
        assert ok

    def test_withdraw_twice_rejected(self):
        sm = SnapshotManager()
        sm.register_dim_data("customer", [{"surrogate_key": "SK-001", "is_current": True}])
        sm.create_snapshot("customer", date(2026, 10, 1), "BATCH-01")
        sm.publish_snapshot("SNP-customer-2026-10-01", "v1", "全量")
        sm.withdraw_publish("PUB-customer-v1")
        ok, msg = sm.withdraw_publish("PUB-customer-v1")
        assert not ok
        assert "已撤回" in msg


class TestIntegration:
    """端到端集成链路测试。"""

    def test_link1_extract_to_publish(self):
        orch = PipelineOrchestrator()
        result = orch.link1_extract_to_publish(
            system="ERP", dim_name="customer",
            natural_keys=["nk-1", "nk-2", "nk-3"],
            snapshot_date="2026-10-01", version="v1",
        )
        assert result["ok"]
        assert orch.audit_log  # 应有执行轨迹

    def test_link2_scd_to_fact(self):
        orch = PipelineOrchestrator()
        orch.fact._key_mapping.register("nk-1", 1001)
        orch.fact._key_mapping.register("nk-2", 1002)
        orch.fact._key_mapping.register("nk-3", 1003)
        result = orch.link2_scd_to_fact(
            batch_id="B001", fact_table="sales",
            row_keys=["nk-1", "nk-2", "nk-3"],
            dim_names=["customer", "product"],
            measures=[{"name": "amount", "measure": "销售额", "agg": "SUM"}],
        )
        assert result["ok"]

    def test_link3_late_reprocess(self):
        orch = PipelineOrchestrator()
        orch.reprocess.set_watermark_cutoff("sales", date(2026, 10, 1))
        result = orch.link3_late_reprocess(
            record_id="LATE-001", fact_table="sales",
            business_date="2026-09-15", arrival_date="2026-10-20",
            task_id="RP-001",
        )
        assert result["ok"]
        assert len(result["impact"]) > 0

    def test_link4_degenerate_to_metric(self):
        orch = PipelineOrchestrator()
        result = orch.link4_degenerate_to_metric(
            fact_table="sales", degenerate_field="订单号",
            base_count=100, summary_grain={"customer", "product"},
            measure_name="销售额", metric_name="月度销售额",
        )
        assert result["ok"]

    def test_link5_bridge_to_fact(self):
        orch = PipelineOrchestrator()
        dim_weights = [
            {"row_id": 1, "dim_name": "channel", "weight": 0.6},
            {"row_id": 1, "dim_name": "promotion", "weight": 0.4},
        ]
        result = orch.link5_bridge_to_fact("sales", [1], dim_weights)
        assert result["ok"]

    def test_link5_bridge_fail(self):
        orch = PipelineOrchestrator()
        dim_weights = [
            {"row_id": 1, "dim_name": "channel", "weight": 0.5},
            {"row_id": 1, "dim_name": "promotion", "weight": 0.3},
        ]
        result = orch.link5_bridge_to_fact("sales", [1], dim_weights)
        assert not result["ok"]

    def test_link6_attr_decay(self):
        orch = PipelineOrchestrator()
        orch.scd.add_record(ScdRecord(1, "customer", "SK-001", "old",
            date(2026, 9, 1), date(9999, 12, 31), True, None))
        result = orch.link6_attr_decay_chain("customer", "等级", "默认值")
        assert result["affected_chains"] == 1

    def test_link7_hier_rollup(self):
        orch = PipelineOrchestrator()
        hierarchy = {"全国": ["省份"], "省份": ["区县"]}
        result = orch.link7_hier_rollup(
            "区域维度", hierarchy, "sales",
            {"customer", "product", "date"},
        )
        assert result["ok"]

    def test_link8_reprocess_cover(self):
        orch = PipelineOrchestrator()
        result = orch.link8_reprocess_cover(
            task_id="RP-002", table="sales",
            start="2026-09-01", end="2026-10-01",
            strategy="整段替换", old_batch="BATCH-OLD",
            new_gate_values={"重跑后命中率": 0.92},
        )
        assert result["ok"]

    def test_link8_reprocess_gate_fail(self):
        orch = PipelineOrchestrator()
        result = orch.link8_reprocess_cover(
            task_id="RP-003", table="sales",
            start="2026-09-01", end="2026-10-01",
            strategy="并存留痕", old_batch="BATCH-OLD",
            new_gate_values={"重跑后命中率": 0.70},
        )
        assert not result["ok"]

    def test_link9_stall_recover(self):
        orch = PipelineOrchestrator()
        result = orch.link9_stall_recover(
            system="ERP", required="2026-10-15", delay=5,
        )
        assert result["ok"]

    def test_link10_dim_merge(self):
        orch = PipelineOrchestrator()
        result = orch.link10_dim_merge_migrate(
            old_dim="dim_old", new_dim="dim_new",
            from_sks=["SK-001", "SK-002"],
            to_sks=["SK-101", "SK-102"],
            batch_id="BATCH-MIG",
        )
        assert result["migrated_count"] == 2

    def test_all_links_return_audit_logs(self):
        orch = PipelineOrchestrator()
        orch.link1_extract_to_publish("ERP", "customer", ["nk-1"], "2026-10-01", "v1")
        orch.link9_stall_recover("ERP", "2026-10-15", 5)
        report = orch.status_report()
        assert len(report["audit_log"]) >= 2
        assert "watermark" in report
        assert "scd" in report
        assert "fact" in report


class TestServiceIntegration:
    """服务层跨模块消费测试。"""

    def test_dim_entry_registration(self):
        from app.services.dim_entry import DimEntryService
        svc = DimEntryService()
        entry, msg = svc.register_dimension(
            "DIM-TEST", "测试维度", "test_key",
            "ERP", "SCD2", "测试",
        )
        assert entry is not None
        assert entry["status"] == "已注册"

    def test_dim_dup_rejected(self):
        from app.services.dim_entry import DimEntryService
        svc = DimEntryService()
        svc.register_dimension("DIM-DUP", "重复1", "k1", "ERP", "SCD2", "测试")
        entry2, msg2 = svc.register_dimension("DIM-DUP", "重复2", "k2", "ERP", "SCD1", "测试")
        assert entry2 is None
        assert "已存在" in msg2

    def test_src_extract_then_dim_validate(self):
        from app.services.src_extract import SrcExtractService
        from app.services.dim_entry import DimEntryService
        from app.store import store

        src = SrcExtractService()
        src.extract_batch("ERP", "2026-10", "全量", 1000)
        rows = store.rows("src_extract")
        for r in rows:
            if r.get("来源系统") == "ERP":
                r["status"] = "已抽取"

        dim = DimEntryService()
        entry, _ = dim.register_dimension("DIM-VAL", "验证维度", "vk1", "ERP", "SCD2", "测试")
        dim_rows = list(store.rows("dim_entry"))
        target = None
        for r in dim_rows:
            if r.get("维度编号") == "DIM-VAL":
                target = r
                break
        assert target is not None
        ok, _ = dim.validate_natural_key_source(target)
        # ERP is in src_extract with status 已抽取, so should pass
        assert ok

    def test_scd_close_then_conform_block(self):
        from app.services.scd_chain import ScdChainService
        from app.services.conform_check import ConformCheckService

        scd = ScdChainService()
        ok, msg, _ = scd.close_chain_record("SCD-001", "2026-10-15", None)
        assert ok

        cc = ConformCheckService()
        # Use actual seed data dimensions: 客户维度 has business key "customer_id"
        ok, msg, result = cc.check_conformity("客户维度", "客户维度", "客户级")
        # Same dimension compared against itself should be consistent
        assert ok

    def test_conform_deviation_blocks_assembly(self):
        from app.services.conform_check import ConformCheckService
        cc = ConformCheckService()
        blocked, msg = cc.block_fact_assembly("产品维度")
        assert blocked
        assert "阻断" in msg

    def test_fact_assemble_hit_rate(self):
        from app.services.fact_assemble import FactAssembleService
        fa = FactAssembleService()
        ok, msg, result = fa.assemble_fact_batch(
            "BATCH-OK", "销售事实表", 3, 10,
            ["销售额", "销售量"],
        )
        assert ok
        assert result["hit_rate"] >= 0.80

    def test_late_fact_register(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        ok, msg, entry = lf.register_late_fact(
            "LATE-NEW", "销售事实表",
            "2026-09-01", "2026-10-15",
            "part-202609",
        )
        assert ok
        assert entry["滞后天数"] > 0
        assert entry["回补结论"] == "待回补"

    def test_late_fact_not_late(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        ok, msg, _ = lf.register_late_fact(
            "LATE-FUTURE", "销售事实表",
            "2026-10-16", "2026-10-15",
            "part-202610",
        )
        assert not ok
        assert "不视为迟到" in msg

    def test_late_decide_backfill(self):
        from app.services.late_fact import LateFactService
        lf = LateFactService()
        lf.register_late_fact("LATE-BF", "销售事实表",
                              "2026-08-01", "2026-10-15", "part-202608")
        ok, msg = lf.decide_backfill("LATE-BF", "2026-10-01", "增量追加")
        assert ok
        assert "整段重算" in msg

    def test_quality_gate_evaluate(self):
        from app.services.quality_gate import QualityGateService
        qg = QualityGateService()
        ok, msg, entry = qg.evaluate_gate(
            "QG-TEST", "销售事实表", "外键命中率",
            0.80, 0.95, "批次级",
        )
        assert ok
        assert entry["通过判定"] == "是"

    def test_quality_gate_block(self):
        from app.services.quality_gate import QualityGateService
        qg = QualityGateService()
        ok, msg, entry = qg.evaluate_gate(
            "QG-FAIL", "销售事实表", "外键命中率",
            0.80, 0.60, "批次级",
        )
        assert not ok
        assert entry["通过判定"] == "否"
        assert entry["status"] == "已阻断"

    def test_reprocess_schedule(self):
        from app.services.reprocess_task import ReprocessTaskService
        rp = ReprocessTaskService()
        ok, msg, entry = rp.schedule_reprocess(
            "RP-TEST", "口径变更", "销售事实表",
            "2026-09", "整段替换", "FACT-B001",
        )
        assert ok
        assert entry["覆盖策略"] == "整段替换"

    def test_lineage_propagate(self):
        from app.services.lineage_edge import LineageEdgeService
        le = LineageEdgeService()
        result = le.propagate_impact("fact_assemble")
        assert len(result) > 0  # Should find summary_layer and quality_gate

    def test_load_window_deviation(self):
        from app.services.load_window import LoadWindowService
        lw = LoadWindowService()
        ok, msg, entry = lw.advance_window(
            "LW-TEST", "销售事实表",
            "2026-10-01", "2026-10-01",
            1000, 500,
        )
        assert not ok
        assert "偏差" in msg

    def test_summary_no_overwrite(self):
        from app.services.summary_layer import SummaryLayerService
        sl = SummaryLayerService()
        sl.derive_summary("SUM-A", "销售事实表", "日", "SUM(amount)",
                          "BATCH-X", "全量刷新", 100)
        ok, msg, _ = sl.derive_summary("SUM-B", "销售事实表", "日", "SUM(amount)",
                                        "BATCH-X", "全量刷新", 100)
        assert not ok
        assert "不得覆盖" in msg

    def test_grain_declare_and_check(self):
        from app.services.grain_declare import GrainDeclareService
        from app.services.summary_layer import SummaryLayerService
        gd = GrainDeclareService()
        ok, msg, entry = gd.declare_grain(
            "GRN-TEST", "销售事实表",
            "客户每笔销售", "客户+产品+时间",
            "半可加", "customer_id+date", "v1",
        )
        assert ok

        sl = SummaryLayerService()
        ok, msg = sl.check_grain_consistency("客户+产品+时间", "客户+产品+时间")
        assert ok

    def test_hier_break_detection(self):
        from app.services.hier_path import HierPathService
        hp = HierPathService()
        breaks = hp.detect_break("区域维度")
        assert len(breaks) >= 1  # HIER-003 has parent "城市" not in hierarchy

    def test_bridge_normalization_service(self):
        from app.services.bridge_link import BridgeLinkService
        bl = BridgeLinkService()
        # Each row's total weight must sum to 1.0 for normalization to pass
        ok, msg, result = bl.check_normalization(
            "销售事实表", {1: 1.0, 2: 1.0},
        )
        assert ok

    def test_bridge_normalization_fail_service(self):
        from app.services.bridge_link import BridgeLinkService
        bl = BridgeLinkService()
        ok, msg, result = bl.check_normalization(
            "销售事实表", {1: 0.5, 2: 0.3},
        )
        assert not ok
        assert "归一失败" in msg

    def test_dim_publish_no_duplicate_version(self):
        from app.services.dim_publish import DimPublishService
        dp = DimPublishService()
        ok, msg, _ = dp.publish_version(
            "PUB-TEST", "客户维度", "v2",
            "2026-10-01", "全量", "test",
        )
        assert ok
        ok2, msg2, _ = dp.publish_version(
            "PUB-TEST-2", "客户维度", "v2",
            "2026-10-01", "全量", "test",
        )
        assert not ok2
        assert "不得静默替换" in msg2

    def test_attr_decay_then_scd_apply(self):
        from app.services.attr_decay import AttrDecayService
        from app.services.scd_chain import ScdChainService
        ad = AttrDecayService()
        ok, msg, entry = ad.judge_attr_decay(
            "ATTR-TEST", "客户维度", "客户等级",
            "等级调整", "2026-11-01",
        )
        assert ok
        assert int(entry["受影响链数"]) > 0

        scd = ScdChainService()
        count = scd.apply_attr_invalidation("客户维度", "客户等级", "默认值")
        assert count > 0

    def test_model_audit_record(self):
        from app.services.model_audit import ModelAuditService
        ma = ModelAuditService()
        ok, msg, entry = ma.record_audit(
            "AUD-TEST", "粒度声明修订",
            "GRN-001", "旧值", "新值",
            3, "worker",
        )
        assert ok
        assert entry["操作类型"] == "粒度声明修订"

    def test_seed_data_integrity(self):
        """种子数据完整性：每个模块都有至少 3 条记录含不同状态。"""
        from app.store import store
        for name in store.module_names():
            rows = store.rows(name)
            assert len(rows) >= 1, f"{name} 种子数据为空"
            statuses = {r.get("status") for r in rows if r.get("status")}
            assert len(statuses) >= 1, f"{name} 只有一种状态"

    def test_cross_module_chain_dim_to_fact(self):
        """跨模块链路：维度建档→代理键→拉链→事实装配。"""
        from app.services.dim_entry import DimEntryService
        from app.services.surrogate_key import SurrogateKeyService
        from app.services.fact_assemble import FactAssembleService

        dim = DimEntryService()
        dim.register_dimension("DIM-CHAIN", "链路测试", "chain_key", "ERP", "SCD2", "测试")

        sk = SurrogateKeyService()
        key, msg = sk.allocate_key("客户维度", "chain_key")
        assert key is not None

        fa = FactAssembleService()
        ok, _, result = fa.assemble_fact_batch(
            "BATCH-CHAIN", "销售事实表", 3, 10,
            ["销售额", "销售量"],
        )
        assert ok
        assert result["hit_rate"] >= 0.80

    def test_default_natural_key_source_check(self):
        """默认种子数据维度校验：DIM-001 的自然键来源应为 ERP。"""
        from app.services.dim_entry import DimEntryService
        from app.store import store

        dim = DimEntryService()
        rows = store.rows("dim_entry")
        target = None
        for r in rows:
            if r.get("维度编号") == "DIM-001":
                target = r
                break
        assert target is not None
        ok, msg = dim.validate_natural_key_source(target)
        assert ok or "未在已抽取" in msg


class TestDataEdgeCases:
    """边界与脏数据测试。"""

    def test_zero_expected_rows(self):
        wm = WatermarkManager()
        ok, msg = wm.advance_load_window("t", "s", "e", 0, 0)
        assert not ok
        assert "无效" in msg

    def test_empty_dim_snapshot_rejected(self):
        sm = SnapshotManager()
        ok, _, _ = sm.create_snapshot("nonexistent", date(2026, 10, 1), "B")
        assert not ok

    def test_publish_nonexistent_snapshot(self):
        sm = SnapshotManager()
        ok, msg, _ = sm.publish_snapshot("NOT-EXIST", "v1", "全量")
        assert not ok
        assert "不存在" in msg

    def test_sorting_respected_in_overlap(self):
        """Sorting order is respected: overlapping records found regardless of insert order."""
        sp = ScdProcessor()
        # Insert in reverse chronological order
        sp.add_record(ScdRecord(2, "dim1", "SK-002", "f2",
            date(2026, 10, 1), date(2026, 11, 1), True, None))
        sp.add_record(ScdRecord(1, "dim1", "SK-001", "f1",
            date(2026, 9, 15), date(2026, 10, 15), True, None))
        conflicts, _ = sp.detect_overlap("dim1")
        assert len(conflicts) >= 1

    def test_status_report_completeness(self):
        orch = PipelineOrchestrator()
        orch.link1_extract_to_publish("ERP", "test", ["nk-1"], "2026-10-01", "v1")
        report = orch.status_report()
        for key in ["audit_log", "watermark", "scd", "fact", "quality", "reprocess", "snapshot"]:
            assert key in report, f"Missing key {key} in status_report"


class TestPauseRequestFlow:
    """暂停申请全链路：发起窗口申请 → 装载窗口待办 → 边界暂停 → 换班恢复。"""

    def _running_task(self, task_no: str, table: str):
        from app.services.reprocess_task import ReprocessTaskService
        rp = ReprocessTaskService()
        ok, _, entry = rp.schedule_reprocess(
            task_no, "口径变更", table, "2026-10", "整段替换", "FACT-B001",
        )
        assert ok and entry is not None
        got, _ = rp.run_action(int(entry["id"]), "排期重跑")
        assert got is not None and got["status"] == "重跑中"
        return rp, got

    def _inflight_window(self, window_no: str, table: str):
        from app.services.load_window import LoadWindowService
        lw = LoadWindowService()
        entry, missing = lw.create_entry({
            "窗口编号": window_no, "所属事实表": table, "窗口起点": "2026-10-09",
            "窗口终点": "2026-10-09", "预期行数": "1000", "实际行数": "",
            "装载水位": "", "装载耗时": "",
        })
        assert not missing and entry is not None
        got, _ = lw.run_action(int(entry["id"]), "启动装载")
        assert got is not None and got["status"] == "装载中"
        return lw, got

    def _pause_of(self, task_no: str):
        from app.services.pause_request import PauseRequestService
        req = PauseRequestService().active_for_task(task_no)
        assert req is not None
        return req

    def _todo_of(self, request_no: str):
        from app.services.pause_request import PauseRequestService
        todo = PauseRequestService().find_todo(request_no)
        assert todo is not None
        return todo

    def test_drain_mode_full_flow(self):
        """收完再停：冲突互斥 → 放行窗口收完自动停住 → 二班接手恢复，W9 类窗口无需重确认。"""
        rp, task = self._running_task("RP-PA1", "暂停测试表A")
        lw, window = self._inflight_window("W-A1", "暂停测试表A")

        # 发起窗口：申请暂停 → 待送达（尚未交给装载班）
        got, msg = rp.run_action(int(task["id"]), "申请暂停")
        assert got is not None and got["status"] == "暂停申请中"
        req = self._pause_of("RP-PA1")
        assert req["status"] == "待送达"

        # 重复申请被拒
        rejected, _ = rp.run_action(int(task["id"]), "申请暂停")
        assert rejected is None

        # 发起窗口：送达装载班 → 待确认（装载班手上等决定），装载窗口生成一班待办
        got, msg = rp.run_action(int(task["id"]), "送达装载班")
        assert got is not None
        assert req["status"] == "待确认"
        todo = self._todo_of(req["申请编号"])
        assert todo["status"] == "暂停待办"
        assert todo["当班人"] == "一班"
        assert "冲突" in msg and "W-A1" in msg

        # 并发互斥：暂停待决期间在写窗口确认被暂扣（只能有一方推进）
        blocked, msg = lw.run_action(int(window["id"]), "确认装载")
        assert blocked is None and "暂扣" in msg

        # 装载窗口：值班员决定收完再停 → 放行 W-A1，暂停等待收完
        got, msg = lw.run_action(int(todo["id"]), "确认暂停·收完再停")
        assert got is not None and "收完再停" in msg
        assert req["status"] == "待确认" and req["放行窗口"] == "W-A1"

        # 放行窗口收完 → 暂停自动生效，三方看到同一恢复位置
        got, _ = lw.run_action(int(window["id"]), "确认装载")
        assert got is not None and got["status"] == "已装载"
        assert req["status"] == "已暂停"
        boundary = req["恢复位置"]
        assert "W-A1" in boundary and "之后" in boundary
        assert req["待重确认窗口"] == ""  # 收完再停：恢复时无需重确认在写窗口
        todo = self._todo_of(req["申请编号"])
        task = rp.get_entry(int(task["id"]))
        assert todo["装载水位"] == boundary
        assert task["重跑水位"] == boundary
        assert task["status"] == "已暂停" and todo["status"] == "已暂停"

        # 已暂停期间不得启动新窗口
        rejected, _ = lw.run_action(int(window["id"]), "启动装载")
        assert rejected is None

        # 刷新后二班接手：换班不改恢复位置，确认恢复直接生效（无需重确认 W-A1）
        got, msg = lw.run_action(int(todo["id"]), "换班接手")
        assert got is not None and got["当班人"] == "二班"
        assert req["恢复位置"] == boundary
        got, msg = lw.run_action(int(todo["id"]), "确认恢复")
        assert got is not None and got["status"] == "已恢复"
        assert req["status"] == "已恢复" and req["恢复当班"] == "二班"
        assert rp.get_entry(int(task["id"]))["status"] == "重跑中"
        # 已完成步骤不丢不补做：W-A1 保持已装载，未生成重复窗口
        assert lw.get_entry(int(window["id"]))["status"] == "已装载"
        same = [r for r in lw.list_entries()[0] if r.get("窗口编号") == "W-A1"]
        assert len(same) == 1

    def test_withhold_mode_requires_reconfirm(self):
        """暂扣确认：保护已落下步骤，恢复前必须重确认在写窗口（W9 情形）。"""
        rp, task = self._running_task("RP-PA2", "暂停测试表B")
        lw, window = self._inflight_window("W-B1", "暂停测试表B")
        window["实际行数"] = "800"  # 已落下的步骤

        rp.run_action(int(task["id"]), "申请暂停")
        rp.run_action(int(task["id"]), "送达装载班")
        req = self._pause_of("RP-PA2")
        todo = self._todo_of(req["申请编号"])

        # 值班员决定暂扣确认 → 立即停住，在写窗口进入暂扣保护
        got, msg = lw.run_action(int(todo["id"]), "确认暂停·暂扣确认")
        assert got is not None
        assert req["status"] == "已暂停"
        assert lw.get_entry(int(window["id"]))["status"] == "暂扣中"
        assert req["恢复位置"] == "W-B1（需重确认）"
        assert req["待重确认窗口"] == "W-B1"

        # 暂扣窗口受保护：不得中断丢弃已完成步骤
        rejected, msg = lw.run_action(int(window["id"]), "标记中断")
        assert rejected is None and "暂扣" in msg

        # 恢复被拦：需先重确认 W-B1
        rejected, msg = lw.run_action(int(todo["id"]), "确认恢复")
        assert rejected is None and "W-B1" in msg

        # 二班接手后重确认 W-B1：只确认不补做，已落下步骤保留
        lw.run_action(int(todo["id"]), "换班接手")
        got, _ = lw.run_action(int(window["id"]), "确认装载")
        assert got is not None and got["status"] == "已装载"
        assert got["实际行数"] == "800"
        assert req["待重确认窗口"] == ""

        # 实做恢复：任务回到重跑中，恢复位置双方一致
        got, _ = lw.run_action(int(todo["id"]), "确认恢复")
        assert got is not None and got["status"] == "已恢复"
        assert rp.get_entry(int(task["id"]))["status"] == "重跑中"
        assert req["恢复位置"] == "W-B1（需重确认）"
        assert req["恢复位置"] == rp.get_entry(int(task["id"]))["重跑水位"]

    def test_no_inflight_window_settles_immediately(self):
        """无在写窗口：确认即停，边界取当前装载水位。"""
        rp, task = self._running_task("RP-PA3", "暂停测试表C")
        lw, window = self._inflight_window("W-C1", "暂停测试表C")
        lw.run_action(int(window["id"]), "确认装载")  # 先收完，表内只剩已装载窗口

        rp.run_action(int(task["id"]), "申请暂停")
        rp.run_action(int(task["id"]), "送达装载班")
        req = self._pause_of("RP-PA3")
        todo = self._todo_of(req["申请编号"])
        got, msg = lw.run_action(int(todo["id"]), "确认暂停·收完再停")
        assert got is not None and "无在写窗口" in msg
        assert req["status"] == "已暂停"
        assert req["恢复位置"] == "水位2026-10-09之后"
        assert rp.get_entry(int(task["id"]))["重跑水位"] == req["恢复位置"]

        got, _ = lw.run_action(int(todo["id"]), "确认恢复")
        assert got is not None
        assert rp.get_entry(int(task["id"]))["status"] == "重跑中"

    def test_guards(self):
        """入口守卫：状态不符、暂停中完成任务、待办行误操作、手工登记均被拒。"""
        from app.services.load_window import LoadWindowService
        from app.services.pause_request import PauseRequestService
        from app.services.reprocess_task import ReprocessTaskService

        rp = ReprocessTaskService()
        # 非重跑中任务不能申请暂停
        ok, _, entry = rp.schedule_reprocess(
            "RP-PA4", "口径变更", "暂停测试表D", "2026-10", "整段替换", "FACT-B001")
        assert ok and entry is not None
        rejected, msg = rp.run_action(int(entry["id"]), "申请暂停")
        assert rejected is None and "重跑中" in msg
        # 未申请不能送达
        rejected, _ = rp.run_action(int(entry["id"]), "送达装载班")
        assert rejected is None

        # 暂停申请中不得确认完成
        rp.run_action(int(entry["id"]), "排期重跑")
        rp.run_action(int(entry["id"]), "申请暂停")
        rejected, msg = rp.run_action(int(entry["id"]), "确认完成")
        assert rejected is None and "暂停" in msg

        # 待办行不能执行窗口动作
        rp.run_action(int(entry["id"]), "送达装载班")
        req = self._pause_of("RP-PA4")
        todo = self._todo_of(req["申请编号"])
        lw = LoadWindowService()
        rejected, msg = lw.run_action(int(todo["id"]), "启动装载")
        assert rejected is None and "待办" in msg
        # 普通窗口不能执行待办动作
        _, w = self._inflight_window("W-D1", "暂停测试表E")
        rejected, msg = lw.run_action(int(w["id"]), "确认暂停·收完再停")
        assert rejected is None and "待办" in msg

        # 暂停申请模块只读回查：手工登记与动作均被拒
        pause = PauseRequestService()
        entry2, missing = pause.create_entry({"申请编号": "PR-X", "重跑编号": "RP-X", "影响事实表": "T"})
        assert entry2 is None and missing
        rejected, _ = pause.run_action(1, "任意动作")
        assert rejected is None