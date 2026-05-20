import os

from overgraph import OverGraph


def test_scrub_healthy_database(tmp_dir):
    db_path = os.path.join(tmp_dir, "scrub_db")
    db = OverGraph.open(db_path)
    for i in range(5):
        db.upsert_node("Person", f"node_{i}")
    db.flush()

    report = db.scrub()
    assert report.total_components_failed == 0
    assert report.total_components_ok > 0
    assert report.total_components_checked > 0
    assert report.duration_ms >= 0
    assert len(report.segments) == 1
    assert report.segments[0].segment_id == 1
    assert report.segments[0].components_ok > 0
    assert len(report.segments[0].findings) == 0
    db.close()


def test_scrub_detects_corruption(tmp_dir):
    db_path = os.path.join(tmp_dir, "scrub_corrupt_db")
    db = OverGraph.open(db_path)
    for i in range(5):
        db.upsert_node("Person", f"node_{i}")
    db.flush()
    db.close()

    core_path = os.path.join(db_path, "segments", "seg_0001", "segment.core")
    data = bytearray(open(core_path, "rb").read())
    data[len(data) // 2] ^= 0xFF
    open(core_path, "wb").write(data)

    db = OverGraph.open(db_path)
    report = db.scrub()
    assert report.total_components_failed > 0
    findings = report.segments[0].findings
    assert len(findings) > 0
    assert any(f.finding_type == "PayloadDigestMismatch" for f in findings)
    assert findings[0].component_kind != ""
    assert findings[0].detail != ""
    db.close()


def test_scrub_repr(tmp_dir):
    db_path = os.path.join(tmp_dir, "scrub_repr_db")
    db = OverGraph.open(db_path)
    db.upsert_node("Person", "x")
    db.flush()

    report = db.scrub()
    r = repr(report)
    assert "ScrubReport" in r
    assert "segments=1" in r
    db.close()
