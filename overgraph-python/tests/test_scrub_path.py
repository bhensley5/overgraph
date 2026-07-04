import os
import shutil
from pathlib import Path

import pytest
from overgraph import OverGraph, async_scrub_path, scrub_path


def create_closed_db(db_path):
    db = OverGraph.open(db_path, compact_after_n_flushes=0)
    for i in range(8):
        db.upsert_node("Person", f"node_{i}")
    db.flush()
    db.close()


def corrupt_segment_core(db_path):
    core_path = os.path.join(db_path, "segments", "seg_0001", "segment.core")
    with open(core_path, "rb") as file:
        data = bytearray(file.read())
    data[len(data) // 2] ^= 0xFF
    with open(core_path, "wb") as file:
        file.write(data)


def write_empty_wal(path):
    with open(path, "wb") as file:
        file.write(b"OVGR")
        file.write((3).to_bytes(4, "little"))


def finding_types(report):
    return [finding.finding_type for finding in report.findings]


def test_scrub_path_healthy_closed_database(tmp_dir):
    db_path = os.path.join(tmp_dir, "healthy_sync")
    create_closed_db(db_path)

    report = scrub_path(db_path)

    assert report.manifest.source == "Current"
    assert report.manifest.segment_count == 1
    assert report.total_components_failed == 0
    assert report.total_components_checked > 0
    assert report.total_wal_bytes_checked > 0
    assert len(report.segments) == 1
    assert len(report.segments[0].findings) == 0
    assert any(wal.role == "Active" for wal in report.wal_generations)
    assert len(report.orphan_segments) == 0
    assert not any(finding.severity == "error" for finding in report.findings)


@pytest.mark.asyncio
async def test_async_scrub_path_healthy_closed_database(tmp_dir):
    db_path = os.path.join(tmp_dir, "healthy_async")
    create_closed_db(db_path)

    report = await async_scrub_path(db_path)

    assert report.manifest.source == "Current"
    assert report.total_components_failed == 0
    assert len(report.segments) == 1
    assert any(wal.role == "Active" for wal in report.wal_generations)


def test_scrub_path_detects_corruption_without_reopen(tmp_dir):
    db_path = os.path.join(tmp_dir, "corrupt_core")
    create_closed_db(db_path)
    corrupt_segment_core(db_path)

    report = scrub_path(db_path)

    assert report.total_components_failed > 0
    assert any(
        finding.finding_type in {"PayloadDigestMismatch", "IdentityHeaderMismatch"}
        for segment in report.segments
        for finding in segment.findings
    )


def test_scrub_path_missing_manifest_returns_finding_without_creating_manifest(tmp_dir):
    db_path = os.path.join(tmp_dir, "missing_manifest")
    os.mkdir(db_path)

    report = scrub_path(db_path)

    assert report.manifest is None
    assert "ManifestMissing" in finding_types(report)
    assert not os.path.exists(os.path.join(db_path, "manifest.current"))


def test_scrub_path_missing_manifest_with_artifacts_reports_orphans(tmp_dir):
    db_path = os.path.join(tmp_dir, "missing_manifest_artifacts")
    os.makedirs(os.path.join(db_path, "segments", "seg_0001"))
    write_empty_wal(os.path.join(db_path, "wal_0.wal"))

    report = scrub_path(db_path)

    assert report.manifest is None
    assert "ManifestMissing" in finding_types(report)
    assert "OrphanSegment" in finding_types(report)
    assert "OrphanWal" in finding_types(report)
    assert not os.path.exists(os.path.join(db_path, "manifest.current"))


def test_scrub_path_option_kwargs_and_unknown_kwargs(tmp_dir):
    db_path = os.path.join(tmp_dir, "options")
    create_closed_db(db_path)

    report = scrub_path(
        db_path,
        validate_wal=False,
        include_orphan_segments=False,
        check_manifest_stability=False,
    )

    assert len(report.wal_generations) == 0
    assert len(report.orphan_segments) == 0
    assert report.total_wal_bytes_checked == 0

    with pytest.raises(ValueError, match="Unknown option 'unknown'"):
        scrub_path(db_path, unknown=True)


def test_scrub_path_include_orphan_segments_returns_deep_result(tmp_dir):
    db_path = os.path.join(tmp_dir, "orphan_deep")
    create_closed_db(db_path)
    orphan_path = os.path.join(db_path, "segments", "seg_0099")
    shutil.copytree(os.path.join(db_path, "segments", "seg_0001"), orphan_path)

    report = scrub_path(db_path, include_orphan_segments=True)

    assert len(report.orphan_segments) == 1
    assert report.orphan_segments[0].semantic_checks_skipped is True
    assert report.orphan_segments[0].components_ok > 0
    assert "OrphanSegmentUnpinned" in finding_types(report)
    assert os.path.exists(orphan_path)


def test_scrub_path_reports_orphan_artifacts_without_deleting_them(tmp_dir):
    db_path = os.path.join(tmp_dir, "orphan_artifacts")
    create_closed_db(db_path)
    orphan_segment = os.path.join(db_path, "segments", "seg_0099")
    orphan_wal = os.path.join(db_path, "wal_99.wal")
    shutil.copytree(os.path.join(db_path, "segments", "seg_0001"), orphan_segment)
    write_empty_wal(orphan_wal)

    report = scrub_path(db_path)

    assert "OrphanSegment" in finding_types(report)
    assert "OrphanWal" in finding_types(report)
    assert len(report.orphan_segments) == 0
    assert os.path.exists(orphan_segment)
    assert os.path.exists(orphan_wal)


def test_async_scrub_path_uses_to_thread():
    source_path = (
        Path(__file__).resolve().parents[1]
        / "python"
        / "overgraph"
        / "async_api.py"
    )
    source = source_path.read_text()
    start = source.index("async def async_scrub_path")
    end = source.index("class AsyncWriteTxn")
    helper_source = source[start:end]

    assert "asyncio.to_thread(scrub_path, path, **options)" in helper_source
