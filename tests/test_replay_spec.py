"""Sampling options and the new raw counter table."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))
import replay_spec


def test_kda_uses_per_tick_changes_without_changing_existing_snapshot_rates():
    tables = {t["name"]: t for t in replay_spec.TABLES}
    table = tables["kda_changes"]
    assert table["sample_ticks"] == 1
    assert table["changes_only"] is True
    assert table["classes"] == ["CDOTA_PlayerResource"]
    assert table["index_count"] == 64
    assert len(table["props"]) == 6
    assert replay_spec.SAMPLE_TICKS == 30
    for name in ("heroes", "gamerules", "player_resource", "team_data"):
        assert "sample_ticks" not in tables[name]
        assert "changes_only" not in tables[name]


@pytest.mark.parametrize("rate", [0, -1, True, 1.5, "1"])
def test_invalid_sampling_rates_fail(rate):
    with pytest.raises(ValueError, match="positive integer"):
        replay_spec.snapshot("bad", [], [], sample_ticks=rate)
