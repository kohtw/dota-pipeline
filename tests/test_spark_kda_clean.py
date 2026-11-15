"""Run with a local Java 17+ runtime and the Spark job requirements installed."""
import json
import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))
import clean_replays as base
import clean_replays as clean
from test_spark_clean import MATCH, fixture


@pytest.fixture(scope="module")
def spark():
    session = base.spark_session()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def raw(spark):
    tables = fixture(spark)
    # The existing general-clean fixture starts with nonzero counters. Here the
    # initial snapshot is a genuine zero baseline and final totals stay intact.
    resource = tables["player_resource"]
    for stat in ("Kills", "Deaths", "Assists"):
        name = f"m_vecPlayerTeamData.m_i{stat}"
        resource = resource.withColumn(name, F.when(F.col("tick") == 30, 0).otherwise(base.col(name)))
    tables["player_resource"].unpersist()
    tables["player_resource"] = resource.cache()
    tables["kda_changes"] = (resource.filter(F.col("index") < 10)
                             .withColumn("tick", F.when(F.col("tick") == 90, 82).otherwise(F.col("tick")))
                             .cache())
    # A tower lands the last hit. Counter credit belongs to player 0, not the tower.
    combat = tables["combatlog"].withColumn("damage_source_name_idx", F.col("attacker_name_idx"))
    for column in ("attacker_name_idx", "damage_source_name_idx"):
        combat = combat.withColumn(column, F.when(F.col("type") == "DOTA_COMBATLOG_DEATH", 101)
                                   .otherwise(F.col(column)))
    tables["combatlog"].unpersist()
    tables["combatlog"] = combat.cache()
    names = tables["names"]
    tower = names.limit(1).select(F.col("match_id"), F.lit(101).alias("idx"),
                                 F.lit("npc_dota_goodguys_tower1_mid").alias("name"))
    tables["names"] = names.unionByName(tower).cache()
    names.unpersist()
    yield tables
    for df in tables.values():
        df.unpersist()


@pytest.fixture(scope="module")
def result(raw):
    value = clean.build_clean(raw, MATCH)
    yield value
    value.release()


def test_tower_last_hit_gets_counter_credit(result):
    death = result.tables["deaths"].first()
    assert death.killer_slot is None
    assert death.killer_name == "npc_dota_goodguys_tower1_mid"
    assert death.credited_killer_slot == 0
    assert death.link_status == "exact"
    assert death.game_time_s == pytest.approx(2 / 3)
    assist = result.tables["death_assists"].first()
    assert assist.assist_slot == 1
    assert not assist.same_side


def test_all_nine_tables_pass_strict_validation(result):
    report = base.validate(result, MATCH)
    assert set(result.tables) == set(base.SCHEMAS)
    assert report["errors"] == []
    assert report["findings"]["scoreboard_disagreements"]["rows"] == 0
    assert result.tables["deaths"].schema.simpleString().endswith(
        "credited_killer_slot:int,link_status:string>")


def test_partial_assists_publish_flags_report_and_marker(raw, tmp_path):
    tables = dict(raw)
    for name in ("player_resource", "kda_changes"):
        df = tables[name]
        final = F.col("tick") > 30
        tables[name] = (df.withColumn("m_vecPlayerTeamData.m_iKills",
            F.when(final & (F.col("index") == 0), 2)
             .otherwise(base.col("m_vecPlayerTeamData.m_iKills")))
            .withColumn("m_vecPlayerTeamData.m_iDeaths",
            F.when(final & (F.col("index") == 6), 1)
             .otherwise(base.col("m_vecPlayerTeamData.m_iDeaths"))))
    combat = tables["combatlog"]
    second = combat.filter(F.col("type") == "DOTA_COMBATLOG_DEATH").withColumn("target_name_idx", F.lit(6))
    tables["combatlog"] = combat.unionByName(second)
    partial = clean.build_clean(tables, MATCH)
    try:
        match = partial.tables["matches"].first()
        assert match.assist_attribution_complete is False
        assert match.assist_attribution_assumed is False
        assert partial.tables["death_assists"].count() == 0
        assert partial.tables["deaths"].count() == 2
        assert partial.attribution["unresolved_assists"] == {1: 1}
        report = clean.validate(partial, MATCH)
        assert report["errors"] == []
        # Without the known withheld credit, the scoreboard check must fail.
        assert clean.scoreboard_disagreements(partial.tables).count() == 1
        assert clean.write_result(tables["players"].sparkSession, partial, report, str(tmp_path))
        saved = json.loads((tmp_path / "_review" / f"match_id={MATCH}" / "report.json").read_text())
        assert saved["assist_attribution"]["unresolved_assists"] == {"1": 1}
        assert (tmp_path / "_complete" / f"match_id={MATCH}" / "_SUCCESS").exists()
    finally:
        partial.release()


def test_writer_saves_new_columns_and_marker_last(spark, result, tmp_path):
    import pyarrow.parquet as pq

    report = base.validate(result, MATCH)
    assert base.write_result(spark, result, report, str(tmp_path))
    path = tmp_path / "deaths" / f"match_id={MATCH}"
    saved = pq.ParquetFile(next(path.glob("*.parquet"))).read().to_pydict()
    assert "match_id" not in saved
    assert saved["credited_killer_slot"] == [0]
    assert (tmp_path / "_complete" / f"match_id={MATCH}" / "_SUCCESS").exists()
    # A preview rerun invalidates success and holds credit as candidates.
    assert not base.write_result(spark, result, report, str(tmp_path), preview=True)
    assert not path.exists()
    assert not (tmp_path / "_complete" / f"match_id={MATCH}").exists()
    review = tmp_path / "_review" / f"match_id={MATCH}"
    assert list((review / "candidates" / "deaths" / f"match_id={MATCH}").glob("*.parquet"))
    assert json.loads((review / "report.json").read_text())["preview"]


def test_missing_per_tick_input_is_actionable(spark, tmp_path):
    with pytest.raises(clean.CreditError, match="Re-parse with per-tick"):
        clean.read_match(spark, str(tmp_path), MATCH)


def test_wrong_match_id_blocks_before_transform(raw):
    with pytest.raises(clean.CreditError, match="wrong match_id"):
        clean.build_clean(raw, MATCH + 1)


def test_resource_index_reassignment_blocks(raw, result):
    original = raw["kda_changes"]
    extra = original.filter(F.col("index") == 0).limit(1).withColumn(
        "m_vecPlayerTeamData.m_iTeamSlot", F.lit(1))
    with pytest.raises(clean.CreditError, match="more than one slot"):
        clean.normalize_counters(original.unionByName(extra), result.tables["players"])


def test_invalid_assist_tokens_do_not_disappear(raw, result):
    combat = raw["combatlog"].withColumn("assist_players", F.lit("0;bad;1"))
    with pytest.raises(clean.CreditError, match="invalid assist player IDs"):
        clean.normalize_deaths(combat, raw["names"], result.tables["players"])


def test_draft_placeholder_hero_is_allowed_but_final_hero_must_match(raw, result):
    changes = raw["kda_changes"].withColumn(
        "m_vecPlayerTeamData.m_nSelectedHeroID",
        F.when(F.col("tick") == 30, 1000000)
        .otherwise(base.col("m_vecPlayerTeamData.m_nSelectedHeroID")))
    assert clean.normalize_counters(changes, result.tables["players"]).count() == 20
    wrong_final = changes.withColumn("m_vecPlayerTeamData.m_nSelectedHeroID", F.lit(1000000))
    with pytest.raises(clean.CreditError, match="hero identity disagrees"):
        clean.normalize_counters(wrong_final, result.tables["players"])


def test_no_spark_required_for_cli_help():
    with pytest.raises(SystemExit) as exit_info:
        clean.main(["--help"])
    assert exit_info.value.code == 0


def test_embedded_counter_match_id_cannot_be_overwritten(spark, tmp_path, monkeypatch):
    path = tmp_path / "kda_changes" / f"match_id={MATCH}"
    spark.createDataFrame([(MATCH + 1,)], "match_id long").write.parquet(str(path))
    monkeypatch.setattr(base, "_read_base_tables", lambda *args: {})
    with pytest.raises(clean.CreditError, match="embedded match_id"):
        clean.read_match(spark, str(tmp_path), MATCH)


def test_cli_missing_counters_writes_failure_report_and_removes_old_marker(
        spark, tmp_path, monkeypatch):
    output = tmp_path / "output"
    marker = output / "_complete" / f"match_id={MATCH}" / "_SUCCESS"
    marker.parent.mkdir(parents=True)
    marker.touch()
    monkeypatch.setattr(base, "spark_session", lambda **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    status = clean.main([str(tmp_path / "input"), str(output), str(MATCH)])
    assert status == 2
    assert not marker.exists()
    report = json.loads((output / "_review" / f"match_id={MATCH}" / "report.json").read_text())
    assert not report["complete"]
    assert "Missing per-tick kda_changes" in report["errors"][0]
