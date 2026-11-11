"""Local Spark regression tests. Run with unittest; no Docker or outside APIs needed."""
import json
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark_jobs"))
import clean_replays as clean
from pyspark.sql import functions as F, types as T

MATCH = 9079737909


class CleanPathTests(unittest.TestCase):
    def test_local_aliases_cannot_overwrite_raw_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "parsed"
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                clean.main([str(raw), str(raw / ".." / "parsed"), str(MATCH)])
            self.assertEqual(exc.exception.code, 2)
            self.assertEqual(clean.normalize_path("s3a://raw/a/../clean/"), "s3a://raw/clean")


def frame(spark, rows, fields):
    types = {"long": T.LongType(), "int": T.IntegerType(), "double": T.DoubleType(),
             "string": T.StringType(), "boolean": T.BooleanType()}
    schema = T.StructType([T.StructField(name, types[kind], True) for name, kind in fields])
    return spark.createDataFrame(rows, schema)


def fixture(spark):
    out = {}
    headers = [{"match_id": MATCH, "slot": i, "game_team": 2 if i < 5 else 3,
                "hero_name": f"npc_dota_hero_test_{i}"} for i in range(10)]
    out["match"] = frame(spark, [{"match_id": MATCH, "game_mode": 22, "game_winner": 2, "end_time": 1000}],
                         [("match_id", "long"), ("game_mode", "int"), ("game_winner", "int"), ("end_time", "long")])
    out["players"] = frame(spark, headers, [("match_id", "long"), ("slot", "int"), ("game_team", "int"), ("hero_name", "string")])
    out["gamerules"] = frame(spark, [
        {"match_id": MATCH, "tick": 30, "m_pGameRules.m_nGameState": 4, "m_pGameRules.m_flGameStartTime": 0., "m_pGameRules.m_flGameEndTime": 0.},
        {"match_id": MATCH, "tick": 60, "m_pGameRules.m_nGameState": 5, "m_pGameRules.m_flGameStartTime": 2., "m_pGameRules.m_flGameEndTime": 0.},
        {"match_id": MATCH, "tick": 90, "m_pGameRules.m_nGameState": 6, "m_pGameRules.m_flGameStartTime": 2., "m_pGameRules.m_flGameEndTime": 3.}],
        [("match_id", "long"), ("tick", "int"), ("m_pGameRules.m_nGameState", "int"),
         ("m_pGameRules.m_flGameStartTime", "double"), ("m_pGameRules.m_flGameEndTime", "double")])
    resource = []
    for i in range(10):
        for tick in [30, 90]:
            resource.append({"match_id": MATCH, "tick": tick, "index": i, "m_vecPlayerData.m_iPlayerTeam": 2 if i < 5 else 3,
                "m_vecPlayerTeamData.m_iTeamSlot": i % 5, "m_vecPlayerTeamData.m_nSelectedHeroID": 0 if tick == 30 else i + 1,
                "m_vecPlayerTeamData.m_iLevel": 2, "m_vecPlayerTeamData.m_iKills": 1 if i == 0 else 0,
                "m_vecPlayerTeamData.m_iDeaths": 1 if i == 5 else 0, "m_vecPlayerTeamData.m_iAssists": 1 if i == 1 else 0})
    resource.append({"match_id": MATCH, "tick": 90, "index": 10, "m_vecPlayerData.m_iPlayerTeam": 1})
    pr_fields = [("match_id", "long"), ("tick", "int"), ("index", "int"), ("m_vecPlayerData.m_iPlayerTeam", "int")]
    pr_fields += [(f"m_vecPlayerTeamData.{p}", "int") for p in ["m_iTeamSlot", "m_nSelectedHeroID", "m_iLevel", "m_iKills", "m_iDeaths", "m_iAssists"]]
    out["player_resource"] = frame(spark, resource, pr_fields)
    tdprops = ["m_iNetWorth", "m_iReliableGold", "m_iUnreliableGold", "m_iTotalEarnedGold", "m_iTotalEarnedXP",
               "m_iLastHitCount", "m_iDenyCount", "m_flHeroDamage", "m_flTowerDamage", "m_fHealing"]
    tdrows = []
    for i in range(10):
        for tick in [30, 90]:
            row = {"match_id": MATCH, "tick": tick, "index": i % 5, "class": "CDOTA_DataRadiant" if i < 5 else "CDOTA_DataDire"}
            for prop in tdprops:
                row[f"m_vecDataTeam.{prop}"] = 100. if prop in tdprops[-3:] else 100
            if i == 0:
                row["m_vecDataTeam.m_iUnreliableGold"] = None
            tdrows.append(row)
    out["team_data"] = frame(spark, tdrows, [("match_id", "long"), ("tick", "int"), ("index", "int"), ("class", "string")]
        + [(f"m_vecDataTeam.{p}", "double" if p in tdprops[-3:] else "int") for p in tdprops])
    hero_fields = [("match_id", "long"), ("tick", "int"), ("handle", "int"), ("m_iPlayerID", "int"),
        ("m_iTeamNum", "int"), ("m_hReplicatingOtherHeroModel", "int"), ("CBodyComponent.m_cellX", "int"),
        ("CBodyComponent.m_cellY", "int"), ("CBodyComponent.m_vecX", "double"), ("CBodyComponent.m_vecY", "double")]
    hero_fields += [(f"m_hItems.{i:04d}", "int") for i in range(17)]
    hero_rows = []
    for i in range(10):
        row = {"match_id": MATCH, "tick": 90, "handle": i, "m_iPlayerID": i * 2, "m_iTeamNum": 2 if i < 5 else 3,
               "m_hReplicatingOtherHeroModel": clean.NULL_HANDLE, "CBodyComponent.m_cellX": 128,
               "CBodyComponent.m_cellY": 128, "CBodyComponent.m_vecX": 10., "CBodyComponent.m_vecY": 20.}
        row.update({f"m_hItems.{slot:04d}": 1000 + i if slot == 0 else clean.NULL_HANDLE for slot in range(17)})
        hero_rows.append(row)
    # A second real entity must lose the lowest-handle tie; an illusion must not supply a position.
    hero_rows.extend([{**hero_rows[0], "handle": 100, "CBodyComponent.m_vecX": 999.},
                      {**hero_rows[0], "handle": 101, "m_hReplicatingOtherHeroModel": 0, "CBodyComponent.m_vecX": 888.}])
    out["heroes"] = frame(spark, hero_rows, hero_fields)
    out["names"] = frame(spark, [{"match_id": MATCH, "idx": i, "name": f"npc_dota_hero_test_{i}"} for i in range(10)]
        + [{"match_id": MATCH, "idx": 100, "name": "item_tango"}], [("match_id", "long"), ("idx", "int"), ("name", "string")])
    combat_rows = []
    for value in [10, 20]:
        combat_rows.append({"match_id": MATCH, "tick": 70, "type": "DOTA_COMBATLOG_DAMAGE", "attacker_name_idx": 0,
            "target_name_idx": 5, "value": value, "attacker_hero": True, "target_hero": True,
            "attacker_illusion": False, "target_illusion": False})
    for _ in range(2):
        combat_rows.append({"match_id": MATCH, "tick": 30, "type": "DOTA_COMBATLOG_PURCHASE", "target_name_idx": 0, "value": 100})
    combat_rows.append({"match_id": MATCH, "tick": 80, "type": "DOTA_COMBATLOG_DEATH", "attacker_name_idx": 0,
        "target_name_idx": 5, "attacker_hero": True, "target_hero": True,
        "attacker_illusion": False, "target_illusion": False, "assist_players": "0;1"})
    out["combatlog"] = frame(spark, combat_rows, [("match_id", "long"), ("tick", "int"), ("type", "string"),
        ("attacker_name_idx", "int"), ("target_name_idx", "int"), ("inflictor_name_idx", "int"), ("value", "int"),
        ("attacker_hero", "boolean"), ("target_hero", "boolean"), ("attacker_illusion", "boolean"), ("target_illusion", "boolean"), ("assist_players", "string")])
    out["entity_names"] = frame(spark, [{"match_id": MATCH, "idx": i, "name": f"item_test_{i}"} for i in range(10)],
        [("match_id", "long"), ("idx", "int"), ("name", "string")])
    out["item_entities"] = frame(spark, [{"match_id": MATCH, "tick": 20, "event": "created", "handle": 1000 + i,
        "m_pEntity.m_nameStringTableIndex": i} for i in range(10)], [("match_id", "long"), ("tick", "int"),
        ("event", "string"), ("handle", "int"), ("m_pEntity.m_nameStringTableIndex", "int")])
    return {name: df.cache() for name, df in out.items()}


class SparkCleanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = clean.spark_session()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.raw = fixture(cls.spark)
        cls.result = clean.build_clean(cls.raw)
        cls.report = clean.validate(cls.result, MATCH)

    @classmethod
    def tearDownClass(cls):
        cls.result.release()
        for df in cls.raw.values():
            df.unpersist()
        cls.spark.stop()

    def test_cleaning_preserves_values_and_observed_identifiers(self):
        self.assertEqual(self.report["errors"], [])
        tables = self.result.tables
        self.assertEqual(tables["matches"].first().game_start_tick, 60)
        self.assertEqual(tables["matches"].first().duration_s, 1.)
        self.assertEqual(tables["players"].filter(F.col("player_slot") == 1).first().hero_id, 2)
        damage = tables["damage_events"].first()
        self.assertEqual((damage.damage, damage.hits, damage.inflictor_name), (30, 2, "dota_unknown"))
        purchase = tables["purchases"].first()
        self.assertEqual((purchase.quantity, purchase.game_time_s), (2, -1.))
        self.assertEqual(tables["death_assists"].first().assist_slot, 1)
        self.assertEqual(tables["positions"].filter(F.col("player_slot") == 0).first().x, 10.)
        self.assertEqual(tables["final_inventory"].count(), 10)
        self.assertIsNone(tables["player_stats"].filter(F.col("player_slot") == 0).first().unspent_gold)
        self.assertEqual(self.report["findings"]["excluded_resource_rows"]["rows"], 1)
        for name, ddl in clean.SCHEMAS.items():
            self.assertEqual(tables[name].schema.simpleString(), T.StructType.fromDDL(ddl).simpleString())

    def test_conflicting_deaths_and_mismatches_block_completion(self):
        raw = dict(self.raw)
        extra_death = raw["combatlog"].filter(F.col("type") == "DOTA_COMBATLOG_DEATH").withColumn("attacker_name_idx", F.lit(2))
        raw["combatlog"] = raw["combatlog"].unionByName(extra_death)
        result = clean.build_clean(raw)
        try:
            report = clean.validate(result, MATCH)
            self.assertEqual(report["findings"]["conflicting_deaths"]["rows"], 2)
            self.assertGreater(report["findings"]["scoreboard_disagreements"]["rows"], 0)
            self.assertTrue(report["errors"])
        finally:
            result.release()

    def test_rerun_clears_empty_partitions_and_preserves_other_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(clean.write_result(self.spark, self.result, self.report, tmp))
            purchases_path = Path(tmp) / "purchases" / f"match_id={MATCH}"
            self.assertTrue(list(purchases_path.glob("*.parquet")))
            part = next(purchases_path.glob("*.parquet"))
            import pyarrow.parquet as pq
            self.assertNotIn("match_id", pq.ParquetFile(part).schema.names)
            neighbor = Path(tmp) / "players" / "match_id=99" / "_KEEP"
            neighbor.parent.mkdir(parents=True)
            neighbor.write_text("other match")
            # Exercise writer behavior for allowed empty tables, independently of transform validation.
            changed = clean.CleanResult(tables={**self.result.tables,
                "purchases": self.result.tables["purchases"].limit(0),
                "death_assists": self.result.tables["death_assists"].limit(0)})
            report = {**self.report, "table_rows": {**self.report["table_rows"], "purchases": 0, "death_assists": 0}, "findings": {}}
            self.assertTrue(clean.write_result(self.spark, changed, report, tmp))
            self.assertFalse(purchases_path.exists())
            self.assertFalse((Path(tmp) / "death_assists" / f"match_id={MATCH}").exists())
            self.assertEqual(neighbor.read_text(), "other match")
            self.assertTrue((Path(tmp) / "_complete" / f"match_id={MATCH}" / "_SUCCESS").exists())
            # The blocked path must invalidate old success and keep candidate credit outside clean tables.
            blocked = {**self.report, "errors": ["credit still unresolved"]}
            self.assertFalse(clean.write_result(self.spark, self.result, blocked, tmp))
            self.assertFalse((Path(tmp) / "_complete" / f"match_id={MATCH}").exists())
            self.assertFalse((Path(tmp) / "deaths" / f"match_id={MATCH}").exists())
            saved = json.loads((Path(tmp) / "_review" / f"match_id={MATCH}" / "report.json").read_text())
            self.assertFalse(saved["complete"])

    def test_failed_write_does_not_leave_a_completion_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "_complete" / f"match_id={MATCH}" / "_SUCCESS"
            marker.parent.mkdir(parents=True)
            marker.touch()
            with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    clean.write_result(self.spark, self.result, self.report, tmp)
            self.assertFalse(marker.exists())

    def test_deleted_inventory_and_unfinished_raw_inputs_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "completion marker missing"):
                clean.read_match(self.spark, tmp, MATCH)
        raw = dict(self.raw)
        deletion = raw["item_entities"].filter(F.col("handle") == 1000).withColumn(
            "event", F.lit("deleted")).withColumn("tick", F.lit(80))
        raw["item_entities"] = raw["item_entities"].unionByName(deletion)
        result = clean.build_clean(raw)
        try:
            report = clean.validate(result, MATCH)
            self.assertEqual(report["table_rows"]["final_inventory"], 9)
            self.assertEqual(report["findings"]["unresolved_inventory_handles"]["rows"], 1)
            self.assertTrue(report["errors"])
        finally:
            result.release()


if __name__ == "__main__":
    unittest.main()
