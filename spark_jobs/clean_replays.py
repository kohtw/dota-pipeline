"""Clean one parsed replay. See docs/running-clean-job.md for usage and review handling.

CLI: clean_replays.py <input_base> <output_base> <match_id> [--preview]
No network APIs are used: inputs and outputs can be local directories or s3a://.
"""

import argparse
import json
import os
import posixpath
import sys
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parent.parent
NULL_HANDLE = 16777215
# Keep output definitions explicit: casts must agree with warehouse/init/02_clean_tables.sql.
SCHEMAS = {
    "matches": "match_id long, game_mode int, winner_team int, end_time long, game_start_tick int, duration_s double",
    "players": "match_id long, player_slot int, team int, team_slot int, hero_name string, hero_id int",
    "player_stats": "match_id long, player_slot int, tick int, game_time_s double, level int, kills int, deaths int, assists int, net_worth int, unspent_gold int, total_earned_gold int, total_earned_xp int, last_hits int, denies int, hero_damage double, tower_damage double, healing double",
    "positions": "match_id long, player_slot int, tick int, game_time_s double, x double, y double",
    "damage_events": "match_id long, tick int, game_time_s double, attacker_name string, target_name string, inflictor_name string, attacker_slot int, target_slot int, damage int, hits int",
    "deaths": "match_id long, tick int, game_time_s double, victim_slot int, killer_slot int, killer_name string",
    "death_assists": "match_id long, tick int, victim_slot int, assist_slot int",
    "purchases": "match_id long, tick int, game_time_s double, player_slot int, item_name string, quantity int",
    "final_inventory": "match_id long, player_slot int, inventory_slot int, item_name string",
}
KEYS = {
    "matches": ["match_id"], "players": ["match_id", "player_slot"],
    "player_stats": ["match_id", "player_slot", "tick"], "positions": ["match_id", "player_slot", "tick"],
    "damage_events": ["match_id", "tick", "attacker_name", "target_name", "inflictor_name"],
    "deaths": ["match_id", "tick", "victim_slot"],
    "death_assists": ["match_id", "tick", "victim_slot", "assist_slot"],
    "purchases": ["match_id", "tick", "player_slot", "item_name"],
    "final_inventory": ["match_id", "player_slot", "inventory_slot"],
}
MAY_BE_EMPTY = {"death_assists", "purchases"}
REQUIRED_RAW = ("match", "players", "gamerules", "heroes", "player_resource", "team_data",
                "combatlog", "names", "entity_names", "item_entities")


def col(name):
    """Replay property names contain literal dots, not nested Spark fields."""
    return F.col(f"`{name}`")


def spark_session(s3=False):
    builder = (SparkSession.builder.appName("dota-clean")
               .master(os.environ.get("SPARK_MASTER", "local[2]"))
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if s3:
        jar_dir = Path(os.environ.get("EXTRACTOR_JARS", ROOT / "parser/build/s3a"))
        jars = sorted(jar_dir.glob("*.jar"))
        if not jars:
            raise RuntimeError("S3 connector JARs missing: build the clean image or run parser/gradlew s3aJars")
        builder = builder.config("spark.jars", ",".join(map(str, jars)))
        endpoint = os.environ["S3_ENDPOINT"]
        builder = (builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
                   .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
                   .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
                   .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"]))
    return builder.getOrCreate()


def filesystem(spark, path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath.getFileSystem(spark._jsc.hadoopConfiguration()), jpath


def normalize_path(path):
    """Resolve local aliases before checking whether output can overwrite the raw tree."""
    uri = urlsplit(path)
    if not uri.scheme:
        return str(Path(path).expanduser().resolve())
    if uri.scheme == "file":
        if uri.netloc not in ("", "localhost"):
            raise ValueError("Only local file:// paths are supported")
        return str(Path(unquote(uri.path)).expanduser().resolve())
    if uri.scheme != "s3a" or not uri.netloc:
        raise ValueError("Paths must be local directories or s3a://bucket/prefix")
    return urlunsplit((uri.scheme, uri.netloc, posixpath.normpath(uri.path).rstrip("/"), "", ""))


def delete_path(spark, path):
    fs, jp = filesystem(spark, path)
    if fs.exists(jp) and not fs.delete(jp, True):
        raise OSError(f"Could not delete {path}")


def has_parts(spark, path):
    fs, jp = filesystem(spark, path)
    return fs.exists(jp) and any(s.isFile() and s.getPath().getName().endswith(".parquet")
                                 for s in fs.listStatus(jp))


def read_match(spark, input_base, match_id):
    if not has_parts(spark, f"{input_base}/_complete/match_id={match_id}"):
        raise ValueError(f"Raw completion marker missing for match {match_id}")
    out = {}
    try:
        for name in REQUIRED_RAW:
            base = f"{input_base}/{name}"
            path = f"{base}/match_id={match_id}"
            if not has_parts(spark, path):
                raise ValueError(f"Missing raw table: {path}")
            out[name] = (spark.read.option("basePath", base).parquet(path)
                         .withColumn("match_id", col("match_id").cast("long")).cache())
        return out
    except Exception:
        for df in out.values():
            df.unpersist()
        raise


def duplicates(df, keys):
    return df.groupBy(*keys).count().filter(col("count") > 1)


def latest(df, keys, ties=()):
    window = Window.partitionBy(*keys).orderBy(col("tick").desc(), *[col(c).asc() for c in ties])
    return df.withColumn("_rn", F.row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def resolve(df, names, index, label):
    lookup = names.select("match_id", col("idx").alias(index), col("name").alias(label))
    return df.join(lookup, ["match_id", index], "left")


@dataclass
class CleanResult:
    tables: dict[str, DataFrame] = field(default_factory=dict)
    diagnostics: dict[str, tuple[DataFrame, bool]] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)

    def note(self, name, df, blocking=True):
        self.diagnostics[name] = (df, blocking)

    def release(self):
        for df in self.tables.values():
            df.unpersist()
        for df, _ in self.diagnostics.values():
            df.unpersist()


def build_clean(raw):
    """Build all nine candidate tables; validation decides whether they can be published."""
    result = CleanResult()
    names, headers = raw["names"], raw["players"]
    # Duplicate lookup keys multiply events before aggregation, so stop rather than double-count.
    for name, keys in {"match": ["match_id"], "players": ["match_id", "slot"],
                       "names": ["match_id", "idx"], "entity_names": ["match_id", "idx"]}.items():
        if duplicates(raw[name], keys).limit(1).count():
            raise ValueError(f"Raw {name} has duplicate keys: {keys}")
    if duplicates(headers, ["match_id", "hero_name"]).limit(1).count():
        raise ValueError("Raw players has ambiguous hero names within a match")

    game = raw["gamerules"].select("match_id", "tick",
        col("m_pGameRules.m_nGameState").alias("state"),
        col("m_pGameRules.m_flGameStartTime").cast("double").alias("start"),
        col("m_pGameRules.m_flGameEndTime").cast("double").alias("end"))
    starts = game.groupBy("match_id").agg(F.min(F.when(col("state") == 5, col("tick"))).alias("game_start_tick"))
    clock = latest(game, ["match_id"]).select("match_id", (col("end") - col("start")).alias("duration_s"), "end")
    result.note("invalid_game_clock", clock.filter(col("end").isNull() | (col("end") <= 0)
                | col("duration_s").isNull() | (col("duration_s") <= 0)))
    matches = (raw["match"].select("match_id", "game_mode", col("game_winner").alias("winner_team"), "end_time")
               .join(starts, "match_id", "left").join(clock.drop("end"), "match_id", "left"))
    result.tables["matches"] = matches

    def timed(df):
        return (df.join(starts, "match_id", "left")
                .withColumn("game_time_s", (col("tick") - col("game_start_tick")) / 30.0).drop("game_start_tick"))

    pr = raw["player_resource"].select("match_id", "tick", "index",
        col("m_vecPlayerData.m_iPlayerTeam").alias("team"),
        col("m_vecPlayerTeamData.m_iTeamSlot").alias("team_slot"),
        col("m_vecPlayerTeamData.m_nSelectedHeroID").alias("hero_id"),
        *[col(f"m_vecPlayerTeamData.m_i{suffix}").alias(label) for suffix, label in
          [("Level", "level"), ("Kills", "kills"), ("Deaths", "deaths"), ("Assists", "assists")]])
    valid_pr = col("team").isin(2, 3) & col("team_slot").between(0, 4)
    result.note("excluded_resource_rows", pr.filter(~F.coalesce(valid_pr, F.lit(False))), blocking=False)
    pr = pr.filter(valid_pr).withColumn("player_slot", (col("team") - 2) * 5 + col("team_slot"))
    result.note("duplicate_player_snapshots", duplicates(pr, ["match_id", "player_slot", "tick"]))
    selected = latest(pr, ["match_id", "player_slot"]).select("match_id", "player_slot",
        F.when(col("hero_id") > 0, col("hero_id")).alias("hero_id"))
    players = (headers.select("match_id", col("slot").alias("player_slot"), col("game_team").alias("team"), "hero_name")
               .withColumn("team_slot", col("player_slot") % 5).join(selected, ["match_id", "player_slot"], "left"))
    result.tables["players"] = players
    roster = players.select("match_id", "player_slot", col("team").alias("header_team"))
    result.note("unmapped_resource_rows", pr.join(roster, ["match_id", "player_slot"], "left")
                .filter(col("header_team").isNull() | (col("team") != col("header_team"))))

    td_fields = {
        "m_iNetWorth": "net_worth", "m_iReliableGold": "reliable", "m_iUnreliableGold": "unreliable",
        "m_iTotalEarnedGold": "total_earned_gold", "m_iTotalEarnedXP": "total_earned_xp",
        "m_iLastHitCount": "last_hits", "m_iDenyCount": "denies", "m_flHeroDamage": "hero_damage",
        "m_flTowerDamage": "tower_damage", "m_fHealing": "healing",
    }
    td = raw["team_data"].select("match_id", "tick", col("index").alias("team_slot"),
        F.when(col("class") == "CDOTA_DataRadiant", 2).when(col("class") == "CDOTA_DataDire", 3).alias("team"),
        *[col(f"m_vecDataTeam.{prop}").alias(label) for prop, label in td_fields.items()])
    join_key = ["match_id", "tick", "team", "team_slot"]
    result.note("duplicate_team_snapshots", duplicates(td, join_key))
    result.note("missing_team_snapshots", pr.join(td.select(*join_key), join_key, "left_anti"))
    stats = pr.join(td, join_key, "left").withColumn("unspent_gold", col("reliable") + col("unreliable"))
    result.tables["player_stats"] = timed(stats)

    heroes = raw["heroes"]
    result.note("unknown_replication_marker", heroes.filter(col("m_hReplicatingOtherHeroModel").isNull()))
    real = heroes.filter(col("m_hReplicatingOtherHeroModel") == NULL_HANDLE)
    valid_hero = (col("m_iPlayerID").between(0, 18) & ((col("m_iPlayerID") % 2) == 0))
    result.note("invalid_hero_player_ids", real.filter(~F.coalesce(valid_hero, F.lit(False))))
    real = real.filter(valid_hero).withColumn("player_slot", (col("m_iPlayerID") / 2).cast("int"))
    result.note("unmapped_hero_teams", real.join(roster, ["match_id", "player_slot"], "left")
                .filter(col("header_team").isNull() | (col("m_iTeamNum") != col("header_team"))))
    # The contract chooses lowest handle when multiple real entities represent a player.
    window = Window.partitionBy("match_id", "player_slot", "tick").orderBy(col("handle"))
    real = real.withColumn("_rn", F.row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    positions = real.select("match_id", "player_slot", "tick",
        (col("CBodyComponent.m_cellX") * 128 + col("CBodyComponent.m_vecX") - 16384).alias("x"),
        (col("CBodyComponent.m_cellY") * 128 + col("CBodyComponent.m_vecY") - 16384).alias("y"))
    result.tables["positions"] = timed(positions)

    combat = raw["combatlog"]
    no_illusions = (col("attacker_illusion") == False) & (col("target_illusion") == False)
    hero_damage = (col("type") == "DOTA_COMBATLOG_DAMAGE") & ((col("attacker_hero") == True) | (col("target_hero") == True))
    result.note("damage_unknown_illusion_flags", combat.filter(hero_damage &
                (col("attacker_illusion").isNull() | col("target_illusion").isNull())))
    damage = combat.filter(hero_damage & no_illusions)
    for index, label in [("attacker_name_idx", "attacker_name"), ("target_name_idx", "target_name"),
                         ("inflictor_name_idx", "inflictor_name")]:
        damage = resolve(damage, names, index, label)
    damage = damage.withColumn("inflictor_name", F.when(col("inflictor_name_idx").isNull(), "dota_unknown")
                               .otherwise(col("inflictor_name")))
    result.note("invalid_damage_values", damage.filter(col("value").isNull() | (col("value") < 0)))
    damage = damage.groupBy(*KEYS["damage_events"]).agg(F.sum("value").alias("damage"), F.count("*").alias("hits"))
    for name, slot in [("attacker_name", "attacker_slot"), ("target_name", "target_slot")]:
        damage = damage.join(players.select("match_id", col("hero_name").alias(name), col("player_slot").alias(slot)),
                             ["match_id", name], "left")
    result.tables["damage_events"] = timed(damage)

    purchases = resolve(combat.filter(col("type") == "DOTA_COMBATLOG_PURCHASE"), names, "target_name_idx", "buyer")
    purchases = resolve(purchases, names, "value", "item_name")
    purchases = purchases.join(players.select("match_id", col("hero_name").alias("buyer"), "player_slot"),
                               ["match_id", "buyer"], "left")
    result.tables["purchases"] = timed(purchases.groupBy(*KEYS["purchases"]).agg(F.count("*").alias("quantity")))

    deaths = combat.filter((col("type") == "DOTA_COMBATLOG_DEATH") & (col("target_hero") == True)
                           & no_illusions).dropDuplicates()
    deaths = resolve(resolve(deaths, names, "attacker_name_idx", "killer_name"), names, "target_name_idx", "victim_name")
    for name, slot in [("victim_name", "victim_slot"), ("killer_name", "killer_slot")]:
        deaths = deaths.join(players.select("match_id", col("hero_name").alias(name), col("player_slot").alias(slot)),
                             ["match_id", name], "left")
    conflicts = duplicates(deaths, KEYS["deaths"])
    result.note("conflicting_deaths", deaths.join(conflicts.select(*KEYS["deaths"]), KEYS["deaths"], "inner"))
    deaths = deaths.join(conflicts.select(*KEYS["deaths"]), KEYS["deaths"], "left_anti")
    result.tables["deaths"] = timed(deaths.select(*KEYS["deaths"], "killer_slot", "killer_name"))
    tokens = deaths.select(*KEYS["deaths"], "killer_slot", F.explode(F.split(col("assist_players"), ";")).alias("token"))
    tokens = tokens.withColumn("token", F.trim(col("token")))
    result.note("invalid_assist_tokens", tokens.filter((col("token") != "") & ~col("token").rlike(r"^[0-9]+$")))
    # Observed replay IDs are already 0-9, contrary to the draft contract's /2 assumption.
    assists = tokens.filter(col("token").rlike(r"^[0-9]+$")).withColumn("assist_slot", F.expr("try_cast(token as int)"))
    result.note("invalid_assist_slots", assists.filter(~F.coalesce(col("assist_slot").between(0, 9), F.lit(False))))
    assists = (assists.filter(col("assist_slot").between(0, 9))
               .filter(col("killer_slot").isNull() | (col("assist_slot") != col("killer_slot"))))
    result.tables["death_assists"] = assists.select(*KEYS["death_assists"]).dropDuplicates()

    items = latest(real, ["match_id", "player_slot"], ties=["handle"]).select("match_id", "player_slot", "tick",
        F.explode(F.array(*[F.struct(F.lit(i).alias("inventory_slot"), col(f"m_hItems.{i:04d}").alias("item_handle"))
                            for i in range(17)])).alias("item")).select("match_id", "player_slot", "tick", "item.*")
    items = items.filter(col("item_handle").isNotNull() & (col("item_handle") != NULL_HANDLE))
    entities = raw["item_entities"]
    created = entities.filter(col("event") == "created").select("match_id", col("handle").alias("item_handle"),
        col("tick").alias("created_tick"), col("m_pEntity.m_nameStringTableIndex").alias("idx"))
    deleted = entities.filter(col("event") == "deleted").select("match_id", col("handle").alias("item_handle"),
        col("tick").alias("deleted_tick"))
    candidates = items.join(created, ["match_id", "item_handle"], "inner").filter(col("created_tick") <= col("tick"))
    life_key = ["match_id", "player_slot", "inventory_slot", "item_handle", "created_tick"]
    ended = (candidates.join(deleted, ["match_id", "item_handle"], "inner")
             .filter((col("deleted_tick") >= col("created_tick")) & (col("deleted_tick") <= col("tick")))
             .select(*life_key).distinct())
    active = candidates.join(ended, life_key, "left_anti")
    result.note("unresolved_inventory_handles", items.join(active.select(*KEYS["final_inventory"]), KEYS["final_inventory"], "left_anti"))
    inventory = active.join(raw["entity_names"].select("match_id", "idx", col("name").alias("item_name")), ["match_id", "idx"], "left")
    result.tables["final_inventory"] = inventory

    # Cut long join plans once, and cast the exact output schema before checking or writing.
    for name, df in result.tables.items():
        fields = [entry.strip().split() for entry in SCHEMAS[name].split(",")]
        result.tables[name] = df.select(*[col(c).cast(t).alias(c) for c, t in fields]).localCheckpoint(eager=True)
    compare = latest(result.tables["player_stats"], ["match_id", "player_slot"]).select("match_id", "player_slot", "kills", "deaths", "assists")
    for table, slot, label in [("deaths", "killer_slot", "event_kills"), ("deaths", "victim_slot", "event_deaths"),
                               ("death_assists", "assist_slot", "event_assists")]:
        counts = result.tables[table].groupBy("match_id", slot).count().select("match_id", col(slot).alias("player_slot"), col("count").alias(label))
        compare = compare.join(counts, ["match_id", "player_slot"], "left").fillna({label: 0})
    mismatch = reduce(lambda a, b: a | b, [col(s).isNull() | (col(s) != col(e))
                        for s, e in [("kills", "event_kills"), ("deaths", "event_deaths"), ("assists", "event_assists")]])
    result.note("scoreboard_disagreements", compare.filter(mismatch))
    return result


def validate(result, match_id):
    """Return counts and blocking findings, including output keys and roster constraints."""
    counts, errors = {}, []
    for name, df in result.tables.items():
        counts[name] = df.count()
        if not counts[name] and name not in MAY_BE_EMPTY:
            errors.append(f"{name}: required table is empty")
        required = set(KEYS[name])
        required.update(c for c in ["game_time_s"] if c in df.columns)
        if name == "players":
            required.update(["team", "team_slot", "hero_name"])
        if name == "final_inventory":
            required.add("item_name")
        invalid = reduce(lambda a, b: a | b, [col(c).isNull() for c in required]) | (col("match_id") != match_id)
        if df.filter(invalid).limit(1).count():
            errors.append(f"{name}: missing required fields or wrong match_id")
        if duplicates(df, KEYS[name]).limit(1).count():
            errors.append(f"{name}: duplicate output key")
    if counts["matches"] != 1:
        errors.append("matches: expected exactly one row")
    if result.tables["matches"].filter(~col("winner_team").isin(2, 3) | col("winner_team").isNull()).limit(1).count():
        errors.append("matches: invalid winner")
    players = result.tables["players"]
    if counts["players"] != 10 or players.filter(~col("player_slot").between(0, 9)
            | (col("team") != F.when(col("player_slot") < 5, 2).otherwise(3))
            | (col("team_slot") != col("player_slot") % 5)).limit(1).count():
        errors.append("players: expected slots 0-9 with matching teams and team slots")
    findings = {}
    for name, (df, blocking) in result.diagnostics.items():
        n = df.count()
        findings[name] = {"rows": n, "blocking": blocking}
        if n and blocking:
            errors.append(f"{name}: {n} rows need review")
    return {"match_id": match_id, "table_rows": counts, "findings": findings, "errors": errors}


def write_json(spark, path, payload):
    fs, jp = filesystem(spark, path)
    fs.mkdirs(jp.getParent())
    stream = fs.create(jp, True)
    try:
        stream.write(bytearray(json.dumps(payload, indent=2).encode("utf-8")))
    finally:
        stream.close()


def write_result(spark, result, report, output_base, preview=False):
    """Invalidate the old marker before touching partitions, including empty-table reruns."""
    match_id = report["match_id"]
    marker = f"{output_base}/_complete/match_id={match_id}"
    delete_path(spark, marker)
    delete_path(spark, f"{output_base}/_review/match_id={match_id}")
    complete = not preview and not report["errors"]
    # Delete the selected partitions explicitly: dynamic overwrite alone leaves old empty outputs.
    for name in SCHEMAS:
        delete_path(spark, f"{output_base}/{name}/match_id={match_id}")
    for name, df in result.tables.items():
        destination = f"{output_base}/{name}"
        if name in {"deaths", "death_assists"} and not complete:
            destination = f"{output_base}/_review/match_id={match_id}/candidates/{name}"
        if report["table_rows"][name]:
            df.write.mode("overwrite").partitionBy("match_id").parquet(destination)
    for name, (df, _) in result.diagnostics.items():
        if report["findings"][name]["rows"]:
            df.write.mode("overwrite").parquet(f"{output_base}/_review/match_id={match_id}/{name}")
    report = {**report, "complete": complete, "preview": preview}
    write_json(spark, f"{output_base}/_review/match_id={match_id}/report.json", report)
    if complete:
        fs, jp = filesystem(spark, f"{marker}/_SUCCESS")
        fs.mkdirs(jp.getParent())
        fs.create(jp, True).close()
    return complete


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "clean":
        argv.pop(0)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_base")
    parser.add_argument("output_base")
    parser.add_argument("match_id", type=int)
    parser.add_argument("--preview", action="store_true", help="Write seven tables and candidate/review data, without a completion marker")
    args = parser.parse_args(argv)
    if args.match_id <= 0:
        parser.error("match_id must be positive")
    try:
        input_base, output_base = normalize_path(args.input_base), normalize_path(args.output_base)
    except ValueError as exc:
        parser.error(str(exc))
    # Prevent destructive output placement inside or above the raw tree.
    if (input_base == output_base or input_base.startswith(output_base.rstrip("/") + "/")
            or output_base.startswith(input_base.rstrip("/") + "/")):
        parser.error("Input and output must be separate, non-overlapping directories")
    spark = spark_session(s3=input_base.startswith("s3a://") or output_base.startswith("s3a://"))
    spark.sparkContext.setLogLevel("WARN")
    raw, result = {}, None
    try:
        # A failed rebuild must not leave an older marker advertising stale success.
        delete_path(spark, f"{output_base}/_complete/match_id={args.match_id}")
        raw = read_match(spark, input_base, args.match_id)
        result = build_clean(raw)
        report = validate(result, args.match_id)
        complete = write_result(spark, result, report, output_base, args.preview)
        print(json.dumps({**report, "complete": complete, "preview": args.preview}, indent=2))
        return 0 if complete or args.preview else 2
    finally:
        if result:
            result.release()
        for df in raw.values():
            df.unpersist()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
