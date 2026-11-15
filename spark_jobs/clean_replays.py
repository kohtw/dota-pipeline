"""Spark clean job using per-tick scoreboard credit for deaths and assists.

Run from the repository root:
  python spark_jobs/clean_replays.py data/parsed data/output/clean_kda MATCH_ID

Requires raw kda_changes in addition to the existing parsed tables. See
docs/running-kda-clean-job.md for its schema and the output contract additions.
Spark reads, resolves and validates the tables and writes partitioned Parquet.
The bounded attribution solver processes one match's small counter/event lists
on the driver, as proposed in the credit plan; the replay itself stays in Spark.
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from functools import reduce
import os
import posixpath
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit
from itertools import product

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parent.parent
NULL_HANDLE = 16777215
# Keep output definitions explicit: casts must agree with warehouse/init/02_clean_tables.sql.
SCHEMAS = {
    "matches": "match_id long, game_mode int, winner_team int, end_time long, game_start_tick int, duration_s double, assist_attribution_complete boolean, assist_attribution_assumed boolean",
    "players": "match_id long, player_slot int, team int, team_slot int, hero_name string, hero_id int",
    "player_stats": "match_id long, player_slot int, tick int, game_time_s double, level int, kills int, deaths int, assists int, net_worth int, unspent_gold int, total_earned_gold int, total_earned_xp int, last_hits int, denies int, hero_damage double, tower_damage double, healing double",
    "positions": "match_id long, player_slot int, tick int, game_time_s double, x double, y double",
    "damage_events": "match_id long, tick int, game_time_s double, attacker_name string, target_name string, inflictor_name string, attacker_slot int, target_slot int, damage int, hits int",
    "deaths": "match_id long, tick int, game_time_s double, victim_slot int, killer_slot int, killer_name string, credited_killer_slot int, link_status string",
    "death_assists": "match_id long, tick int, victim_slot int, assist_slot int, same_side boolean",
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


def _read_base_tables(spark, input_base, match_id):
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
    attribution: dict = field(default_factory=dict)

    def note(self, name, df, blocking=True):
        self.diagnostics[name] = (df, blocking)

    def release(self):
        for df in self.tables.values():
            df.unpersist()
        for df, _ in self.diagnostics.values():
            df.unpersist()


def _build_replay_tables(raw):
    """Build the seven non-credit tables; counter attribution adds deaths and assists."""
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
    result.tables["matches"] = (matches
        .withColumn("assist_attribution_complete", F.lit(True))
        .withColumn("assist_attribution_assumed", F.lit(False)))

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

    # heroes = raw["heroes"]
    # These summons share the hero class prefix but lack hero properties.
    summon_classes = [
        "CDOTA_Unit_Hero_Beastmaster_Boar",
        "CDOTA_Unit_Hero_Beastmaster_Hawk",
    ]
    heroes = raw["heroes"].filter(~col("class").isin(*summon_classes))
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
        if name == "matches":
            required.update(["assist_attribution_complete", "assist_attribution_assumed"])
        if name == "final_inventory":
            required.add("item_name")
        if name == "deaths":
            required.add("link_status")
        if name == "death_assists":
            required.add("same_side")
        invalid = reduce(lambda a, b: a | b, [col(c).isNull() for c in required]) | (col("match_id") != match_id)
        if df.filter(invalid).limit(1).count():
            errors.append(f"{name}: missing required fields or wrong match_id")
        if duplicates(df, KEYS[name]).limit(1).count():
            errors.append(f"{name}: duplicate output key")
        if name == "deaths":
            if df.filter(~col("link_status").isin("exact", "participant_assumed", "same_side_possible")
                         | (col("credited_killer_slot").isNotNull()
                            & ~col("credited_killer_slot").between(0, 9))).limit(1).count():
                errors.append("deaths: invalid credit label or credited killer slot")
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
    return {"match_id": match_id, "table_rows": counts, "findings": findings,
            "errors": errors, "assist_attribution": result.attribution}


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


class CreditError(ValueError):
    """Inputs or attribution cannot establish consistent scoreboard credit."""


@dataclass
class CreditResult:
    deaths: list[dict]
    death_assists: list[dict]
    review: list[dict]
    unresolved_assists: dict[int, int] = field(default_factory=dict)
    ambiguous_groups: list[dict] = field(default_factory=list)


STATS = ("kills", "deaths", "assists")


def _integer(value, label):
    if isinstance(value, bool) or not isinstance(value, int):
        raise CreditError(f"{label} must be an integer")
    return value


def credit_kda(players, counters, deaths, final_scoreboard, *, window_ticks=4,
               max_search_states=100_000):
    """Return established credit and explicitly withheld assist counts.

    Inputs are normalized records for one match: players have player_slot/team;
    counters have tick/player_slot/kills/deaths/assists; deaths have tick,
    victim_slot, explicit illusion flags, optional last hitter/source owner and
    a list of assist slots. final_scoreboard supplies separately sampled final K/D/A.
    Counters require the initial zero row and every subsequent per-tick change.

    Search is bounded per connected timing window. Exhaustion blocks rather than
    assuming that the first solution is unique. Counter deltas greater than one
    are supported when several deaths occur at the same tick. Unresolved groups
    publish deaths only after proving unique kill credit; their assists are
    withheld and accounted for against the same final totals.
    """
    if _integer(window_ticks, "window_ticks") < 0:
        raise CreditError("window_ticks must be nonnegative")
    if _integer(max_search_states, "max_search_states") < 1:
        raise CreditError("max_search_states must be positive")
    teams = {}
    for row in players:
        slot = _integer(row["player_slot"], "player_slot")
        team = _integer(row["team"], "team")
        if slot not in range(10) or team != 2 + slot // 5 or slot in teams:
            raise CreditError("Roster must have unique normalized slots and matching teams")
        teams[slot] = team
    if set(teams) != set(range(10)):
        raise CreditError("Expected roster slots 0-9")

    histories = defaultdict(list)
    for row in counters:
        slot = _integer(row["player_slot"], "counter player_slot")
        if slot not in teams:
            raise CreditError("Counter references unknown player")
        histories[slot].append(row)
    tokens = Counter()
    last = {}
    for slot in teams:
        rows = sorted(histories[slot], key=lambda r: r["tick"])
        if not rows:
            raise CreditError(f"Missing counter history for slot {slot}")
        previous = (0, 0, 0)
        previous_tick = None
        for index, row in enumerate(rows):
            tick = _integer(row["tick"], "counter tick")
            values = tuple(_integer(row[s], s) for s in STATS)
            if previous_tick is not None and tick <= previous_tick:
                raise CreditError(f"Duplicate counter tick for slot {slot}")
            if index == 0 and values != (0, 0, 0):
                raise CreditError(f"Counters must start at zero for slot {slot}")
            for stat, old, new in zip(STATS, previous, values):
                if new < old:
                    raise CreditError(f"Counter goes down: slot {slot} {stat}")
                if new > old:
                    tokens[stat, slot, tick] += new - old
            previous, previous_tick = values, tick
        last[slot] = previous

    final = {}
    for row in final_scoreboard:
        slot = _integer(row["player_slot"], "scoreboard player_slot")
        if slot not in teams or slot in final:
            raise CreditError("Scoreboard has unknown or duplicate player")
        final[slot] = tuple(_integer(row[s], s) for s in STATS)
    if final != last:
        raise CreditError("Final scoreboard disagrees with counter history")

    review = []
    groups = defaultdict(list)
    for raw in deaths:
        row = dict(raw)
        tick = _integer(row["tick"], "death tick")
        victim = _integer(row["victim_slot"], "victim_slot")
        if victim not in teams:
            raise CreditError("Death references unknown victim")
        if any(type(row.get(flag)) is not bool
               for flag in ("attacker_illusion", "target_illusion")):
            raise CreditError("Explicit illusion flags are required")
        if row["target_illusion"]:
            review.append({"tick": tick, "victim_slot": victim, "reason": "illusion_victim"})
            continue
        for key in ("killer_slot", "source_owner_slot"):
            if row.get(key) is not None:
                value = _integer(row[key], key)
                if value not in teams:
                    raise CreditError(f"Unknown {key}")
        participants = row.get("assist_players", [])
        if not isinstance(participants, (list, tuple, set)):
            raise CreditError("assist_players must contain normalized integer slots")
        participants = tuple(sorted(set(_integer(p, "assist slot") for p in participants)))
        if any(p not in teams for p in participants):
            raise CreditError("Unknown assist slot")
        row["assist_players"] = participants
        groups[tick, victim].append(row)

    events = []
    for (tick, victim), rows in sorted(groups.items()):
        # Meepo may emit a self-source duplicate alongside the actual attacker.
        nonself = [r for r in rows
                   if (r.get("source_owner_slot") if r.get("source_owner_slot") is not None
                       else r.get("killer_slot")) != victim]
        preferred = nonself or rows
        signatures = {(r.get("killer_slot"), r.get("killer_name"),
                       r.get("source_owner_slot"), r["attacker_illusion"]) for r in preferred}
        if len(signatures) != 1:
            raise CreditError(f"Conflicting death sources at tick {tick}, victim {victim}")
        row = dict(preferred[0])
        row["assist_players"] = tuple(sorted({p for r in preferred for p in r["assist_players"]}))
        if len(rows) > 1:
            review.append({"tick": tick, "victim_slot": victim, "reason": "merged_death_records"})
        if not any(stat == "deaths" and slot == victim and tick <= ct <= tick + window_ticks
                   for stat, slot, ct in tokens):
            review.append({"tick": tick, "victim_slot": victim, "reason": "no_death_counter_rise"})
        else:
            events.append(row)

    # Independent timing components keep simultaneous fights local to the solver.
    components = []
    for event in events:
        if not components or event["tick"] > components[-1][-1]["tick"] + window_ticks:
            components.append([])
        components[-1].append(event)
    buckets = [Counter() for _ in components]
    for token, count in tokens.items():
        stat, slot, tick = token
        possible = [i for i, component in enumerate(components)
                    if any(e["tick"] <= tick <= e["tick"] + window_ticks
                           and (stat != "deaths" or e["victim_slot"] == slot) for e in component)]
        if len(possible) != 1:
            raise CreditError(f"Counter increase has no matching death window: {token}")
        buckets[possible[0]][token] = count

    output, assists = [], []
    unresolved_assists = Counter()
    ambiguous_groups = []
    for component, budget in zip(components, buckets):
        allow_same_side = False
        participant_fallback = False
        primary = _solve(component, budget, teams, window_ticks, max_search_states)
        if not primary:
            # Actual same-team credit is allowed, but must still be uniquely determined.
            allow_same_side = True
            primary = _solve(component, budget, teams, window_ticks, max_search_states,
                             same_side=True)
        if len(primary) > 1:
            # Lists are incomplete for some unique counter-backed assists. Use
            # them only to resolve ambiguity, and keep the assumption visible.
            restricted = _solve(
                component, budget, teams, window_ticks, max_search_states,
                same_side=allow_same_side, restrict_assist_participants=True)
            if len(restricted) == 1:
                primary = restricted
                participant_fallback = True
        if not primary:
            raise CreditError("No consistent credit answer")
        chosen = primary[0]
        partial = len(primary) > 1
        if partial:
            # Two matching answers are not proof of unique kills. Search for an
            # alternative killer on every death, including relaxed team/participant
            # rules. Exhaustion still raises; no partial output is then published.
            for index in range(len(component)):
                if _solve(component, budget, teams, window_ticks, max_search_states,
                          same_side=True, restrict_participants=False,
                          different_from=chosen, different_index=index,
                          different_killer_only=True):
                    raise CreditError("Ambiguous kill credit assignment")
            withheld = Counter()
            for (stat, slot, _), count in budget.items():
                if stat == "assists":
                    withheld[slot] += count
            unresolved_assists.update(withheld)
            ambiguous_groups.append({
                "first_tick": component[0]["tick"], "last_tick": component[-1]["tick"],
                "deaths": [{"tick": e["tick"], "victim_slot": e["victim_slot"],
                            "assist_players": list(e["assist_players"])} for e in component],
                "unresolved_assists": dict(withheld),
                "counter_increases": [{"stat": stat, "player_slot": slot, "tick": tick,
                                       "count": count}
                                      for (stat, slot, tick), count in budget.items()],
            })
        for index, (event, (killer, credited_assists)) in enumerate(zip(component, chosen)):
            status = "exact"
            # Search for an alternative to THIS link, not just the first two
            # alternatives elsewhere in a simultaneous fight.
            if partial:
                review.append({"tick": event["tick"], "victim_slot": event["victim_slot"],
                               "reason": "unresolved_assist_attribution"})
            elif participant_fallback:
                # The participant assumption affects the whole timing component.
                status = "participant_assumed"
            elif _solve(component, budget, teams, window_ticks, max_search_states,
                      same_side=True, different_from=chosen, different_index=index):
                status = "same_side_possible"
            elif _solve(component, budget, teams, window_ticks, max_search_states,
                        restrict_participants=False, same_side=True,
                        different_from=chosen, different_index=index):
                status = "participant_assumed"
            row = {"tick": event["tick"], "victim_slot": event["victim_slot"],
                   "killer_slot": event.get("killer_slot"), "killer_name": event.get("killer_name"),
                   "credited_killer_slot": killer, "link_status": status}
            output.append(row)
            for slot in ([] if partial else credited_assists):
                assists.append({"tick": event["tick"], "victim_slot": event["victim_slot"],
                                "assist_slot": slot, "same_side": teams[slot] == teams[event["victim_slot"]]})

    totals = {slot: [0, 0, 0] for slot in teams}
    for row in output:
        totals[row["victim_slot"]][1] += 1
        if row["credited_killer_slot"] is not None:
            totals[row["credited_killer_slot"]][0] += 1
    for row in assists:
        totals[row["assist_slot"]][2] += 1
    for slot, count in unresolved_assists.items():
        totals[slot][2] += count
    if {slot: tuple(value) for slot, value in totals.items()} != final:
        raise CreditError("Final scoreboard disagrees with credited totals")
    return CreditResult(output, assists, review, dict(unresolved_assists), ambiguous_groups)


def _solve(events, budget, teams, window, limit, *, restrict_participants=True,
           restrict_assist_participants=False, same_side=False,
           different_from=None, different_index=None, different_killer_only=False):
    """Search counter-token allocations; distinguish links, not token permutations."""
    remaining = budget.copy()
    answers = set()
    states = 0

    def visit(index, links):
        nonlocal states
        states += 1
        if states > limit:
            raise CreditError("Credit search limit exceeded; attribution not established")
        if len(answers) >= (1 if different_from is not None else 2):
            return
        if index == len(events):
            answer = tuple(links)
            differs = (different_from is None or
                       (answer[different_index][0] != different_from[different_index][0]
                        if different_killer_only else
                        answer[different_index] != different_from[different_index]))
            if not any(remaining.values()) and differs:
                answers.add(answer)
            return
        event = events[index]
        tick, victim = event["tick"], event["victim_slot"]
        # A remaining token must be usable by some unprocessed event.
        for (stat, slot, ct), count in remaining.items():
            if count and not any(e["tick"] <= ct <= e["tick"] + window
                                 and (stat != "deaths" or e["victim_slot"] == slot)
                                 for e in events[index:]):
                return
        available = defaultdict(list)
        for token, count in remaining.items():
            stat, slot, ct = token
            if count and tick <= ct <= tick + window:
                available[stat, slot].append(token)
        death_options = available["deaths", victim]
        if not death_options:
            return
        owner = event.get("source_owner_slot")
        if owner is None and not event["attacker_illusion"]:
            owner = event.get("killer_slot")
        enemy_owner = owner is not None and teams[owner] != teams[victim]
        killers = [None]
        for slot in teams:
            if not available["kills", slot] or slot == victim:
                continue
            if not same_side and teams[slot] == teams[victim]:
                continue
            if enemy_owner and slot != owner:
                continue
            if (not enemy_owner and restrict_participants and event["assist_players"]
                    and slot not in event["assist_players"]):
                continue
            killers.append(slot)
        if enemy_owner:
            killers = [k for k in killers if k == owner]
        for killer in killers:
            if (different_killer_only and index == different_index
                    and killer == different_from[index][0]):
                continue
            participants = set(event["assist_players"])

            assist_slots = [
                s for s in teams
                if s != killer
                and available["assists", s]
                and (same_side or teams[s] != teams[victim])
                and (not restrict_assist_participants or s in participants)
            ]
            for mask in product((False, True), repeat=len(assist_slots)):
                selected = tuple(s for s, keep in zip(assist_slots, mask) if keep)
                options = [death_options]
                if killer is not None:
                    options.append(available["kills", killer])
                options.extend(available["assists", s] for s in selected)
                for used in product(*options):
                    for token in used:
                        remaining[token] -= 1
                    visit(index + 1, [*links, (killer, selected)])
                    for token in used:
                        remaining[token] += 1
                    if len(answers) >= (1 if different_from is not None else 2):
                        return

    visit(0, [])
    return sorted(answers, key=repr)


COUNTER_PROPS = {
    "m_vecPlayerData.m_iPlayerTeam": "team",
    "m_vecPlayerTeamData.m_iTeamSlot": "team_slot",
    "m_vecPlayerTeamData.m_nSelectedHeroID": "hero_id",
    "m_vecPlayerTeamData.m_iKills": "kills",
    "m_vecPlayerTeamData.m_iDeaths": "deaths",
    "m_vecPlayerTeamData.m_iAssists": "assists",
}
DEATH_SCHEMA = SCHEMAS["deaths"]
ASSIST_SCHEMA = SCHEMAS["death_assists"]
REVIEW_SCHEMA = "match_id long, tick int, victim_slot int, reason string"


def require_columns(df, names, label):
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise CreditError(f"{label}: missing columns {missing}")


def reject_rows(df, condition, message):
    if df.filter(condition).limit(1).count():
        raise CreditError(message)


def read_match(spark, input_base, match_id):
    """Require the new input before starting the existing nine-table transform."""
    path = f"{input_base}/kda_changes/match_id={match_id}"
    if not has_parts(spark, path):
        raise CreditError(
            f"Missing per-tick kda_changes: {path}. Re-parse with per-tick, "
            "changes-only K/D/A extraction; one-second snapshots cannot replace it.")
    raw = _read_base_tables(spark, input_base, match_id)
    try:
        changes = spark.read.parquet(path)
        if "match_id" in changes.columns:
            reject_rows(changes, F.col("match_id").isNull() | (F.col("match_id") != match_id),
                        "kda_changes: embedded match_id disagrees with partition")
        raw["kda_changes"] = changes.withColumn("match_id", F.lit(match_id).cast("long")).cache()
        return raw
    except Exception:
        for df in raw.values():
            df.unpersist()
        raise


def normalize_counters(raw, players):
    """Map resource array indices to stable clean slots using Spark joins.

    Initial rows can precede team assignment. Recover their slot using the index's
    later assignment, but reject index reuse or multiple indices for one player.
    """
    require_columns(raw, ["match_id", "tick", "index", *COUNTER_PROPS], "kda_changes")
    counters = raw.select("match_id", "tick", "index",
                          *[col(p).alias(c) for p, c in COUNTER_PROPS.items()])
    assigned = counters.filter(F.col("team").isin(2, 3))
    reject_rows(assigned, F.col("team_slot").isNull() | ~F.col("team_slot").between(0, 4),
                "kda_changes: invalid team slot")
    mapping = (assigned.select("match_id", "index", "team", "team_slot")
               .distinct().withColumn("player_slot", (F.col("team") - 2) * 5 + F.col("team_slot")))
    for keys in (["match_id", "index"], ["match_id", "player_slot"]):
        if duplicates(mapping, keys).limit(1).count():
            raise CreditError("kda_changes: player index maps to more than one slot or vice versa")
    roster = players.select("match_id", "player_slot", F.col("team").alias("roster_team"))
    reject_rows(mapping.join(roster, ["match_id", "player_slot"], "left"),
                F.col("roster_team").isNull() | (F.col("team") != F.col("roster_team")),
                "kda_changes: assignment disagrees with the player roster")
    # Draft/selection rows can contain temporary or placeholder hero IDs. Check
    # the final identity against the independently sampled final roster instead.
    if "hero_id" in players.columns:
        identities = players.select("match_id", "player_slot", F.col("hero_id").alias("roster_hero"))
        observed = counters.join(mapping.select("match_id", "index", "player_slot"),
                                 ["match_id", "index"], "inner").join(
                                     identities, ["match_id", "player_slot"], "left")
        observed = latest(observed, ["match_id", "player_slot"])
        reject_rows(observed, F.col("roster_hero").isNotNull()
                    & (F.col("hero_id").isNull() | (F.col("hero_id") != F.col("roster_hero"))),
                    "kda_changes: hero identity disagrees with the player roster")
    return (counters.join(mapping.select("match_id", "index", "player_slot"),
                          ["match_id", "index"], "inner")
            .select("match_id", "tick", "player_slot", "kills", "deaths", "assists"))


def normalize_deaths(combat, names, players):
    """Resolve direct attacker, damage source owner and victim without illusion filtering."""
    require_columns(combat, ["match_id", "tick", "type", "target_hero", "attacker_illusion",
                             "target_illusion", "attacker_name_idx", "target_name_idx",
                             "damage_source_name_idx", "assist_players"], "combatlog")
    deaths = combat.filter((F.col("type") == "DOTA_COMBATLOG_DEATH")
                           & (F.col("target_hero") == True))
    for index, name in [("attacker_name_idx", "killer_name"),
                        ("target_name_idx", "victim_name"),
                        ("damage_source_name_idx", "source_name")]:
        deaths = resolve(deaths, names, index, name)
    for name, slot in [("killer_name", "killer_slot"), ("victim_name", "victim_slot"),
                       ("source_name", "source_owner_slot")]:
        lookup = players.select("match_id", F.col("hero_name").alias(name), F.col("player_slot").alias(slot))
        deaths = deaths.join(lookup, ["match_id", name], "left")
    reject_rows(deaths, F.col("victim_slot").isNull(), "combatlog: unresolved hero victim")
    # Raw assist IDs are already 0-9. Preserve invalid tokens so they fail,
    # rather than disappearing in a cast or silently changing their meaning.
    deaths = deaths.withColumn("_tokens", F.filter(
        F.transform(F.split(F.coalesce(F.col("assist_players"), F.lit("")), ";"),
                    lambda x: F.trim(x)), lambda x: x != ""))
    reject_rows(deaths, F.exists(F.col("_tokens"), lambda x: ~x.rlike("^[0-9]$")),
                "combatlog: invalid assist player IDs; expected normalized slots 0-9")
    return deaths.select("tick", "victim_slot", "killer_slot", "killer_name", "source_owner_slot",
                         "attacker_illusion", "target_illusion",
                         F.transform(F.col("_tokens"), lambda x: x.cast("int")).alias("assist_players"))


def build_clean(raw, match_id, *, window_ticks=4, max_search_states=100_000):
    """Return all nine clean Spark DataFrames, with counter-based credit columns."""
    if not isinstance(match_id, int) or isinstance(match_id, bool) or match_id <= 0:
        raise CreditError("Expected a positive match_id")
    if "kda_changes" not in raw:
        raise CreditError("Missing per-tick kda_changes; re-parsing is required")
    for name, df in raw.items():
        require_columns(df, ["match_id"], name)
        reject_rows(df, F.col("match_id").isNull() | (F.col("match_id") != match_id),
                    f"{name}: missing or wrong match_id")
    headers = raw["match"].select("match_id").collect()
    if len(headers) != 1:
        raise CreditError("Expected exactly one match header")

    # Build the seven replay tables, then add counter-established death credit.
    result = _build_replay_tables(raw)
    try:
        players = result.tables["players"]
        counters = normalize_counters(raw["kda_changes"], players)
        death_events = normalize_deaths(raw["combatlog"], raw["names"], players)
        scoreboard = latest(result.tables["player_stats"], ["match_id", "player_slot"])
        credited = credit_kda(
            [r.asDict() for r in players.select("player_slot", "team").collect()],
            [r.asDict() for r in counters.drop("match_id").collect()],
            [r.asDict() for r in death_events.collect()],
            [r.asDict() for r in scoreboard.select("player_slot", "kills", "deaths", "assists").collect()],
            window_ticks=window_ticks, max_search_states=max_search_states)
        start = raw["gamerules"].filter(col("m_pGameRules.m_nGameState") == 5).agg(
            F.min("tick").alias("tick")).first().tick
        if start is None:
            raise CreditError("No game-start tick found")
        spark = players.sparkSession
        deaths = [{**row, "match_id": match_id, "game_time_s": (row["tick"] - start) / 30.0}
                  for row in credited.deaths]
        assists = [{**row, "match_id": match_id} for row in credited.death_assists]
        result.tables["deaths"] = spark.createDataFrame(deaths, DEATH_SCHEMA).cache()
        result.tables["death_assists"] = spark.createDataFrame(assists, ASSIST_SCHEMA).cache()
        complete = not credited.ambiguous_groups
        assumed = any(row["link_status"] != "exact" for row in credited.deaths)
        result.tables["matches"] = (result.tables["matches"]
            .withColumn("assist_attribution_complete", F.lit(complete))
            .withColumn("assist_attribution_assumed", F.lit(assumed)))
        result.attribution = {
            "complete": complete, "assumed": assumed,
            "unresolved_assists": credited.unresolved_assists,
            "ambiguous_groups": credited.ambiguous_groups,
        }
        result.note("kda_credit_review", spark.createDataFrame(
            [{**r, "match_id": match_id} for r in credited.review], REVIEW_SCHEMA), blocking=False)
        result.note("kda_assumed_links", result.tables["deaths"].filter(F.col("link_status") != "exact"),
                    blocking=False)
        result.note("kda_same_side_assists", result.tables["death_assists"].filter("same_side"), blocking=False)
        result.note("scoreboard_disagreements", scoreboard_disagreements(
            result.tables, credited.unresolved_assists))
        return result
    except Exception:
        result.release()
        raise


def scoreboard_disagreements(tables, unresolved_assists=None):
    """Require exact kills/deaths and account for every withheld assist."""
    comparison = latest(tables["player_stats"], ["match_id", "player_slot"]).select(
        "match_id", "player_slot", "kills", "deaths", "assists")
    for table, slot, label in [("deaths", "credited_killer_slot", "event_kills"),
                               ("deaths", "victim_slot", "event_deaths"),
                               ("death_assists", "assist_slot", "event_assists")]:
        counts = (tables[table].filter(F.col(slot).isNotNull()).groupBy("match_id", slot).count()
                  .select("match_id", F.col(slot).alias("player_slot"), F.col("count").alias(label)))
        comparison = comparison.join(counts, ["match_id", "player_slot"], "left").fillna({label: 0})
    unresolved = F.lit(0)
    for slot, count in (unresolved_assists or {}).items():
        if slot not in range(10) or not isinstance(count, int) or count < 0:
            raise CreditError("Invalid unresolved assist accounting")
        unresolved = F.when(F.col("player_slot") == slot, F.lit(count)).otherwise(unresolved)
    comparison = comparison.withColumn("unresolved_assists", unresolved)
    return comparison.filter(
        F.col("kills").isNull() | F.col("deaths").isNull() | F.col("assists").isNull()
        | (F.col("kills") != F.col("event_kills"))
        | (F.col("deaths") != F.col("event_deaths"))
        | (F.col("assists") != F.col("event_assists") + F.col("unresolved_assists")))


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "clean":
        argv.pop(0)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_base")
    parser.add_argument("output_base")
    parser.add_argument("match_id", type=int)
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--window-ticks", type=int, default=4)
    parser.add_argument("--max-search-states", type=int, default=100_000)
    args = parser.parse_args(argv)
    if args.match_id <= 0 or args.window_ticks < 0 or args.max_search_states < 1:
        parser.error("match_id/search limit must be positive and window ticks nonnegative")
    input_base, output_base = normalize_path(args.input_base), normalize_path(args.output_base)
    if (input_base == output_base or input_base.startswith(output_base + "/")
            or output_base.startswith(input_base + "/")):
        parser.error("Input and output must be separate, non-overlapping directories")
    spark = spark_session(s3=input_base.startswith("s3a://") or output_base.startswith("s3a://"))
    spark.sparkContext.setLogLevel("WARN")
    raw, result = {}, None
    try:
        delete_path(spark, f"{output_base}/_complete/match_id={args.match_id}")
        raw = read_match(spark, input_base, args.match_id)
        result = build_clean(raw, args.match_id, window_ticks=args.window_ticks,
                             max_search_states=args.max_search_states)
        report = {**validate(result, args.match_id), "credit_method": "per_tick_counters",
                  "contract_version": "kda-credit-v2", "window_ticks": args.window_ticks}
        complete = write_result(spark, result, report, output_base, args.preview)
        print(json.dumps({**report, "complete": complete, "preview": args.preview}, indent=2))
        return 0 if complete or args.preview else 2
    except CreditError as exc:
        report = {"match_id": args.match_id, "complete": False, "preview": args.preview,
                  "credit_method": "per_tick_counters", "errors": [str(exc)]}
        write_json(spark, f"{output_base}/_review/match_id={args.match_id}/report.json", report)
        print(json.dumps(report, indent=2), file=sys.stderr)
        return 2
    finally:
        if result:
            result.release()
        for df in raw.values():
            df.unpersist()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
