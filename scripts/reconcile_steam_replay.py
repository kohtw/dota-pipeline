"""
Cross-check each player's hero and K/D/A between our replay parse and Valve's match summary.

  Replay: the last parsed player_resource snapshot per player (parsed/player_resource in Garage),
          for every match with a parsed/_complete marker, so matches the clean job rejected count too.
  Steam:  stg.steam_players, as loaded by steam_summary_elt.

Next to each match it shows the clean job's own check from clean/_review/.../report.json
(scoreboard_disagreements: players whose player_resource K/D/A differ from the death events).

Run inside the scheduler container, which has boto3, psycopg2 and pyarrow:
  docker cp scripts/reconcile_steam_replay.py dota-pipeline-airflow-scheduler-1:/tmp/
  docker exec -e PYTHONPATH=/opt/airflow/dags dota-pipeline-airflow-scheduler-1 \\
      python /tmp/reconcile_steam_replay.py /data/reports/steam-replay-reconciliation.csv
"""

import argparse
import csv
import json
import os
import re

STATS = ("hero_id", "kills", "deaths", "assists")
RESOURCE = "m_vecPlayerTeamData."
RESOURCE_COLUMNS = ["tick", "m_vecPlayerData.m_iPlayerTeam", f"{RESOURCE}m_iTeamSlot",
                    f"{RESOURCE}m_nSelectedHeroID", f"{RESOURCE}m_iKills", f"{RESOURCE}m_iDeaths",
                    f"{RESOURCE}m_iAssists"]


def clean_slot(steam_slot: int) -> int:
    """Steam's player_slot (0-4 Radiant, 128-132 Dire) as the clean player_slot (0-9)."""
    return steam_slot - 128 + 5 if steam_slot >= 128 else steam_slot


def final_scoreboard(match_id: int, resource_rows: list[dict]) -> list[dict]:
    """Hero and K/D/A per player from the last player_resource snapshot of each (team, team_slot)."""
    last: dict[int, dict] = {}
    for r in resource_rows:
        team = r["m_vecPlayerData.m_iPlayerTeam"]
        if team not in (2, 3):
            continue
        slot = (team - 2) * 5 + r[f"{RESOURCE}m_iTeamSlot"]
        if slot not in last or r["tick"] > last[slot]["tick"]:
            last[slot] = r
    return [{"match_id": match_id, "player_slot": slot,
             "hero_id": r[f"{RESOURCE}m_nSelectedHeroID"], "kills": r[f"{RESOURCE}m_iKills"],
             "deaths": r[f"{RESOURCE}m_iDeaths"], "assists": r[f"{RESOURCE}m_iAssists"]}
            for slot, r in sorted(last.items())]


def compare(replay_rows: list[dict], steam_rows: list[dict]) -> list[dict]:
    """One row per player seen by either source, keyed on the clean player_slot. Values are compared
    only when both sources have the player; a missing value then counts as a mismatch."""
    replay = {(r["match_id"], r["player_slot"]): r for r in replay_rows}
    steam = {(r["match_id"], clean_slot(r["player_slot"])): r for r in steam_rows}
    rows = []
    for match_id, slot in sorted(replay.keys() | steam.keys()):
        r, s = replay.get((match_id, slot)), steam.get((match_id, slot))
        in_both = r is not None and s is not None
        row = {"match_id": match_id, "player_slot": slot,
               "steam_player_slot": s["player_slot"] if s else None, "in_both": in_both}
        mismatched = []
        for stat in STATS:
            ours, valve = (r or {}).get(stat), (s or {}).get(stat)
            row[f"{stat}_replay"], row[f"{stat}_steam"] = ours, valve
            if in_both and (ours is None or ours != valve):
                mismatched.append(stat)
        compared = len(STATS) if in_both else 0
        row.update(values_compared=compared, values_equal=compared - len(mismatched),
                   all_equal=in_both and not mismatched, mismatched=",".join(mismatched))
        rows.append(row)
    return rows


def summarize(rows: list[dict]) -> dict:
    """Totals over the players both sources have."""
    both = [r for r in rows if r["in_both"]]
    return {"matches": len({r["match_id"] for r in both}), "players": len(both),
            "values_compared": sum(r["values_compared"] for r in both),
            "values_equal": sum(r["values_equal"] for r in both),
            "players_all_equal": sum(r["all_equal"] for r in both)}


def markdown(rows: list[dict], clean: dict[int, dict]) -> str:
    total = summarize(rows)
    lines = ["| Matches | Players | Values compared | Equal | Players fully equal |", "|---|---|---|---|---|",
             f"| {total['matches']} | {total['players']} | {total['values_compared']} | "
             f"{total['values_equal']} | {total['players_all_equal']} |", "",
             "| Match | Steam rows | Players compared | Equal / compared | Mismatched | Clean result | "
             "Clean scoreboard disagreements |", "|---|---|---|---|---|---|---|"]
    for match_id in sorted({r["match_id"] for r in rows}):
        mine = [r for r in rows if r["match_id"] == match_id]
        s = summarize(mine)
        mismatched = "; ".join(f"slot {r['player_slot']}: {r['mismatched']}" for r in mine if r["mismatched"])
        steam_rows = sum(r["steam_player_slot"] is not None for r in mine)
        c = clean.get(match_id, {})
        lines.append(f"| {match_id} | {steam_rows} | {s['players']} | {s['values_equal']} / {s['values_compared']} | "
                     f"{mismatched or '–'} | {c.get('result', 'no report')} | "
                     f"{'–' if c.get('disagreements') is None else c['disagreements']} |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", help="write one row per player to this CSV file")
    args = ap.parse_args()

    from dota import storage, warehouse
    from dota.clean_contract import PARSED_COMPLETE, read_parquet, report_key

    match_ids = sorted({int(m.group(1)) for k in storage.list_keys(f"{PARSED_COMPLETE}/")
                        if (m := re.search(r"/match_id=(\d+)/", k))})
    replay_rows, clean = [], {}
    for match_id in match_ids:
        parts = [k for k in storage.list_keys(f"parsed/player_resource/match_id={match_id}/") if k.endswith(".parquet")]
        table = read_parquet(parts, storage.read).select(RESOURCE_COLUMNS)
        replay_rows += final_scoreboard(match_id, table.to_pylist())
        if storage.exists(report_key(match_id)):
            report = json.loads(storage.read(report_key(match_id)))
            clean[match_id] = {
                "result": "complete" if report.get("complete") else "rejected",
                "disagreements": report.get("findings", {}).get("scoreboard_disagreements", {}).get("rows")}

    conn = warehouse.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT match_id, player_slot, hero_id, kills, deaths, assists "
                        "FROM stg.steam_players WHERE match_id = ANY(%s)", (match_ids,))
            names = [d[0] for d in cur.description]
            steam_rows = [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    rows = compare(replay_rows, steam_rows)
    for row in rows:
        c = clean.get(row["match_id"], {})
        row["clean_result"], row["clean_scoreboard_disagreements"] = c.get("result"), c.get("disagreements")
    os.makedirs(os.path.dirname(os.path.abspath(args.csv)), exist_ok=True)
    with open(args.csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["match_id"])
        writer.writeheader()
        writer.writerows(rows)
    print(markdown(rows, clean))
    print(f"\n{len(match_ids)} parsed matches; wrote {len(rows)} player rows to {args.csv}")


if __name__ == "__main__":
    main()
