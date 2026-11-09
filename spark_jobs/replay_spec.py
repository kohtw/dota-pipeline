"""
What to extract from each replay: every raw table and its columns.

The Java extractor (parser/src/main/java/ReplayExtractor.java) knows only *how* to read a replay;
this file says *what* to read. Add a column or table here, no Java change needed.

Values are raw replay values. Column names are the replay's own property names.
Types: "int", "long", "float", "bool", "string".
Class patterns: exact class name, or a prefix ending in "*".
"""

SAMPLE_TICKS = 30  # snapshot tables: one sample per second (30 ticks)

BODY = [  # world position = cell * 128 + vec - 16384 (computed in clean, not here)
    ("CBodyComponent.m_cellX", "int"),
    ("CBodyComponent.m_cellY", "int"),
    ("CBodyComponent.m_vecX", "float"),
    ("CBodyComponent.m_vecY", "float"),
]

ITEM_SLOTS = [(f"m_hItems.{i:04d}", "int") for i in range(17)]  # item entity handles; 16777215 = empty


def file_info(name):
    return {"name": name, "kind": "file_info"}


def file_players(name):
    return {"name": name, "kind": "file_players"}


def combatlog(name):
    return {"name": name, "kind": "combatlog"}


def string_table(name, table):
    return {"name": name, "kind": "string_table", "string_table": table}


def snapshot(name, classes, props, index_count=0):
    """Every SAMPLE_TICKS: one row per matching entity (per array index if index_count, '{i}' in props)."""
    return {"name": name, "kind": "snapshot", "classes": classes, "props": props, "index_count": index_count}


def lifecycle(name, classes, props):
    """One row when a matching entity is created, one when it is deleted."""
    return {"name": name, "kind": "lifecycle", "classes": classes, "props": props}


TABLES = [
    file_info("match"),
    file_players("players"),
    combatlog("combatlog"),
    string_table("names", "CombatLogNames"),
    string_table("entity_names", "EntityNames"),
    snapshot("gamerules", ["CDOTAGamerulesProxy"], [
        ("m_pGameRules.m_nGameState", "int"),
        ("m_pGameRules.m_flPreGameStartTime", "float"),
        ("m_pGameRules.m_flGameStartTime", "float"),
        ("m_pGameRules.m_flGameEndTime", "float"),
        ("m_pGameRules.m_bGamePaused", "bool"),
    ]),
    snapshot("heroes", ["CDOTA_Unit_Hero_*"], [  # illusions included
        ("m_iPlayerID", "int"),
        ("m_iTeamNum", "int"),
        ("m_hReplicatingOtherHeroModel", "int"),
        *BODY,
        *ITEM_SLOTS,
    ]),
    snapshot("player_resource", ["CDOTA_PlayerResource"], [
        ("m_vecPlayerData.{i}.m_iPlayerTeam", "int"),
        ("m_vecPlayerTeamData.{i}.m_iTeamSlot", "int"),
        ("m_vecPlayerTeamData.{i}.m_nSelectedHeroID", "int"),
        ("m_vecPlayerTeamData.{i}.m_iLevel", "int"),
        ("m_vecPlayerTeamData.{i}.m_iKills", "int"),
        ("m_vecPlayerTeamData.{i}.m_iDeaths", "int"),
        ("m_vecPlayerTeamData.{i}.m_iAssists", "int"),
    ], index_count=64),
    snapshot("team_data", ["CDOTA_DataRadiant", "CDOTA_DataDire"], [
        ("m_vecDataTeam.{i}.m_iNetWorth", "int"),
        ("m_vecDataTeam.{i}.m_iReliableGold", "int"),
        ("m_vecDataTeam.{i}.m_iUnreliableGold", "int"),
        ("m_vecDataTeam.{i}.m_iTotalEarnedGold", "int"),
        ("m_vecDataTeam.{i}.m_iTotalEarnedXP", "int"),
        ("m_vecDataTeam.{i}.m_iLastHitCount", "int"),
        ("m_vecDataTeam.{i}.m_iDenyCount", "int"),
        ("m_vecDataTeam.{i}.m_flHeroDamage", "float"),
        ("m_vecDataTeam.{i}.m_flTowerDamage", "float"),
        ("m_vecDataTeam.{i}.m_fHealing", "float"),
        ("m_vecDataTeam.{i}.m_fStuns", "float"),
        ("m_vecDataTeam.{i}.m_iObserverWardsPlaced", "int"),
        ("m_vecDataTeam.{i}.m_iSentryWardsPlaced", "int"),
        ("m_vecDataTeam.{i}.m_iCampsStacked", "int"),
        ("m_vecDataTeam.{i}.m_iRunePickups", "int"),
        ("m_vecDataTeam.{i}.m_iTowerKills", "int"),
        ("m_vecDataTeam.{i}.m_iRoshanKills", "int"),
    ], index_count=64),
    lifecycle("wards", ["CDOTA_NPC_Observer_Ward*"], [
        ("m_nPlayerOwnerID", "int"),
        ("m_iTeamNum", "int"),
        *BODY,
    ]),
    lifecycle("item_entities", ["CDOTA_Item*"], [  # join heroes.m_hItems.* on handle -> entity_names.idx
        ("m_pEntity.m_nameStringTableIndex", "int"),
    ]),
]

SPEC = {"sample_ticks": SAMPLE_TICKS, "tables": TABLES}
