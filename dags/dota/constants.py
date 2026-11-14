"""Static reference data parsed into dbt seed rows. Heroes, items and patches come from Valve's
dota2.com datafeed. Game modes come from OpenDota, because the datafeed has no list of them."""

from datetime import datetime, timezone

PRIMARY_ATTRS = {0: "str", 1: "agi", 2: "int", 3: "all"}
# the order of herodata's role_levels
ROLES = ("Carry", "Support", "Nuker", "Disabler", "Jungler", "Durable", "Escape", "Pusher", "Initiator")


def parse_heroes(herolist: dict, role_levels: dict[int, list[int]]) -> list[dict]:
    """Rows for the dim_heroes seed, sorted by hero_id. `herolist` is the datafeed's herolist
    response; `role_levels` maps hero_id to that hero's herodata role_levels."""
    rows = []
    for hero in herolist["result"]["data"]["heroes"]:
        levels = role_levels[hero["id"]]
        if len(levels) != len(ROLES):
            # Valve added or reordered a role; mapping by position would mislabel every hero
            raise ValueError(f"hero {hero['id']}: expected {len(ROLES)} role levels, got {len(levels)}")
        # strongest roles first; ties keep the datafeed's order
        roles = sorted((i for i, level in enumerate(levels) if level > 0), key=lambda i: -levels[i])
        rows.append({
            "hero_id": int(hero["id"]),
            "npc_name": hero["name"],
            "localized_name": hero["name_loc"],
            "primary_attr": PRIMARY_ATTRS[hero["primary_attr"]],
            "roles": "|".join(ROLES[i] for i in roles),
        })
    return sorted(rows, key=lambda r: r["hero_id"])


def parse_items(itemlist: dict) -> list[dict]:
    """Rows for the dim_items seed, sorted by item_id. `name` (item_blink) is what replays use."""
    rows = [
        {
            "item_id": int(item["id"]),
            "name": item["name"],
            "localized_name": item["name_loc"],
            "is_recipe": item["name"].startswith("item_recipe_"),
            "neutral_tier": item["neutral_item_tier"] if item["neutral_item_tier"] >= 0 else None,
        }
        for item in itemlist["result"]["data"]["itemabilities"]
    ]
    return sorted(rows, key=lambda r: r["item_id"])


def parse_patches(patchnoteslist: dict) -> list[dict]:
    """Rows for the dim_patches seed, oldest first, with the release time in UTC."""
    rows = [
        {
            "patch": p["patch_name"],
            "released_at": datetime.fromtimestamp(p["patch_timestamp"], timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        for p in patchnoteslist["patches"]
    ]
    return sorted(rows, key=lambda r: r["released_at"])


def parse_game_modes(raw: dict) -> list[dict]:
    """Rows for the dim_game_modes seed from OpenDota's /constants/game_mode, sorted by game_mode_id."""
    rows = [
        {
            "game_mode_id": int(mode["id"]),
            "name": mode["name"],
            "label": " ".join(w.capitalize() for w in mode["name"].removeprefix("game_mode_").split("_")),
        }
        for mode in raw.values()
    ]
    return sorted(rows, key=lambda r: r["game_mode_id"])
