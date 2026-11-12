"""Static reference data from OpenDota's constants endpoints, parsed into seed rows."""


def parse_heroes(raw: dict) -> list[dict]:
    """Rows for the dim_heroes seed from /constants/heroes, sorted by hero_id."""
    rows = [
        {
            "hero_id": int(hero["id"]),
            "npc_name": hero["name"],
            "localized_name": hero["localized_name"],
            "primary_attr": hero["primary_attr"],
            "roles": "|".join(hero["roles"]),
        }
        for hero in raw.values()
    ]
    return sorted(rows, key=lambda r: r["hero_id"])


def parse_game_modes(raw: dict) -> list[dict]:
    """Rows for the dim_game_modes seed from /constants/game_mode, sorted by game_mode_id."""
    rows = [
        {
            "game_mode_id": int(mode["id"]),
            "name": mode["name"],
            "label": " ".join(w.capitalize() for w in mode["name"].removeprefix("game_mode_").split("_")),
        }
        for mode in raw.values()
    ]
    return sorted(rows, key=lambda r: r["game_mode_id"])
