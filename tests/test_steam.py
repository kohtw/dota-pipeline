from dota import steam


def pages(*page_list):
    """A fake steam.call returning the given result dicts in order."""
    it = iter(page_list)
    calls = []

    def fake(method, **params):
        calls.append(params)
        return next(it)

    return fake, calls


def match(seq, mode=22, lobby=7):
    return {"match_id": seq * 10, "match_seq_num": seq, "game_mode": mode, "lobby_type": lobby}


def test_save_page_called_once_per_page_with_start_seq_and_raw_result(monkeypatch):
    p1 = {"status": 1, "matches": [match(100), match(101, mode=23)]}
    p2 = {"status": 1, "matches": [match(102)]}
    p3 = {"status": 1, "matches": []}
    fake, _ = pages(p1, p2, p3)
    monkeypatch.setattr(steam, "call", fake)
    saved = []
    ids = steam.ranked_all_pick_from(100, 50, save_page=lambda s, r: saved.append((s, r)))
    assert ids == [1000, 1020]
    assert saved == [(100, p1), (102, p2), (103, p3)]


def test_without_save_page_result_is_unchanged(monkeypatch):
    fake, _ = pages({"matches": [match(1), match(2), match(3)]})
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_from(1, 2) == [10, 20]


def test_max_pages_is_overridable(monkeypatch):
    fake, calls = pages(*[{"matches": [match(i, mode=23)]} for i in range(1, 10)])
    monkeypatch.setattr(steam, "call", fake)
    assert steam.ranked_all_pick_from(1, 50, max_pages=3) == []
    assert len(calls) == 3


def test_default_max_pages_is_the_module_constant(monkeypatch):
    fake, calls = pages(*[{"matches": [match(i, mode=23)]} for i in range(1, 40)])
    monkeypatch.setattr(steam, "call", fake)
    steam.ranked_all_pick_from(1, 50)
    assert len(calls) == steam.MAX_PAGES


def test_http_error_message_does_not_contain_the_api_key(monkeypatch):
    import pytest

    class Resp:
        ok, status_code, reason = False, 403, "Forbidden"

    monkeypatch.setenv("STEAM_API_KEY", "SECRET123")
    monkeypatch.setattr(steam.requests, "get", lambda *a, **k: Resp())
    with pytest.raises(RuntimeError) as err:
        steam.call("GetMatchHistory", matches_requested=1)
    assert "SECRET123" not in str(err.value)
    assert "403" in str(err.value)
