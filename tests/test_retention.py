import pytest

from dota import clean_contract, retention, warehouse

MATCH = 9079737909
DEM = f"replays/{MATCH}.dem"


class Calls:
    """Records delete_key calls; `present` is the set of keys exists() reports."""

    def __init__(self, present: set[str] | None = None):
        self.present = present or set()
        self.exists_calls: list[str] = []
        self.deleted: list[str] = []

    def exists(self, key: str) -> bool:
        self.exists_calls.append(key)
        return key in self.present

    def delete_key(self, key: str) -> None:
        self.deleted.append(key)


def apply(mode: str, calls: Calls) -> str:
    return retention.apply_retention(MATCH, mode, exists=calls.exists, delete_key=calls.delete_key)


def test_keep_makes_no_storage_call():
    calls = Calls({DEM})
    assert apply("keep", calls) == "kept"
    assert calls.exists_calls == [] and calls.deleted == []


def test_delete_removes_dem_once():
    calls = Calls({DEM})
    assert apply("delete", calls) == "deleted"
    assert calls.deleted == [DEM]


def test_mode_is_stripped_and_lowercased():
    calls = Calls({DEM})
    assert apply(" Delete ", calls) == "deleted"
    assert calls.deleted == [DEM]


def test_missing_dem_is_absent_and_not_deleted():
    calls = Calls()
    assert apply("delete", calls) == "absent"
    assert calls.deleted == []


def test_archive_is_not_implemented():
    with pytest.raises(NotImplementedError):
        apply("archive", Calls({DEM}))


def test_unknown_mode_is_rejected_before_any_storage_call():
    calls = Calls({DEM})
    with pytest.raises(ValueError):
        apply("shred", calls)
    assert calls.exists_calls == [] and calls.deleted == []


def eligible(marker_present: bool) -> bool:
    present = {warehouse.loaded_marker_key(MATCH)} if marker_present else set()
    return retention.eligible(MATCH, read=lambda key: b"", list_keys=lambda prefix: [],
                              exists=lambda key: key in present)


def test_eligible_when_loaded_and_contract_passes(monkeypatch):
    monkeypatch.setattr(clean_contract, "check_parsed", lambda *a: {"matches": 1})
    assert eligible(True) is True


def test_not_eligible_without_loaded_marker(monkeypatch):
    def boom(*a):
        raise AssertionError("contract must not be consulted without the marker")
    monkeypatch.setattr(clean_contract, "check_parsed", boom)
    assert eligible(False) is False


def test_not_eligible_when_contract_fails(monkeypatch):
    def fail(*a):
        raise clean_contract.CleanContractError("bad")
    monkeypatch.setattr(clean_contract, "check_parsed", fail)
    assert eligible(True) is False
