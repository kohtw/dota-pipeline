from dota.errors import is_transient_status, redact


def test_redact_hides_key_followed_by_another_param():
    assert redact("https://x/y?key=A&b=1") == "https://x/y?key=<redacted>&b=1"


def test_redact_stops_at_a_quote():
    assert redact('url "https://x/y?key=A"') == 'url "https://x/y?key=<redacted>"'


def test_redact_hides_key_at_end_of_string():
    assert redact("https://x/y?b=1&key=A") == "https://x/y?b=1&key=<redacted>"


def test_redact_stops_at_a_space():
    assert redact("url: /y?key=A (Caused by timeout)") == "url: /y?key=<redacted> (Caused by timeout)"


def test_redact_leaves_text_without_a_key_alone():
    assert redact("https://x/y?b=1") == "https://x/y?b=1"


def test_is_transient_status():
    assert [is_transient_status(s) for s in (200, 404, 429, 500, 503)] == [False, False, True, True, True]
