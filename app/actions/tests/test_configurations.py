import pytest
from pydantic import ValidationError

from app.actions.configurations import PullObservationsConfig, SpotFeed, dedupe_feeds


def test_minimal_config_has_spec_defaults():
    cfg = PullObservationsConfig(feeds=[{"feed_id": "abc"}])
    assert cfg.subject_type == "security_vehicle"
    assert cfg.initial_lookback_hours == 48
    assert cfg.run_on_schedule is True
    assert cfg.feeds[0].feed_password is None
    assert cfg.feeds[0].name is None


def test_feed_password_is_a_secret():
    cfg = PullObservationsConfig(feeds=[{"feed_id": "abc", "feed_password": "hunter2"}])
    assert "hunter2" not in repr(cfg)
    assert "hunter2" not in str(cfg.dict())
    assert cfg.feeds[0].feed_password.get_secret_value() == "hunter2"


@pytest.mark.parametrize("feeds", [[], [{"feed_id": str(i)} for i in range(51)]])
def test_feed_count_bounds(feeds):
    with pytest.raises(ValidationError):
        PullObservationsConfig(feeds=feeds)


def test_fifty_feeds_is_allowed():
    cfg = PullObservationsConfig(feeds=[{"feed_id": str(i)} for i in range(50)])
    assert len(cfg.feeds) == 50


@pytest.mark.parametrize("hours", [0, 169])
def test_lookback_bounds(hours):
    with pytest.raises(ValidationError):
        PullObservationsConfig(feeds=[{"feed_id": "a"}], initial_lookback_hours=hours)


def test_json_schema_exposes_portal_fields():
    schema = PullObservationsConfig.schema()
    assert set(schema["properties"]) == {"feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"}
    assert schema["properties"]["feeds"]["maxItems"] == 50
    assert schema["properties"]["feeds"]["minItems"] == 1
    assert schema["definitions"]["SpotFeed"]["properties"]["feed_password"]["format"] == "password"
    assert schema["definitions"]["SpotFeed"]["required"] == ["feed_id"]


def test_ui_schema_annotates_subject_type_and_password():
    ui = PullObservationsConfig.ui_schema()
    assert ui["ui:order"] == ["feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"]
    assert ui["subject_type"] == {
        "gundi:reference": {
            "action": "list_subject_types",
            "target": "destination",
            "params": {},
            "allow_free_text": True,
        }
    }
    assert "ui:widget" not in ui["subject_type"]
    assert ui["feeds"] == {"items": {"feed_password": {"ui:widget": "password"}}}


def test_dedupe_feeds_first_wins():
    feeds = [
        SpotFeed(feed_id="a", feed_password="1"),
        SpotFeed(feed_id="b"),
        SpotFeed(feed_id="a", feed_password="2"),
    ]
    out = dedupe_feeds(feeds)
    assert [f.feed_id for f in out] == ["a", "b"]
    assert out[0].feed_password.get_secret_value() == "1"
