from datetime import datetime, timedelta, timezone

import pytest

from app.actions.transform import is_valid_message, message_to_observation, messages_to_observations


def test_fixture_maps_every_message(spot_messages):
    observations, dropped = messages_to_observations(spot_messages, subject_type="ranger", feed_id="F1")
    assert len(observations) == 51
    assert dropped == 0


def test_observation_fields(spot_messages):
    obs = message_to_observation(spot_messages[0], subject_type="ranger", feed_id="F1")
    assert obs == {
        "source": "0-2825446",
        "source_name": "DAS Spot",
        "type": "tracking-device",
        "subject_type": "ranger",
        "recorded_at": "2018-11-19T00:27:12+00:00",
        "location": {"lat": 47.70104, "lon": -122.29115},
        "additional": {
            "messengerId": "0-2825446",
            "messengerName": "DAS Spot",
            "messageType": "UNLIMITED-TRACK",
            "modelId": "SPOT3",
            "batteryState": "GOOD",
            "altitude": 68,
            "feedId": "F1",
        },
    }


def test_optional_additional_fields_are_omitted(spot_messages):
    msg = {k: v for k, v in spot_messages[0].items() if k not in ("batteryState", "altitude")}
    additional = message_to_observation(msg, subject_type="x", feed_id="F")["additional"]
    assert "batteryState" not in additional
    assert "altitude" not in additional


@pytest.mark.parametrize("patch", [
    {"unixTime": -30610224000},  # year 1000: a decommissioned device
    {"unixTime": 1262304000},    # 2010-01-01: the year must be > 2010
    {"latitude": 91.0},
    {"latitude": -90.5},
    {"longitude": 180.1},
    {"longitude": -181},
])
def test_invalid_messages_are_dropped(spot_messages, patch):
    assert is_valid_message({**spot_messages[0], **patch}) is False


def test_fixture_message_is_valid(spot_messages):
    assert is_valid_message(spot_messages[0]) is True


@pytest.mark.parametrize("make", [
    lambda m: {**m, "latitude": None},
    lambda m: {**m, "longitude": None},
    lambda m: {**m, "latitude": "n/a"},
    lambda m: {k: v for k, v in m.items() if k != "latitude"},
    lambda m: {k: v for k, v in m.items() if k != "unixTime"},
])
def test_message_without_position_is_invalid(spot_messages, make):
    assert is_valid_message(make(spot_messages[0])) is False


def test_messages_to_observations_counts_dropped(spot_messages):
    bad = {**spot_messages[0], "latitude": 999}
    observations, dropped = messages_to_observations([bad, spot_messages[1]], subject_type="x", feed_id="F")
    assert len(observations) == 1
    assert dropped == 1


NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def test_message_far_in_the_future_is_invalid(spot_messages):
    ahead = int((NOW + timedelta(days=2)).timestamp())
    assert is_valid_message({**spot_messages[0], "unixTime": ahead}, now=NOW) is False
    soon = int((NOW + timedelta(hours=1)).timestamp())
    assert is_valid_message({**spot_messages[0], "unixTime": soon}, now=NOW) is True


def test_messages_to_observations_drops_future_dated(spot_messages):
    ahead = int((NOW + timedelta(days=2)).timestamp())
    observations, dropped = messages_to_observations(
        [{**spot_messages[0], "unixTime": ahead}, spot_messages[1]], subject_type="x", feed_id="F", now=NOW
    )
    assert len(observations) == 1 and dropped == 1


@pytest.mark.parametrize("patch", [{"messengerId": None}, {"messengerId": ""}])
def test_message_without_messenger_id_is_invalid(spot_messages, patch):
    assert is_valid_message({**spot_messages[0], **patch}) is False


def test_message_missing_messenger_id_key_is_invalid(spot_messages):
    assert is_valid_message({k: v for k, v in spot_messages[0].items() if k != "messengerId"}) is False
