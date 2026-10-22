"""Turn raw SPOT feed messages into Gundi observations."""
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

# Decommissioned devices report dates around year 1000; the legacy provider
# dropped anything from 2010 or earlier and so do we.
OBS_MIN_YEAR = 2010
# A message dated further ahead than this is a bad clock reading, not skew:
# junk. (Anything between now and this bound is not sendable yet either; the
# handler holds those back for a later run rather than dropping them.)
MAX_FUTURE_SKEW = timedelta(days=1)
SOURCE_TYPE = "tracking-device"

_OPTIONAL_ADDITIONAL = ("batteryState", "altitude")


def _number(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("not a number")
    return float(value)


def is_valid_message(message: dict, *, now: Optional[datetime] = None) -> bool:
    """A message is usable when it identifies its device, has an in-range
    position, and carries a plausible time (after 2010 and, when `now` is
    given, not more than a day ahead of it)."""
    if not message.get("messengerId"):
        return False
    try:
        lat = _number(message["latitude"])
        lon = _number(message["longitude"])
        unix_time = message["unixTime"]
        if isinstance(unix_time, bool) or not isinstance(unix_time, int):
            return False
        recorded_at = datetime.fromtimestamp(unix_time, tz=timezone.utc)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return False
    if now is not None and recorded_at > now + MAX_FUTURE_SKEW:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0 and recorded_at.year > OBS_MIN_YEAR


def message_to_observation(message: dict, *, subject_type: str, feed_id: str) -> dict:
    additional = {
        "messengerId": message.get("messengerId"),
        "messengerName": message.get("messengerName"),
        "messageType": message.get("messageType"),
        "modelId": message.get("modelId"),
    }
    for key in _OPTIONAL_ADDITIONAL:
        if message.get(key) is not None:
            additional[key] = message[key]
    additional["feedId"] = feed_id
    return {
        "source": message.get("messengerId"),
        "source_name": message.get("messengerName"),
        "type": SOURCE_TYPE,
        "subject_type": subject_type,
        "recorded_at": datetime.fromtimestamp(int(message["unixTime"]), tz=timezone.utc).isoformat(),
        "location": {"lat": float(message["latitude"]), "lon": float(message["longitude"])},
        "additional": additional,
    }


def messages_to_observations(
    messages: list, *, subject_type: str, feed_id: str, now: Optional[datetime] = None
) -> Tuple[list, int]:
    """Transform the valid messages; return (observations, number dropped as invalid)."""
    observations = []
    dropped = 0
    for message in messages:
        if not is_valid_message(message, now=now):
            dropped += 1
            continue
        observations.append(message_to_observation(message, subject_type=subject_type, feed_id=feed_id))
    return observations, dropped
