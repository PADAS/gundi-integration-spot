import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, call, patch

import httpx
import pytest
from gundi_client_v2.errors import GundiAPIError
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from app.actions.client import SpotBadResponse, SpotFeedAuthError, SpotFeedNotFound, SpotRateLimited
from app.actions.configurations import PullObservationsConfig
from app.actions.handlers import (
    ACTION_ID,
    FEED_COOLDOWN_SECONDS,
    FEED_REQUEST_SPACING_SECONDS,
    RETRY_BACKOFF_SECONDS,
    _classify_feed_error,
    _compute_window,
    _get_pull_lock,
    action_pull_observations,
)
from app.services.errors import (
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
)

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)
T = int((NOW - timedelta(days=1)).timestamp())  # a unixTime inside SPOT's 7-day retention relative to NOW
INTEGRATION_ID = "aaaa1111-bbbb-cccc-dddd-eeee2222ffff"


def _msg(unix_time, messenger="0-1"):
    return {
        "messengerId": messenger, "messengerName": f"Tracker {messenger}", "unixTime": unix_time,
        "messageType": "TRACK", "latitude": 1.5, "longitude": 2.5, "modelId": "SPOT3",
    }


@pytest.fixture
def integration():
    return Integration.parse_obj({
        "id": INTEGRATION_ID,
        "name": "SPOT Test",
        "base_url": "",
        "enabled": True,
        "type": {
            "id": "50229e21-a9fe-4caa-862c-8592dfb2479b",
            "name": "SPOT",
            "value": "spot",
            "description": "SPOT integration",
            "actions": [{
                "id": "75b3040f-ab1f-42e7-b39f-8965c088b154",
                "type": "pull",
                "name": "Pull Observations",
                "value": "pull_observations",
            }],
        },
        "owner": {"id": "a91b400b-482a-4546-8fcb-ee42b01deeb6", "name": "Test Org", "description": ""},
        "configurations": [],
        "additional": {},
        "default_route": None,
        "status": "healthy",
        "status_details": "",
    })


@pytest.fixture
def state_manager():
    manager = AsyncMock()
    manager.get_state = AsyncMock(return_value={})
    manager.set_state = AsyncMock()
    manager.set_if_absent = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def fetch():
    return AsyncMock(return_value=[])


@pytest.fixture
def send():
    return AsyncMock(return_value={"status": "ok"})


@pytest.fixture
def log_activity():
    return AsyncMock()


@pytest.fixture
def sleep():
    return AsyncMock()


@pytest.fixture
def run(integration, state_manager, fetch, send, log_activity, sleep, mock_publish_event):
    """Run the handler with every collaborator patched; returns the awaitable result."""

    async def _run(config: PullObservationsConfig):
        with patch("app.actions.handlers.IntegrationStateManager", return_value=state_manager), \
             patch("app.actions.handlers.fetch_messages", fetch), \
             patch("app.actions.handlers.send_observations_to_gundi", send), \
             patch("app.actions.handlers.log_action_activity", log_activity), \
             patch("app.actions.handlers._sleep", sleep), \
             patch("app.actions.handlers._now", return_value=NOW), \
             patch("app.services.activity_logger.publish_event", mock_publish_event):
            return await action_pull_observations(integration=integration, action_config=config)

    return _run


def _config(*feed_ids, **overrides):
    feeds = [{"feed_id": f} if isinstance(f, str) else f for f in feed_ids]
    return PullObservationsConfig(feeds=feeds, **overrides)


def _state_by_feed(mapping):
    async def get_state(integration_id, action_id, source_id="no-source"):
        return mapping.get(source_id, {})
    return get_state


# --- window ------------------------------------------------------------------

def test_compute_window_first_run_uses_lookback():
    start, end = _compute_window(None, initial_lookback_hours=48, now=NOW)
    assert (start, end) == (NOW - timedelta(hours=48), NOW)


def test_compute_window_continues_one_second_after_state():
    start, _ = _compute_window(T, initial_lookback_hours=48, now=NOW)
    assert start == datetime.fromtimestamp(T + 1, tz=timezone.utc)


def test_compute_window_clamps_to_seven_days():
    start, _ = _compute_window(1_000_000_000, initial_lookback_hours=168, now=NOW)
    assert start == NOW - timedelta(days=7)
    start, _ = _compute_window(None, initial_lookback_hours=168, now=NOW)
    assert start == NOW - timedelta(days=7)


# --- happy paths --------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_run_sends_observations_and_writes_state(run, fetch, send, state_manager, spot_messages):
    fetch.return_value = spot_messages
    result = await run(_config("F1", subject_type="ranger"))

    send.assert_awaited_once()
    observations = send.await_args.kwargs["observations"]
    assert len(observations) == 51
    assert {o["subject_type"] for o in observations} == {"ranger"}
    assert send.await_args.kwargs["integration_id"] == INTEGRATION_ID
    state_manager.set_state.assert_awaited_once_with(
        integration_id=INTEGRATION_ID, action_id=ACTION_ID,
        state={"last_unix_time": max(m["unixTime"] for m in spot_messages)}, source_id="F1",
    )
    assert result == {"feeds": 1, "observations_sent": 51, "observations_dropped": 0, "feeds_failed": [], "feeds_skipped": []}


@pytest.mark.asyncio
async def test_second_run_requests_from_state_and_filters_client_side(run, fetch, send, state_manager):
    state_manager.get_state = AsyncMock(return_value={"last_unix_time": T})
    fetch.return_value = [_msg(T - 1), _msg(T), _msg(T + 1)]
    await run(_config("F1"))

    assert fetch.await_args.args[1] == datetime.fromtimestamp(T + 1, tz=timezone.utc)
    observations = send.await_args.kwargs["observations"]
    assert [o["recorded_at"] for o in observations] == [datetime.fromtimestamp(T + 1, tz=timezone.utc).isoformat()]
    state_manager.set_state.assert_awaited_once()
    assert state_manager.set_state.await_args.kwargs["state"] == {"last_unix_time": T + 1}


@pytest.mark.asyncio
async def test_messages_at_or_below_state_are_skipped_and_state_never_regresses(run, fetch, send, state_manager):
    state_manager.get_state = AsyncMock(return_value={"last_unix_time": T})
    fetch.return_value = [_msg(T - 10), _msg(T)]
    result = await run(_config("F1"))

    send.assert_not_awaited()
    state_manager.set_state.assert_not_awaited()
    assert result["observations_sent"] == 0


@pytest.mark.asyncio
async def test_state_is_tracked_per_feed(run, fetch, send, state_manager):
    state_manager.get_state = _state_by_feed({"A": {"last_unix_time": T}})
    fetch.return_value = [_msg(T + 5)]
    await run(_config("A", "B"))

    starts = {c.args[0]: c.args[1] for c in fetch.await_args_list}
    assert starts == {
        "A": datetime.fromtimestamp(T + 1, tz=timezone.utc),
        "B": NOW - timedelta(hours=48),
    }
    assert [c.kwargs["source_id"] for c in state_manager.set_state.await_args_list] == ["A", "B"]


@pytest.mark.asyncio
async def test_duplicate_feed_ids_fetch_once(run, fetch):
    result = await run(_config("A", "A"))
    assert fetch.await_count == 1
    assert result["feeds"] == 1


@pytest.mark.asyncio
async def test_sleeps_between_feeds_but_not_after_last(run, sleep):
    await run(_config("A", "B", "C"))
    assert sleep.await_args_list == [call(FEED_REQUEST_SPACING_SECONDS)] * 2


@pytest.mark.asyncio
async def test_invalid_messages_are_dropped_and_counted(run, fetch, send, spot_messages):
    fetch.return_value = [{**spot_messages[0], "latitude": 999}] + spot_messages[1:]
    result = await run(_config("F1"))
    assert result["observations_dropped"] == 1
    assert len(send.await_args.kwargs["observations"]) == 50


@pytest.mark.asyncio
async def test_nothing_to_send_skips_gundi(run, fetch, send, state_manager):
    fetch.return_value = []
    await run(_config("F1"))
    send.assert_not_awaited()
    state_manager.set_state.assert_not_awaited()


@pytest.mark.asyncio
async def test_password_is_passed_to_the_client(run, fetch):
    await run(_config({"feed_id": "A", "feed_password": "hunter2"}))
    assert fetch.await_args.kwargs["feed_password"] == "hunter2"


# --- feed-specific failures ---------------------------------------------------

@pytest.mark.asyncio
async def test_feed_auth_error_is_logged_and_next_feed_runs(run, fetch, send, log_activity):
    async def fetch_side_effect(feed_id, *args, **kwargs):
        if feed_id == "A":
            raise SpotFeedAuthError("Feed password is incorrect", code="E-0195")
        return [_msg(T + 1)]
    fetch.side_effect = fetch_side_effect

    result = await run(_config({"feed_id": "A", "feed_password": "hunter2"}, "B"))

    log_activity.assert_awaited_once()
    kwargs = log_activity.await_args.kwargs
    assert kwargs["level"] == LogLevel.ERROR
    assert kwargs["data"]["feed_id"] == "A"
    assert kwargs["data"]["error_type"] == "auth"
    assert "hunter2" not in repr(log_activity.await_args)
    assert result["feeds_failed"] == ["A"]
    assert result["observations_sent"] == 1
    send.assert_awaited_once()


@pytest.mark.parametrize("exc, expected_type", [
    (SpotFeedAuthError, "auth"),
    (SpotFeedNotFound, "configuration"),
    (SpotRateLimited, "rate_limit"),
    (SpotBadResponse, "bad_response"),
])
def test_classify_feed_error(exc, expected_type):
    assert _classify_feed_error(exc("x", code="E-0001")).error_type == expected_type


@pytest.mark.asyncio
async def test_all_feeds_failed_raises_configuration_error(run, fetch):
    fetch.side_effect = SpotFeedNotFound("Feed not found")
    with pytest.raises(IntegrationConfigurationError) as info:
        await run(_config("AAAA", "BBBB"))
    assert "AAAA" not in str(info.value) and "BBBB" not in str(info.value)


# --- systemic failures ----------------------------------------------------------

@pytest.mark.asyncio
async def test_transport_error_retries_then_aborts(run, fetch, sleep):
    fetch.side_effect = httpx.ConnectError("boom")
    with pytest.raises(IntegrationConnectionError):
        await run(_config("A", "B"))
    assert fetch.await_count == len(RETRY_BACKOFF_SECONDS)
    assert sleep.await_args_list == [call(RETRY_BACKOFF_SECONDS[0]), call(RETRY_BACKOFF_SECONDS[1])]
    assert {c.args[0] for c in fetch.await_args_list} == {"A"}


@pytest.mark.asyncio
async def test_5xx_retries_then_aborts(run, fetch):
    request = httpx.Request("GET", "https://api.findmespot.com/x")
    fetch.side_effect = httpx.HTTPStatusError("503", request=request, response=httpx.Response(503, request=request))
    with pytest.raises(IntegrationBadResponseError):
        await run(_config("A"))
    assert fetch.await_count == len(RETRY_BACKOFF_SECONDS)


@pytest.mark.asyncio
async def test_transport_error_recovers_on_second_attempt(run, fetch, send, log_activity):
    fetch.side_effect = [httpx.ReadTimeout("slow"), [_msg(T + 1)]]
    result = await run(_config("A"))
    assert result["observations_sent"] == 1
    send.assert_awaited_once()
    log_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_4xx_is_not_retried(run, fetch, sleep):
    fetch.side_effect = SpotFeedAuthError("Feed password required")
    with pytest.raises(IntegrationConfigurationError):
        await run(_config("A"))
    assert fetch.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_gundi_send_failure_aborts_and_keeps_state(run, fetch, send, state_manager):
    fetch.return_value = [_msg(T + 1)]
    send.side_effect = [{"status": "ok"}, GundiAPIError(status_code=503, detail="Gundi is down")]
    with pytest.raises(GundiAPIError):
        await run(_config("A", "B"))
    assert [c.kwargs["source_id"] for c in state_manager.set_state.await_args_list] == ["A"]


@pytest.mark.asyncio
async def test_overlapping_run_is_skipped(run, fetch):
    async with _get_pull_lock(INTEGRATION_ID):
        result = await run(_config("A"))
    assert result == {"skipped": True}
    fetch.assert_not_awaited()


# --- review fixes ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_password_never_reaches_runtime_logs_or_exception_text(
    integration, state_manager, send, log_activity, sleep, mock_publish_event, caplog, monkeypatch
):
    """Critical review finding: httpx logs every request URL at INFO (with feedPassword in
    the query), and a 5xx retry log line quoted the URL too. Run the real client path."""
    seen = []

    def transport(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(503, text="down", request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        "app.actions.client.httpx.AsyncClient",
        lambda **kwargs: real_async_client(transport=httpx.MockTransport(transport), **kwargs),
    )
    caplog.set_level(logging.INFO)
    with patch("app.actions.handlers.IntegrationStateManager", return_value=state_manager), \
         patch("app.actions.handlers.send_observations_to_gundi", send), \
         patch("app.actions.handlers.log_action_activity", log_activity), \
         patch("app.actions.handlers._sleep", sleep), \
         patch("app.actions.handlers._now", return_value=NOW), \
         patch("app.services.activity_logger.publish_event", mock_publish_event):
        with pytest.raises(IntegrationBadResponseError) as info:
            await action_pull_observations(
                integration=integration, action_config=_config({"feed_id": "A", "feed_password": "hunter2"})
            )

    assert seen and all("feedPassword=hunter2" in url for url in seen)  # the real client sent it
    assert "hunter2" not in caplog.text
    assert "hunter2" not in str(info.value)
    httpx_lines = [r.getMessage() for r in caplog.records if r.name == "httpx"]
    assert httpx_lines and all("feedPassword=**********" in line for line in httpx_lines)
    retry_lines = [r.getMessage() for r in caplog.records if "attempt 1/" in r.getMessage()]
    assert retry_lines and "hunter2" not in retry_lines[0]


@pytest.mark.asyncio
async def test_feed_in_cooldown_is_skipped(run, fetch, state_manager):
    """A PubSub redelivery (or a second instance) must not re-poll a feed inside SPOT's 2.5-minute window."""
    state_manager.set_if_absent = AsyncMock(side_effect=lambda *a, **k: k["source_id"] != "A:cooldown")
    fetch.return_value = [_msg(T + 1)]
    result = await run(_config("A", "B"))

    assert [c.args[0] for c in fetch.await_args_list] == ["B"]
    assert result["feeds_skipped"] == ["A"]
    assert result["observations_sent"] == 1
    cooldown_calls = state_manager.set_if_absent.await_args_list
    assert [c.kwargs["source_id"] for c in cooldown_calls] == ["A:cooldown", "B:cooldown"]
    assert {c.kwargs["ttl_seconds"] for c in cooldown_calls} == {FEED_COOLDOWN_SECONDS}


@pytest.mark.asyncio
async def test_all_feeds_in_cooldown_is_not_a_failure(run, fetch, state_manager):
    state_manager.set_if_absent = AsyncMock(return_value=False)
    result = await run(_config("A"))
    fetch.assert_not_awaited()
    assert result["feeds_skipped"] == ["A"] and result["feeds_failed"] == []


@pytest.mark.asyncio
async def test_future_dated_message_is_dropped_and_does_not_advance_state(run, fetch, send, state_manager):
    """One bad clock reading must not brick a feed: a message 2 days ahead of now is junk."""
    future = int((NOW + timedelta(days=2)).timestamp())
    fetch.return_value = [_msg(T + 1), _msg(future)]
    result = await run(_config("A"))

    assert len(send.await_args.kwargs["observations"]) == 1
    assert state_manager.set_state.await_args.kwargs["state"] == {"last_unix_time": T + 1}
    assert result["observations_dropped"] == 1


@pytest.mark.asyncio
async def test_non_integer_unix_time_is_dropped_on_first_run(run, fetch, send, state_manager):
    fetch.return_value = [_msg("not-a-time"), _msg(T + 1)]
    result = await run(_config("A"))
    assert result == {"feeds": 1, "observations_sent": 1, "observations_dropped": 1, "feeds_failed": [], "feeds_skipped": []}
    assert state_manager.set_state.await_args.kwargs["state"] == {"last_unix_time": T + 1}
