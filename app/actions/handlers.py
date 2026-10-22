"""Pull action: fetch new positions from a customer's SPOT feeds and send them to Gundi.

Design: docs/superpowers/specs/2026-10-22-gundi-integration-spot-design.md
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from app.actions.client import (
    SpotBadResponse,
    SpotFeedAuthError,
    SpotFeedError,
    SpotFeedNotFound,
    SpotRateLimited,
    fetch_messages,
)
from app.actions.configurations import PullObservationsConfig, SpotFeed, dedupe_feeds
from app.actions.core import action_title
from app.actions.transform import messages_to_observations
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.errors import (
    IntegrationAuthError,
    IntegrationBadResponseError,
    IntegrationConfigurationError,
    IntegrationConnectionError,
    IntegrationError,
    IntegrationRateLimitError,
)
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager

logger = logging.getLogger(__name__)

ACTION_ID = "pull_observations"
FEED_REQUEST_SPACING_SECONDS = 2   # SPOT asks for >= 2 s between calls to different feeds
SPOT_RETENTION_DAYS = 7            # SPOT only ever serves the last 7 days
RETRY_BACKOFF_SECONDS = (1, 2, 4)  # one attempt per entry; sleep between attempts
REQUEST_TIMEOUT_SECONDS = 10.0

# Module-level aliases so tests can patch time and sleeping without touching asyncio.
_sleep = asyncio.sleep


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


_pull_locks: Dict[str, asyncio.Lock] = {}


def _get_pull_lock(integration_id: str) -> asyncio.Lock:
    if integration_id not in _pull_locks:
        _pull_locks[integration_id] = asyncio.Lock()
    return _pull_locks[integration_id]


def _compute_window(
    last_unix_time: Optional[int], *, initial_lookback_hours: int, now: datetime
) -> Tuple[datetime, datetime]:
    """[start, end] to ask SPOT for: one second past the stored mark, or the
    initial lookback on a feed's first run; never older than SPOT's retention."""
    if last_unix_time is not None:
        start = datetime.fromtimestamp(int(last_unix_time) + 1, tz=timezone.utc)
    else:
        start = now - timedelta(hours=initial_lookback_hours)
    start = max(start, now - timedelta(days=SPOT_RETENTION_DAYS))
    return start, now


def _classify_feed_error(exc: Exception) -> IntegrationError:
    """Map a client exception to the template's classified errors (portal titles)."""
    message = str(exc)
    if isinstance(exc, SpotFeedAuthError):
        return IntegrationAuthError(message)
    if isinstance(exc, SpotFeedNotFound):
        return IntegrationConfigurationError(message)
    if isinstance(exc, SpotRateLimited):
        return IntegrationRateLimitError(message, status_code=429)
    if isinstance(exc, SpotBadResponse):
        return IntegrationBadResponseError(message)
    if isinstance(exc, httpx.HTTPStatusError):
        return IntegrationBadResponseError(message, status_code=exc.response.status_code)
    if isinstance(exc, httpx.HTTPError):
        return IntegrationConnectionError(message)
    return IntegrationError(message)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, httpx.TransportError)


async def _fetch_with_retries(
    feed_id: str, start: datetime, end: datetime, *, feed_password: Optional[str]
) -> list:
    """Retry transport errors and 5xx with a short backoff; SPOT's own errors (4xx, envelopes) are final."""
    attempts = len(RETRY_BACKOFF_SECONDS)
    for attempt in range(attempts):
        try:
            return await fetch_messages(
                feed_id, start, end, feed_password=feed_password, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except Exception as e:
            if not _is_retryable(e) or attempt == attempts - 1:
                raise
            logger.warning(
                "SPOT request for feed %s failed (attempt %d/%d): %s", feed_id, attempt + 1, attempts, e
            )
            await _sleep(RETRY_BACKOFF_SECONDS[attempt])
    raise AssertionError("unreachable")


async def _pull_feed(
    integration_id: str,
    feed: SpotFeed,
    *,
    subject_type: str,
    initial_lookback_hours: int,
    state_manager: IntegrationStateManager,
    now: datetime,
) -> Tuple[int, int]:
    """Fetch, filter, send and advance one feed. Returns (observations sent, messages dropped)."""
    state = await state_manager.get_state(
        integration_id=integration_id, action_id=ACTION_ID, source_id=feed.feed_id
    )
    last_unix_time = state.get("last_unix_time") if state else None
    last_unix_time = int(last_unix_time) if last_unix_time is not None else None
    start, end = _compute_window(last_unix_time, initial_lookback_hours=initial_lookback_hours, now=now)

    password = feed.feed_password.get_secret_value() if feed.feed_password else None
    messages = await _fetch_with_retries(feed.feed_id, start, end, feed_password=password)
    if last_unix_time is not None:
        # SPOT may ignore the dates, re-send, or deliver out of order: filter here regardless.
        messages = [m for m in messages if isinstance(m.get("unixTime"), int) and m["unixTime"] > last_unix_time]
    logger.info("SPOT feed %s: %d messages since %s", feed.feed_id, len(messages), start.isoformat())

    observations, dropped = messages_to_observations(messages, subject_type=subject_type, feed_id=feed.feed_id)
    if dropped:
        logger.info("SPOT feed %s: dropped %d messages with invalid position or date", feed.feed_id, dropped)
    if not observations:
        return 0, dropped

    await send_observations_to_gundi(observations=observations, integration_id=integration_id)

    newest = max(m["unixTime"] for m in messages if m.get("unixTime") is not None)
    if last_unix_time is None or newest > last_unix_time:
        await state_manager.set_state(
            integration_id=integration_id,
            action_id=ACTION_ID,
            state={"last_unix_time": newest},
            source_id=feed.feed_id,
        )
    return len(observations), dropped


@action_title("Pull Observations")
@crontab_schedule("*/5 * * * *")
@activity_logger()
async def action_pull_observations(integration: Integration, action_config: PullObservationsConfig) -> dict:
    integration_id = str(integration.id)
    lock = _get_pull_lock(integration_id)
    if lock.locked():
        logger.warning("pull_observations already running for %s, skipping", integration_id)
        return {"skipped": True}
    async with lock:
        return await _pull_all_feeds(integration_id, action_config)


async def _pull_all_feeds(integration_id: str, action_config: PullObservationsConfig) -> dict:
    feeds = dedupe_feeds(action_config.feeds)
    now = _now()
    state_manager = IntegrationStateManager()
    observations_sent = 0
    observations_dropped = 0
    feeds_failed: List[str] = []

    for index, feed in enumerate(feeds):
        if index:
            await _sleep(FEED_REQUEST_SPACING_SECONDS)
        try:
            sent, dropped = await _pull_feed(
                integration_id,
                feed,
                subject_type=action_config.subject_type,
                initial_lookback_hours=action_config.initial_lookback_hours,
                state_manager=state_manager,
                now=now,
            )
        except SpotFeedError as e:
            # Feed-specific: log it against this feed and carry on with the others.
            classified = _classify_feed_error(e)
            logger.error("SPOT feed %s failed: %s", feed.feed_id, classified.message)
            await log_action_activity(
                integration_id=integration_id,
                action_id=ACTION_ID,
                title=f"SPOT feed {feed.feed_id}: {classified.default_title}",
                level=LogLevel.ERROR,
                data={
                    "feed_id": feed.feed_id,
                    "feed_name": feed.name,
                    "error_type": classified.error_type,
                    "message": classified.message,
                },
            )
            feeds_failed.append(feed.feed_id)
            continue
        except httpx.HTTPError as e:
            # Systemic (SPOT or the network is down after retries): abort the run.
            raise _classify_feed_error(e) from e
        observations_sent += sent
        observations_dropped += dropped

    if feeds_failed and len(feeds_failed) == len(feeds):
        # Final (not redelivered by PubSub): the schedule is the only thing that re-polls SPOT.
        raise IntegrationConfigurationError(
            f"All {len(feeds)} SPOT feeds failed; see the feed errors in this run's activity log"
        )
    return {
        "feeds": len(feeds),
        "observations_sent": observations_sent,
        "observations_dropped": observations_dropped,
        "feeds_failed": feeds_failed,
    }
