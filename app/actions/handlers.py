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
    SpotFeedRejected,
    SpotRateLimited,
    fetch_messages,
)
from app.actions.configurations import PullObservationsConfig, SpotFeed, dedupe_feeds
from app.actions.core import action_title
from app.actions.transform import MAX_FUTURE_SKEW, messages_to_observations
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
from app.services.redaction import redact_text
from app.services.state import IntegrationStateManager

logger = logging.getLogger(__name__)

ACTION_ID = "pull_observations"
FEED_REQUEST_SPACING_SECONDS = 2   # SPOT asks for >= 2 s between calls to different feeds
FEED_COOLDOWN_SECONDS = 150        # SPOT asks for >= 2.5 min between calls to the same feed
SPOT_RETENTION_DAYS = 7            # SPOT only ever serves the last 7 days
RETRY_BACKOFF_SECONDS = (1, 2, 4)  # one attempt per entry; sleep between attempts
REQUEST_TIMEOUT_SECONDS = 10.0

# Module-level aliases so tests can patch time and sleeping without touching asyncio.
_sleep = asyncio.sleep


class _RedactingFilter(logging.Filter):
    """httpx logs every request URL at INFO, and SPOT's only credential travels
    in that URL (feedPassword=...). Mask it before the record reaches any handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_text(record.getMessage())
        record.args = ()
        return True


def _install_httpx_redaction() -> None:
    """Idempotent: a reload or re-import must not stack filters."""
    httpx_logger = logging.getLogger("httpx")
    if not any(type(f).__name__ == _RedactingFilter.__name__ for f in httpx_logger.filters):
        httpx_logger.addFilter(_RedactingFilter())


_install_httpx_redaction()


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
    """Map a client exception to the template's classified errors (portal titles).
    The message is redacted: an httpx error quotes the request URL, password included."""
    message = redact_text(str(exc).splitlines()[0] if str(exc) else type(exc).__name__)
    if isinstance(exc, SpotFeedAuthError):
        return IntegrationAuthError(message)
    if isinstance(exc, SpotFeedNotFound):
        return IntegrationConfigurationError(message)
    if isinstance(exc, SpotRateLimited):
        return IntegrationRateLimitError(message, status_code=429)
    if isinstance(exc, (SpotBadResponse, SpotFeedRejected)):
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


class SpotUnreachable(Exception):
    """SPOT gave no usable answer after retries (transport failure or 5xx).
    Carries the classified IntegrationError the run should fail with."""

    def __init__(self, classified: IntegrationError):
        super().__init__(classified.message)
        self.classified = classified


async def _fetch_with_retries(
    feed_id: str, start: datetime, end: datetime, *, feed_password: Optional[str], client: httpx.AsyncClient
) -> list:
    """Retry transport errors and 5xx with a short backoff; SPOT's own errors (4xx, envelopes) are final.
    Exhausted or non-retryable httpx errors become SpotUnreachable so that an httpx error raised
    later by the Gundi send is never mistaken for a SPOT outage."""
    attempts = len(RETRY_BACKOFF_SECONDS)
    for attempt in range(attempts):
        try:
            return await fetch_messages(feed_id, start, end, feed_password=feed_password, client=client)
        except httpx.HTTPError as e:
            if not _is_retryable(e) or attempt == attempts - 1:
                raise SpotUnreachable(_classify_feed_error(e)) from e
            logger.warning(
                "SPOT request for feed %s failed (attempt %d/%d): %s",
                feed_id, attempt + 1, attempts, redact_text(str(e).splitlines()[0] if str(e) else type(e).__name__),
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
    client: httpx.AsyncClient,
) -> Tuple[int, int]:
    """Fetch, filter, send and advance one feed. Returns (observations sent, messages dropped)."""
    state = await state_manager.get_state(
        integration_id=integration_id, action_id=ACTION_ID, source_id=feed.feed_id
    )
    last_unix_time = state.get("last_unix_time") if state else None
    last_unix_time = int(last_unix_time) if last_unix_time is not None else None
    start, end = _compute_window(last_unix_time, initial_lookback_hours=initial_lookback_hours, now=now)

    password = feed.feed_password.get_secret_value() if feed.feed_password else None
    try:
        messages = await _fetch_with_retries(feed.feed_id, start, end, feed_password=password, client=client)
    except SpotUnreachable:
        # SPOT was never polled, so this attempt must not count against the cooldown.
        await state_manager.delete_state(
            integration_id=integration_id, action_id=ACTION_ID, source_id=_cooldown_key(feed.feed_id)
        )
        raise
    now_ts = int(now.timestamp())
    horizon_ts = now_ts + int(MAX_FUTURE_SKEW.total_seconds())
    dropped = 0
    current = []
    for m in messages:
        unix_time = m.get("unixTime")
        if not isinstance(unix_time, int) or isinstance(unix_time, bool):
            dropped += 1  # not a time at all: junk, counted like any other invalid message
            continue
        if last_unix_time is not None and unix_time <= last_unix_time:
            continue  # SPOT may ignore the dates, re-send, or deliver out of order: already handled
        if unix_time > horizon_ts:
            dropped += 1  # further ahead than any clock skew: junk
            continue
        if unix_time > now_ts:
            continue  # not sendable yet: a later run picks it up once now has passed it
        current.append(m)
    logger.info("SPOT feed %s: %d new messages since %s", feed.feed_id, len(current), start.isoformat())

    observations, invalid = messages_to_observations(
        current, subject_type=subject_type, feed_id=feed.feed_id, now=now
    )
    dropped += invalid
    if dropped:
        logger.info("SPOT feed %s: dropped %d messages with invalid position or date", feed.feed_id, dropped)
    if observations:
        await send_observations_to_gundi(observations=observations, integration_id=integration_id)

    # Advance to the newest time among the messages handled this run, valid
    # position or not, so a device sending only junk does not make every run
    # re-fetch the same window. A junk year-1000 date never wins (it is below
    # the mark), nothing after now was included, so the next window can never
    # start after it ends. Integer comparison only: nothing here can raise.
    plausible = [m["unixTime"] for m in current]
    if plausible:
        newest = max(plausible)
        if last_unix_time is None or newest > last_unix_time:
            await state_manager.set_state(
                integration_id=integration_id,
                action_id=ACTION_ID,
                state={"last_unix_time": newest},
                source_id=feed.feed_id,
            )
    return len(observations), dropped


def _cooldown_key(feed_id: str) -> str:
    return f"{feed_id}:cooldown"


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
    feeds_skipped: List[str] = []
    failures: List[IntegrationError] = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        polled_any = False
        for feed in feeds:
            # One poll per feed per cooldown window, across PubSub redeliveries and
            # instances: SPOT blocks IPs that hit the same feed too often.
            first_in_window = await state_manager.set_if_absent(
                integration_id, ACTION_ID, ttl_seconds=FEED_COOLDOWN_SECONDS, source_id=_cooldown_key(feed.feed_id)
            )
            if not first_in_window:
                logger.info("SPOT feed %s polled less than %ss ago, skipping", feed.feed_id, FEED_COOLDOWN_SECONDS)
                feeds_skipped.append(feed.feed_id)
                continue
            if polled_any:
                await _sleep(FEED_REQUEST_SPACING_SECONDS)
            polled_any = True
            try:
                sent, dropped = await _pull_feed(
                    integration_id,
                    feed,
                    subject_type=action_config.subject_type,
                    initial_lookback_hours=action_config.initial_lookback_hours,
                    state_manager=state_manager,
                    now=now,
                    client=client,
                )
            except SpotRateLimited as e:
                # A 429 is IP-level, not per feed: stop hitting SPOT. Retryable, so
                # PubSub redelivers after backoff; the cooldown keys throttle it.
                raise _classify_feed_error(e) from e
            except SpotUnreachable as e:
                # SPOT or the network is down after retries: abort the run (retryable).
                raise e.classified from e
            except SpotBadResponse as e:
                # Not a feed response at all (maintenance page, redirect): SPOT-wide, abort (retryable).
                raise _classify_feed_error(e) from e
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
                failures.append(classified)
                continue
            observations_sent += sent
            observations_dropped += dropped

    if feeds_failed and len(feeds_failed) == len(feeds):
        # Fail the run with the classification that dominates the feed errors
        # (ties go to the first seen) so the portal title is meaningful. The
        # message names no feed, per IntegrationConfigurationError's contract.
        dominant = max(failures, key=lambda f: sum(1 for g in failures if g.error_type == f.error_type))
        raise type(dominant)(
            f"All {len(feeds_failed)} polled SPOT feeds failed; see the feed errors in this run's activity log"
        )
    return {
        "feeds": len(feeds),
        "observations_sent": observations_sent,
        "observations_dropped": observations_dropped,
        "feeds_failed": feeds_failed,
        "feeds_skipped": feeds_skipped,
    }
