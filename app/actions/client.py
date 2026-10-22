"""Client for the SPOT public feed API (findmespot.com share pages).

Pure interface: plain arguments in, raw SPOT message dicts out. Nothing here
imports from the Gundi runtime, sleeps, retries or logs to the portal; the
action handler owns all of that.

API notes (findmespot.com support docs, verified 2026-10-22):
- GET {base}/{feed_id}/message.json?startDate=...&endDate=...[&feedPassword=...][&start=N]
- dates are "%Y-%m-%dT%H:%M:%S-0000"; only the -0000 offset is honoured
- pages of 50; `start=51`, `start=101`, ... fetch the next ones
- the feed only ever serves the last 7 days
- errors come back as HTTP 200 with {"response": {"errors": {"error": {...}}}}
"""
import json
from datetime import datetime, timezone
from typing import Optional

import httpx

SPOT_API_BASE = "https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed"
PAGE_SIZE = 50
MAX_PAGES = 200
NO_MESSAGES_ERROR_CODE = "E-0160"
SPOT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S-0000"


class SpotFeedError(Exception):
    """Base for SPOT feed failures; `code`/`text` come from SPOT's error envelope when present."""

    def __init__(self, text: str, *, code: Optional[str] = None):
        super().__init__(text)
        self.text = text
        self.code = code


class SpotFeedNotFound(SpotFeedError):
    pass


class SpotFeedAuthError(SpotFeedError):
    pass


class SpotRateLimited(SpotFeedError):
    pass


class SpotBadResponse(SpotFeedError):
    """SPOT answered with something that is not a feed response at all (non-JSON
    body, redirect, unexpected shape): a SPOT-wide problem, not this feed's."""


class SpotFeedRejected(SpotFeedError):
    """SPOT returned an error envelope for this feed that is not recognised as a
    password or feed-id problem: feed-specific, but of unknown cause."""


def format_spot_datetime(dt: datetime) -> str:
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(SPOT_DATETIME_FORMAT)


def _classify_error(code: Optional[str], text: str) -> SpotFeedError:
    lowered = text.lower()
    if "password" in lowered:
        return SpotFeedAuthError(text, code=code)
    if "feed" in lowered and any(hint in lowered for hint in ("not found", "invalid", "does not exist")):
        return SpotFeedNotFound(text, code=code)
    return SpotFeedRejected(text, code=code)


def parse_feed_response(payload: dict) -> list:
    """Return the message dicts of a feed response.

    Returns [] for an empty feed and for SPOT's "no messages" error (E-0160);
    raises a SpotFeedError subclass for every other error envelope or shape.
    """
    response = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(response, dict):
        raise SpotBadResponse("Unexpected SPOT response: no 'response' object")

    if "errors" in response:
        error = (response.get("errors") or {}).get("error")
        if isinstance(error, list):
            error = error[0] if error else None
        if not isinstance(error, dict):
            raise SpotBadResponse("Unexpected SPOT response: malformed error envelope")
        code = error.get("code")
        text = error.get("text") or error.get("description") or "Unknown SPOT error"
        if code == NO_MESSAGES_ERROR_CODE:
            return []
        raise _classify_error(code, text)

    feed_response = response.get("feedMessageResponse")
    if not isinstance(feed_response, dict):
        raise SpotBadResponse("Unexpected SPOT response: no 'feedMessageResponse'")
    messages = (feed_response.get("messages") or {}).get("message")
    if messages is None:
        return []
    if isinstance(messages, dict):  # a single message arrives as a dict, not a one-item list
        return [messages]
    return list(messages)


def _raise_for_client_status(response: httpx.Response) -> None:
    status = response.status_code
    if status < 300:
        return
    if status < 400:
        # Redirects are not followed: the feed password travels in the query
        # string and must not be replayed to wherever SPOT points. The target
        # is deliberately not quoted.
        raise SpotBadResponse(f"SPOT redirected the feed request (HTTP {status})")
    if status >= 500:
        response.raise_for_status()
    text = f"SPOT returned HTTP {status}"
    if status in (401, 403):
        raise SpotFeedAuthError(text)
    if status == 404:
        raise SpotFeedNotFound(text)
    if status == 429:
        raise SpotRateLimited(text)
    raise SpotBadResponse(text)


async def fetch_messages(
    feed_id: str,
    start: datetime,
    end: datetime,
    *,
    feed_password: Optional[str] = None,
    base_url: str = SPOT_API_BASE,
    client: Optional[httpx.AsyncClient] = None,
    timeout: float = 10.0,
) -> list:
    """Fetch every message in [start, end] for one feed, following SPOT's 50-message pages."""
    url = f"{base_url}/{feed_id}/message.json"
    base_params = {"startDate": format_spot_datetime(start), "endDate": format_spot_datetime(end)}
    if feed_password:
        base_params["feedPassword"] = feed_password

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=timeout)
    try:
        messages = []
        total_count = None
        for page_index in range(MAX_PAGES):
            params = dict(base_params)
            if page_index:
                params["start"] = str(1 + PAGE_SIZE * page_index)
            response = await client.get(url, params=params, headers={"Accept": "application/json"})
            _raise_for_client_status(response)
            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError) as e:
                raise SpotBadResponse(f"SPOT returned a non-JSON body: {e}") from e
            page = parse_feed_response(payload)
            messages.extend(page)
            feed_response = payload.get("response", {}).get("feedMessageResponse") or {}
            if isinstance(feed_response.get("totalCount"), int):
                total_count = feed_response["totalCount"]
            if len(page) < PAGE_SIZE or (total_count is not None and len(messages) >= total_count):
                break
        return messages
    finally:
        if own_client:
            await client.aclose()
