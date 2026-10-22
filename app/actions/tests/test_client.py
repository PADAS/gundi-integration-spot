import copy
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.actions.client import (
    MAX_PAGES,
    PAGE_SIZE,
    SPOT_API_BASE,
    SpotBadResponse,
    SpotFeedAuthError,
    SpotFeedError,
    SpotFeedNotFound,
    SpotRateLimited,
    fetch_messages,
    format_spot_datetime,
    parse_feed_response,
)

START = datetime(2026, 10, 20, tzinfo=timezone.utc)
END = datetime(2026, 10, 22, tzinfo=timezone.utc)


def _page(messages, total_count=None):
    body = {"response": {"feedMessageResponse": {
        "count": len(messages),
        "totalCount": total_count if total_count is not None else len(messages),
        "messages": {"message": messages},
    }}}
    return body


def _messages(n, start_id=0):
    return [{"id": start_id + i, "unixTime": 1_700_000_000 + i, "latitude": 1.0, "longitude": 2.0} for i in range(n)]


class Recorder:
    """A MockTransport handler that records requests and serves canned responses in order."""

    def __init__(self, responses):
        self.requests = []
        self.responses = list(responses)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(response, httpx.Response):
            return response
        status, body = response
        if isinstance(body, (dict, list)):
            return httpx.Response(status, json=body)
        return httpx.Response(status, text=body, headers={"content-type": "text/html"})


def _client(recorder):
    return httpx.AsyncClient(transport=httpx.MockTransport(recorder))


def test_format_spot_datetime_uses_minus_0000_offset():
    assert format_spot_datetime(datetime(2026, 10, 22, 13, 5, 7, 123456, tzinfo=timezone.utc)) == "2026-10-22T13:05:07-0000"
    assert format_spot_datetime(datetime(2026, 10, 22, 16, 5, 7, tzinfo=timezone(timedelta(hours=3)))) == "2026-10-22T13:05:07-0000"


def test_parse_feed_response_returns_message_list(spot_feed_payload, spot_messages):
    assert parse_feed_response(spot_feed_payload) == spot_messages


def test_parse_feed_response_normalises_single_message_dict(spot_feed_payload, spot_messages):
    payload = copy.deepcopy(spot_feed_payload)
    only = spot_messages[0]
    payload["response"]["feedMessageResponse"]["messages"]["message"] = only
    payload["response"]["feedMessageResponse"]["count"] = 1
    assert parse_feed_response(payload) == [only]


def test_parse_feed_response_without_messages_key_is_empty():
    assert parse_feed_response({"response": {"feedMessageResponse": {"count": 0}}}) == []
    assert parse_feed_response({"response": {"feedMessageResponse": {"count": 0, "messages": None}}}) == []


def test_parse_feed_response_no_messages_error_is_empty():
    payload = {"response": {"errors": {"error": {
        "code": "E-0160", "text": "No Messages to display", "description": "No displayable messages found for feed",
    }}}}
    assert parse_feed_response(payload) == []


@pytest.mark.parametrize("text, exc", [
    ("Feed password is incorrect", SpotFeedAuthError),
    ("Feed password required", SpotFeedAuthError),
    ("Feed not found", SpotFeedNotFound),
    ("Invalid feed id", SpotFeedNotFound),
    ("Something else", SpotBadResponse),
])
def test_parse_feed_response_error_envelope_raises(text, exc):
    with pytest.raises(exc) as info:
        parse_feed_response({"response": {"errors": {"error": {"code": "E-0195", "text": text}}}})
    assert info.value.code == "E-0195"
    assert info.value.text == text
    assert str(info.value) == text


def test_parse_feed_response_error_list_uses_first():
    payload = {"response": {"errors": {"error": [
        {"code": "E-0001", "text": "Feed password required"},
        {"code": "E-0002", "text": "Something else"},
    ]}}}
    with pytest.raises(SpotFeedAuthError) as info:
        parse_feed_response(payload)
    assert info.value.code == "E-0001"


@pytest.mark.parametrize("payload", [{"response": {}}, {"foo": 1}, {"response": {"errors": {}}}])
def test_parse_feed_response_unexpected_shape_raises_bad_response(payload):
    with pytest.raises(SpotBadResponse):
        parse_feed_response(payload)


@pytest.mark.asyncio
async def test_fetch_messages_builds_url_and_params():
    recorder = Recorder([(200, _page(_messages(3)))])
    async with _client(recorder) as client:
        messages = await fetch_messages("FEEDID", START, END, client=client)
    assert messages == _messages(3)
    request = recorder.requests[0]
    assert str(request.url).startswith(SPOT_API_BASE)
    assert request.url.path.endswith("/FEEDID/message.json")
    assert request.url.params["startDate"] == "2026-10-20T00:00:00-0000"
    assert request.url.params["endDate"] == "2026-10-22T00:00:00-0000"
    assert "feedPassword" not in request.url.params
    assert "start" not in request.url.params


@pytest.mark.asyncio
async def test_fetch_messages_sends_password_when_set():
    recorder = Recorder([(200, _page([]))])
    async with _client(recorder) as client:
        await fetch_messages("F", START, END, feed_password="secret", client=client)
    assert recorder.requests[0].url.params["feedPassword"] == "secret"


@pytest.mark.asyncio
async def test_fetch_messages_url_encodes_password():
    recorder = Recorder([(200, _page([]))])
    async with _client(recorder) as client:
        await fetch_messages("F", START, END, feed_password="a&b #c", client=client)
    query = recorder.requests[0].url.query.decode()
    assert "a&b" not in query and "#" not in query and " " not in query
    assert recorder.requests[0].url.params["feedPassword"] == "a&b #c"


@pytest.mark.asyncio
async def test_fetch_messages_follows_pages_until_short_page():
    pages = [
        (200, _page(_messages(50, 0), total_count=107)),
        (200, _page(_messages(50, 50), total_count=107)),
        (200, _page(_messages(7, 100), total_count=107)),
    ]
    recorder = Recorder(pages)
    async with _client(recorder) as client:
        messages = await fetch_messages("F", START, END, client=client)
    assert len(messages) == 107
    assert [m["id"] for m in messages] == list(range(107))
    starts = [r.url.params.get("start") for r in recorder.requests]
    assert starts == [None, "51", "101"]


@pytest.mark.asyncio
async def test_fetch_messages_stops_at_total_count():
    recorder = Recorder([(200, _page(_messages(50), total_count=50))])
    async with _client(recorder) as client:
        messages = await fetch_messages("F", START, END, client=client)
    assert len(messages) == 50
    assert len(recorder.requests) == 1


@pytest.mark.asyncio
async def test_fetch_messages_stops_at_max_pages(monkeypatch):
    monkeypatch.setattr("app.actions.client.MAX_PAGES", 2)
    recorder = Recorder([(200, _page(_messages(PAGE_SIZE), total_count=10_000))])
    async with _client(recorder) as client:
        messages = await fetch_messages("F", START, END, client=client)
    assert len(messages) == 2 * PAGE_SIZE
    assert len(recorder.requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status, exc", [
    (401, SpotFeedAuthError), (403, SpotFeedAuthError), (404, SpotFeedNotFound),
    (429, SpotRateLimited), (400, SpotBadResponse),
])
async def test_fetch_messages_maps_4xx(status, exc):
    recorder = Recorder([(status, "nope")])
    async with _client(recorder) as client:
        with pytest.raises(exc) as info:
            await fetch_messages("F", START, END, client=client)
    assert isinstance(info.value, SpotFeedError)


@pytest.mark.asyncio
async def test_fetch_messages_5xx_propagates_httpx_error():
    recorder = Recorder([(503, "down")])
    async with _client(recorder) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_messages("F", START, END, client=client)


@pytest.mark.asyncio
async def test_fetch_messages_non_json_raises_bad_response():
    recorder = Recorder([(200, "<html>maintenance</html>")])
    async with _client(recorder) as client:
        with pytest.raises(SpotBadResponse):
            await fetch_messages("F", START, END, client=client)


@pytest.mark.asyncio
async def test_fetch_messages_uses_given_client_without_closing_it():
    recorder = Recorder([(200, _page([]))])
    client = _client(recorder)
    await fetch_messages("F", START, END, client=client)
    assert not client.is_closed
    await client.aclose()


@pytest.mark.asyncio
async def test_fetch_messages_creates_and_closes_its_own_client(monkeypatch):
    created = []
    real_async_client = httpx.AsyncClient

    def factory(**kwargs):
        client = real_async_client(transport=httpx.MockTransport(Recorder([(200, _page([]))])), **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr("app.actions.client.httpx.AsyncClient", factory)
    assert await fetch_messages("F", START, END, timeout=3.0) == []
    assert len(created) == 1 and created[0].is_closed
    assert created[0].timeout == httpx.Timeout(3.0)
