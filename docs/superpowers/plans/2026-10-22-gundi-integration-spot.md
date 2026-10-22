# gundi-integration-spot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Gundi v2 action runner that polls SPOT share feeds listed in a connection's configuration every 5 minutes and sends new positions to Gundi as observations, replacing the legacy `das-spot-provider` Lambda.

**Architecture:** Fork of `gundi-integration-action-runner` in the classic `app/` layout. Three new modules under `app/actions/`: a pure `httpx` SPOT client (`client.py`), a transform module (`transform.py`), and config models (`configurations.py`); the handler (`handlers.py`) orchestrates per-feed state, retries, error policy and the Gundi send. No webhook, no auth action, no new runtime dependency.

**Tech Stack:** Python 3.10, FastAPI template runtime, pydantic 1.10 (v1 API), httpx 0.28, Redis-backed `IntegrationStateManager`, pytest + pytest-asyncio + pytest-mock.

**Spec:** `docs/superpowers/specs/2026-10-22-gundi-integration-spot-design.md`

## Global Constraints

- Python 3.10 in CI and Docker; create the local venv with `uv venv --python 3.10 .venv`.
- pydantic `~=1.10.15`: v1 API only (`Field`, `validator`, `.dict()`, `.schema()`, `List[...]`, `Optional[...]`).
- No new runtime dependency: `httpx~=0.28.1` is in `requirements-base.in`; tests use `httpx.MockTransport`, not `respx`.
- Only these files are fork-owned: `app/actions/{client,transform,configurations,handlers}.py`, `app/actions/tests/**`, `README.md`, `docs/**`. Nothing else in the template is edited.
- Client purity: `app/actions/client.py` imports nothing from `app.*`; its functions take plain args; no retries, sleeps, or Gundi logging inside it.
- Secrets: `feed_password` is `pydantic.SecretStr` and never appears in logs, activity events, or exception text.
- SPOT API: base `https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed/{feed_id}/message.json`; dates as `%Y-%m-%dT%H:%M:%S-0000`; pages of 50 via `start=51`, `start=101`, …; 7-day retention; ≥2 s between feeds.
- Schedule `*/5 * * * *`; retries 3 attempts with 1/2/4 s backoff and a 10 s request timeout, in the handler only.
- At most 50 feeds per connection, `initial_lookback_hours` 1..168, default 48; `subject_type` default `security_vehicle`.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- All work on branch `feat/spot-integration` (rename the existing `design/spot-integration`); one PR to `main` at the end.

## Review Focus

1. A `feedMessageResponse` with `count: 0` and no `messages` key (or `messages: null`) must yield an empty list, not a `KeyError`. → Task 1 test `test_parse_feed_response_without_messages_key_is_empty`.
2. A message whose `latitude`/`longitude` are missing or `null` (SPOT custom/OK messages without a fix) must be dropped as invalid, not crash the run. → Task 3 test `test_message_without_position_is_invalid`.
3. A `feed_password` containing `&`, `#` or spaces must reach SPOT URL-encoded in the query string. → Task 1 test `test_fetch_messages_url_encodes_password`.
4. Messages SPOT returns out of order or re-sent (a `unixTime` at or below the stored high-water mark) must be skipped and must never move state backwards. → Task 4 test `test_messages_at_or_below_state_are_skipped_and_state_never_regresses`.
5. Two feeds where only one has stored state: the new feed uses the initial lookback while the other continues from its mark; state is per feed. → Task 4 test `test_state_is_tracked_per_feed`.

---

### Task 1: SPOT client

**Files:**
- Create: `app/actions/client.py`
- Create: `app/actions/tests/fixtures/spot_feed_response.json` (copy of `/Users/chrisdo/padas/das-spot-provider/das_spot/tests/test_data.json`, 51 messages)
- Create: `app/actions/tests/conftest.py`
- Test: `app/actions/tests/test_client.py`

**Interfaces:**
- Consumes: nothing from the template.
- Produces (all in `app/actions/client.py`):
  - `SPOT_API_BASE = "https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed"`, `PAGE_SIZE = 50`, `MAX_PAGES = 200`, `NO_MESSAGES_ERROR_CODE = "E-0160"`
  - `class SpotFeedError(Exception)`: `__init__(self, text: str, *, code: str | None = None)`, stores `self.text`, `self.code`, and `str(exc) == text`; subclasses `SpotFeedNotFound`, `SpotFeedAuthError`, `SpotRateLimited`, `SpotBadResponse`
  - `format_spot_datetime(dt: datetime) -> str`
  - `parse_feed_response(payload: dict) -> list[dict]`
  - `async fetch_messages(feed_id: str, start: datetime, end: datetime, *, feed_password: str | None = None, base_url: str = SPOT_API_BASE, client: httpx.AsyncClient | None = None, timeout: float = 10.0) -> list[dict]`

- [ ] **Step 1: Set up the working branch and environment**

```bash
cd /Users/chrisdo/padas/gundi-integration-spot
git branch -m design/spot-integration feat/spot-integration
uv venv --python 3.10 .venv && source .venv/bin/activate
pip install pip-tools typing_extensions
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in
pip install -r requirements.txt
pytest -q   # template suite must pass before any fork code exists
```
Expected: all template tests pass. Do not commit `requirements.txt` changes unless `requirements.in` changed (it did not).

- [ ] **Step 2: Add the fixture and a conftest that loads it**

Copy the legacy JSON to `app/actions/tests/fixtures/spot_feed_response.json`. In `app/actions/tests/conftest.py` add:

```python
@pytest.fixture
def spot_feed_payload() -> dict: ...        # json.load of the fixture file
@pytest.fixture
def spot_messages(spot_feed_payload) -> list[dict]: ...  # payload["response"]["feedMessageResponse"]["messages"]["message"]
```

- [ ] **Step 3: Write the failing client tests**

In `app/actions/tests/test_client.py`, using `httpx.MockTransport` handlers that record each `httpx.Request` and return canned JSON:

```python
def test_format_spot_datetime_uses_minus_0000_offset():
    assert format_spot_datetime(datetime(2026, 10, 22, 13, 5, 7, 123456, tzinfo=timezone.utc)) == "2026-10-22T13:05:07-0000"
    # an aware non-UTC datetime is converted to UTC first
    assert format_spot_datetime(datetime(2026, 10, 22, 16, 5, 7, tzinfo=timezone(timedelta(hours=3)))) == "2026-10-22T13:05:07-0000"

def test_parse_feed_response_returns_message_list(spot_feed_payload, spot_messages):
    assert parse_feed_response(spot_feed_payload) == spot_messages

def test_parse_feed_response_normalises_single_message_dict(spot_feed_payload):
    # messages.message is a dict when exactly one message is returned
    ...; assert parse_feed_response(payload) == [the_dict]

def test_parse_feed_response_without_messages_key_is_empty():
    assert parse_feed_response({"response": {"feedMessageResponse": {"count": 0}}}) == []
    assert parse_feed_response({"response": {"feedMessageResponse": {"count": 0, "messages": None}}}) == []

def test_parse_feed_response_no_messages_error_is_empty():
    payload = {"response": {"errors": {"error": {"code": "E-0160", "text": "No Messages to display", "description": "..."}}}}
    assert parse_feed_response(payload) == []

@pytest.mark.parametrize("text, exc", [
    ("Feed password is incorrect", SpotFeedAuthError),
    ("Feed password required", SpotFeedAuthError),
    ("Feed not found", SpotFeedNotFound),
    ("Invalid feed id", SpotFeedNotFound),
    ("Something else", SpotBadResponse),
])
def test_parse_feed_response_error_envelope_raises(text, exc):
    with pytest.raises(exc) as info: parse_feed_response({"response": {"errors": {"error": {"code": "E-0195", "text": text}}}})
    assert info.value.code == "E-0195" and info.value.text == text

def test_parse_feed_response_error_list_uses_first():  # errors.error may be a list
def test_parse_feed_response_unexpected_shape_raises_bad_response():  # {"response": {}} and {"foo": 1}

async def test_fetch_messages_builds_url_and_params():
    # asserts request.url.path ends with /FEEDID/message.json and query has
    # startDate=2026-10-20T00:00:00-0000, endDate=2026-10-22T00:00:00-0000, no feedPassword, no start
async def test_fetch_messages_sends_password_when_set():  # feedPassword=secret in query
async def test_fetch_messages_url_encodes_password():     # password "a&b #c" → "a%26b+%23c" or "a%26b%20%23c" in request.url.query
async def test_fetch_messages_follows_pages_until_short_page():
    # three canned pages of 50, 50, 7 → 107 messages; requests carry no start, start=51, start=101
async def test_fetch_messages_stops_at_total_count():      # page 1 has totalCount=50, count=50 → exactly one request
async def test_fetch_messages_stops_at_max_pages(monkeypatch):  # MAX_PAGES=2, every page full → 100 messages, 2 requests
@pytest.mark.parametrize("status, exc", [(401, SpotFeedAuthError), (403, SpotFeedAuthError), (404, SpotFeedNotFound), (429, SpotRateLimited), (400, SpotBadResponse)])
async def test_fetch_messages_maps_4xx(status, exc):
async def test_fetch_messages_5xx_propagates_httpx_error():  # 503 → httpx.HTTPStatusError
async def test_fetch_messages_non_json_raises_bad_response():  # 200 text/html body
async def test_fetch_messages_uses_given_client_without_closing_it():
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `pytest app/actions/tests/test_client.py -q`
Expected: FAIL with `ModuleNotFoundError: app.actions.client`

- [ ] **Step 5: Implement `app/actions/client.py`**

Signatures as in Interfaces. Decisions the tests do not fix:
- `parse_feed_response` reads `payload["response"]`; if it has `errors`, take `errors["error"]` (first element if a list), build `code`/`text` (fallback `description`), return `[]` for `E-0160`, else classify by lower-cased text: contains `"password"` → `SpotFeedAuthError`; contains `"feed"` and any of `"not found"`, `"invalid"`, `"does not exist"` → `SpotFeedNotFound`; otherwise `SpotBadResponse`. If neither `feedMessageResponse` nor `errors` is present → `SpotBadResponse`. **Note for the builder:** the codes/texts here are the best available from community reports; capture live envelopes for "bad password", "unknown feed" and "no messages" during Task 5 and adjust the table and the parametrised test together.
- `fetch_messages` creates an `httpx.AsyncClient(timeout=timeout)` only when `client` is `None` and closes only what it created. Page loop: first request without `start`; subsequent `start = 1 + PAGE_SIZE * page_index`; stop when a page has fewer than `PAGE_SIZE`, when accumulated ≥ `totalCount` (if present), or after `MAX_PAGES`. Status mapping before JSON parsing: 401/403 → `SpotFeedAuthError`, 404 → `SpotFeedNotFound`, 429 → `SpotRateLimited`, other 4xx → `SpotBadResponse`, 5xx → `response.raise_for_status()` (propagates `httpx.HTTPStatusError`). `json.JSONDecodeError`/`ValueError` from `response.json()` → `SpotBadResponse`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pytest app/actions/tests/test_client.py -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add app/actions/client.py app/actions/tests/
git commit -m "feat: add a pure SPOT public feed client"
```

---

### Task 2: Configuration models

**Files:**
- Create: `app/actions/configurations.py` (currently empty)
- Test: `app/actions/tests/test_configurations.py`

**Interfaces:**
- Consumes: `PullActionConfiguration` from `app.actions.core`; `GlobalUISchemaOptions` from `app.services.utils`.
- Produces:
  - `class SpotFeed(pydantic.BaseModel)`: `feed_id: str` (required, title "Feed ID"), `feed_password: Optional[SecretStr] = None` (title "Feed password", `format="password"`), `name: Optional[str] = None` (title "Name").
  - `class PullObservationsConfig(PullActionConfiguration)`: `feeds: List[SpotFeed]` (required, `min_items=1`, `max_items=50`, title "SPOT feeds"), `subject_type: str = "security_vehicle"` (title "Subject type"), `initial_lookback_hours: int = 48` (`ge=1`, `le=168`, title "Initial lookback (hours)"), `ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(order=["feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"])`, and a `ui_schema` classmethod override.
  - `_reference(action: str, params: Optional[dict] = None, *, target: str = "self") -> dict` (same shape as `gundi-integration-africam`'s helper).
  - `dedupe_feeds(feeds: List[SpotFeed]) -> List[SpotFeed]` — first occurrence of each `feed_id` wins, order preserved.

- [ ] **Step 1: Write the failing tests**

```python
def test_minimal_config_has_spec_defaults():
    cfg = PullObservationsConfig(feeds=[{"feed_id": "abc"}])
    assert cfg.subject_type == "security_vehicle" and cfg.initial_lookback_hours == 48 and cfg.run_on_schedule is True
    assert cfg.feeds[0].feed_password is None and cfg.feeds[0].name is None

def test_feed_password_is_a_secret():
    cfg = PullObservationsConfig(feeds=[{"feed_id": "abc", "feed_password": "hunter2"}])
    assert "hunter2" not in repr(cfg) and "hunter2" not in str(cfg.dict())
    assert cfg.feeds[0].feed_password.get_secret_value() == "hunter2"

@pytest.mark.parametrize("feeds", [[], [{"feed_id": str(i)} for i in range(51)]])
def test_feed_count_bounds(feeds):
    with pytest.raises(ValidationError): PullObservationsConfig(feeds=feeds)

@pytest.mark.parametrize("hours", [0, 169])
def test_lookback_bounds(hours):
    with pytest.raises(ValidationError): PullObservationsConfig(feeds=[{"feed_id": "a"}], initial_lookback_hours=hours)

def test_json_schema_exposes_portal_fields():
    schema = PullObservationsConfig.schema()
    assert set(schema["properties"]) == {"feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"}
    assert schema["properties"]["feeds"]["maxItems"] == 50 and schema["properties"]["feeds"]["minItems"] == 1
    assert schema["definitions"]["SpotFeed"]["properties"]["feed_password"]["format"] == "password"

def test_ui_schema_annotates_subject_type_and_password():
    ui = PullObservationsConfig.ui_schema()
    assert ui["ui:order"] == ["feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"]
    assert ui["subject_type"] == {"gundi:reference": {"action": "list_subject_types", "target": "destination", "params": {}, "allow_free_text": True}}
    assert "ui:widget" not in ui["subject_type"]
    assert ui["feeds"] == {"items": {"feed_password": {"ui:widget": "password"}}}

def test_dedupe_feeds_first_wins():
    feeds = [SpotFeed(feed_id="a", feed_password="1"), SpotFeed(feed_id="b"), SpotFeed(feed_id="a", feed_password="2")]
    out = dedupe_feeds(feeds)
    assert [f.feed_id for f in out] == ["a", "b"] and out[0].feed_password.get_secret_value() == "1"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest app/actions/tests/test_configurations.py -q`
Expected: FAIL with `ImportError` (names not defined)

- [ ] **Step 3: Implement `app/actions/configurations.py`**

Models as in Interfaces. Field descriptions (portal help text), verbatim:
- `feed_id`: "The feed ID from the SPOT share page URL."
- `feed_password`: "Only needed if the share page is password protected."
- `name`: "Optional label shown in the activity log."
- `feeds`: "One row per SPOT share page (up to 50)."
- `subject_type`: "EarthRanger subject subtype for these trackers. Pick from the connected site's list, or type one."
- `initial_lookback_hours`: "How far back to fetch the first time a feed is seen. SPOT keeps 7 days at most."

`ui_schema` calls `super().ui_schema(*args, **kwargs)` then sets the two entries exactly as the test asserts.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest app/actions/tests/test_configurations.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/actions/tests/test_configurations.py
git commit -m "feat: add the SPOT pull_observations configuration model"
```

---

### Task 3: Transform SPOT messages to Gundi observations

**Files:**
- Create: `app/actions/transform.py`
- Test: `app/actions/tests/test_transform.py`

**Interfaces:**
- Consumes: `spot_messages` fixture from Task 1.
- Produces (in `app/actions/transform.py`):
  - `OBS_MIN_YEAR = 2010`, `SOURCE_TYPE = "tracking-device"`
  - `is_valid_message(message: dict) -> bool`
  - `message_to_observation(message: dict, *, subject_type: str, feed_id: str) -> dict`
  - `messages_to_observations(messages: list[dict], *, subject_type: str, feed_id: str) -> tuple[list[dict], int]` returning `(observations, dropped_count)`

- [ ] **Step 1: Write the failing tests**

```python
def test_fixture_maps_every_message(spot_messages):
    observations, dropped = messages_to_observations(spot_messages, subject_type="ranger", feed_id="F1")
    assert len(observations) == 51 and dropped == 0

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
            "messengerId": "0-2825446", "messengerName": "DAS Spot", "messageType": "UNLIMITED-TRACK",
            "modelId": "SPOT3", "batteryState": "GOOD", "altitude": 68, "feedId": "F1",
        },
    }

def test_optional_additional_fields_are_omitted(spot_messages):
    msg = {k: v for k, v in spot_messages[0].items() if k not in ("batteryState", "altitude")}
    additional = message_to_observation(msg, subject_type="x", feed_id="F")["additional"]
    assert "batteryState" not in additional and "altitude" not in additional

@pytest.mark.parametrize("patch", [
    {"unixTime": -30610224000},            # year 1000: decommissioned device
    {"unixTime": 1262304000},              # 2010-01-01: year must be > 2010
    {"latitude": 91.0}, {"latitude": -90.5}, {"longitude": 180.1}, {"longitude": -181},
])
def test_invalid_messages_are_dropped(spot_messages, patch):
    assert is_valid_message({**spot_messages[0], **patch}) is False

@pytest.mark.parametrize("make", [
    lambda m: {**m, "latitude": None},
    lambda m: {**m, "longitude": None},
    lambda m: {k: v for k, v in m.items() if k != "latitude"},
    lambda m: {k: v for k, v in m.items() if k != "unixTime"},
])
def test_message_without_position_is_invalid(spot_messages, make):
    assert is_valid_message(make(spot_messages[0])) is False

def test_messages_to_observations_counts_dropped(spot_messages):
    bad = {**spot_messages[0], "latitude": 999}
    observations, dropped = messages_to_observations([bad, spot_messages[1]], subject_type="x", feed_id="F")
    assert len(observations) == 1 and dropped == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest app/actions/tests/test_transform.py -q`
Expected: FAIL with `ModuleNotFoundError: app.actions.transform`

- [ ] **Step 3: Implement `app/actions/transform.py`**

`is_valid_message` requires numeric `latitude` in [-90, 90], numeric `longitude` in [-180, 180], integer `unixTime` whose UTC year is > `OBS_MIN_YEAR`; any missing key or `None` is invalid. `recorded_at` is `datetime.fromtimestamp(unix_time, tz=timezone.utc).isoformat()`. `location` values are `float`. `messages_to_observations` never raises on a bad message; it counts it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest app/actions/tests/test_transform.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/actions/transform.py app/actions/tests/test_transform.py
git commit -m "feat: transform SPOT messages into Gundi observations"
```

---

### Task 4: The `pull_observations` handler

**Files:**
- Create: `app/actions/handlers.py` (currently empty)
- Test: `app/actions/tests/test_handlers.py`

**Interfaces:**
- Consumes: Task 1 `fetch_messages` and the `Spot*` exceptions; Task 2 `PullObservationsConfig`, `SpotFeed`, `dedupe_feeds`; Task 3 `messages_to_observations`; template `IntegrationStateManager` (`app.services.state`), `send_observations_to_gundi` (`app.services.gundi`), `activity_logger`, `log_action_activity` (`app.services.activity_logger`), `crontab_schedule` (`app.services.action_scheduler`), `action_title` (`app.actions.core`), `Integration*Error` classes (`app.services.errors`), `LogLevel` (`gundi_core.events`).
- Produces (in `app/actions/handlers.py`):
  - Constants `FEED_REQUEST_SPACING_SECONDS = 2`, `SPOT_RETENTION_DAYS = 7`, `RETRY_BACKOFF_SECONDS = (1, 2, 4)`, `REQUEST_TIMEOUT_SECONDS = 10.0`, `ACTION_ID = "pull_observations"`.
  - `_sleep = asyncio.sleep` (module-level alias so tests patch `app.actions.handlers._sleep`).
  - `async action_pull_observations(integration: Integration, action_config: PullObservationsConfig) -> dict`, decorated top-to-bottom with `@action_title("Pull Observations")`, `@crontab_schedule("*/5 * * * *")`, `@activity_logger()`.
  - `_compute_window(last_unix_time: Optional[int], *, initial_lookback_hours: int, now: datetime) -> tuple[datetime, datetime]`
  - `async _fetch_with_retries(feed_id: str, start: datetime, end: datetime, *, feed_password: Optional[str]) -> list[dict]`
  - `_classify_feed_error(exc: Exception) -> IntegrationError`
  - `async _pull_feed(integration_id: str, feed: SpotFeed, *, subject_type: str, initial_lookback_hours: int, state_manager: IntegrationStateManager, now: datetime) -> tuple[int, int]` returning `(sent, dropped)`.

- [ ] **Step 1: Write the failing tests**

Patch targets for every handler test: `app.actions.handlers.IntegrationStateManager` (returns an `AsyncMock` whose `get_state` returns a dict and `set_state` records calls), `app.actions.handlers.fetch_messages` (`AsyncMock`), `app.actions.handlers.send_observations_to_gundi` (`AsyncMock`), `app.actions.handlers._sleep` (`AsyncMock`), `app.services.activity_logger.publish_event` (the template's `mock_publish_event` fixture), and `app.actions.handlers.log_action_activity` (`AsyncMock`). Build the `Integration` fixture like `gundi-integration-spidertracks/app/actions/tests/test_handlers.py::integration_with_auth`, with type value `spot` and a single `pull_observations` action and no configurations. Call the handler with keyword arguments (`integration=`, `action_config=`) so `activity_logger` finds them. Use a fixed `NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)` and patch `app.actions.handlers._now` (another module alias, `_now = lambda: datetime.now(tz=timezone.utc)`).

```python
def test_compute_window_first_run_uses_lookback():
    start, end = _compute_window(None, initial_lookback_hours=48, now=NOW)
    assert (start, end) == (NOW - timedelta(hours=48), NOW)

def test_compute_window_continues_one_second_after_state():
    start, _ = _compute_window(1_700_000_000, initial_lookback_hours=48, now=NOW)
    assert start == datetime.fromtimestamp(1_700_000_001, tz=timezone.utc)

def test_compute_window_clamps_to_seven_days():
    start, _ = _compute_window(1_000_000_000, initial_lookback_hours=168, now=NOW)
    assert start == NOW - timedelta(days=7)
    start, _ = _compute_window(None, initial_lookback_hours=168, now=NOW)
    assert start == NOW - timedelta(days=7)

async def test_first_run_sends_observations_and_writes_state(...):
    # state {} ; fetch returns spot_messages ; expect one send with 51 observations whose subject_type is the config's,
    # set_state called once with (integration_id, "pull_observations", {"last_unix_time": max unixTime}, source_id=feed_id),
    # result == {"feeds": 1, "observations_sent": 51, "observations_dropped": 0, "feeds_failed": []}

async def test_second_run_requests_from_state_and_filters_client_side(...):
    # state {"last_unix_time": T}; fetch returns messages with unixTime T-1, T, T+1 → only T+1 is sent; fetch was called with start == T+1 s

async def test_messages_at_or_below_state_are_skipped_and_state_never_regresses(...):
    # state {"last_unix_time": T}; fetch returns only unixTime <= T → nothing sent, set_state NOT called

async def test_state_is_tracked_per_feed(...):
    # feeds A (state {"last_unix_time": T}) and B (state {}) → fetch called with A's start = T+1 s and B's start = NOW - 48 h;
    # set_state called with source_id "A" and "B" separately

async def test_duplicate_feed_ids_fetch_once(...):          # feeds [A, A] → fetch called once; result["feeds"] == 1
async def test_sleeps_between_feeds_but_not_after_last(...):  # 3 feeds → _sleep awaited twice with 2
async def test_invalid_messages_are_dropped_and_counted(...): # result["observations_dropped"] == 1, send got 50
async def test_nothing_to_send_skips_gundi(...):             # fetch returns [] → send not called, set_state not called

async def test_feed_auth_error_is_logged_and_next_feed_runs(...):
    # feed A raises SpotFeedAuthError, feed B succeeds → log_action_activity called once at LogLevel.ERROR with data["feed_id"] == "A",
    # the password never appears in any call's args, result["feeds_failed"] == ["A"], B's observations sent

@pytest.mark.parametrize("exc, expected_type", [(SpotFeedAuthError, "auth"), (SpotFeedNotFound, "configuration"), (SpotRateLimited, "rate_limit"), (SpotBadResponse, "bad_response")])
def test_classify_feed_error(exc, expected_type):
    assert _classify_feed_error(exc("x", code="E-0001")).error_type == expected_type

async def test_all_feeds_failed_raises_configuration_error(...):
    # both feeds raise SpotFeedNotFound → pytest.raises(IntegrationConfigurationError); message contains no feed id

async def test_transport_error_retries_then_aborts(...):
    # fetch raises httpx.ConnectError 3 times for feed A → _sleep awaited with 1, 2 (backoff) ; IntegrationConnectionError raised ; feed B never fetched
async def test_5xx_retries_then_aborts(...):                 # httpx.HTTPStatusError(503) ×3 → IntegrationBadResponseError
async def test_transport_error_recovers_on_second_attempt(...): # ConnectError then messages → sent normally, no error logged
async def test_4xx_is_not_retried(...):                      # SpotFeedAuthError → fetch called once

async def test_gundi_send_failure_aborts_and_keeps_state(...):
    # feed A sent OK (state written), feed B's send raises GundiAPIError → exception propagates, set_state called only for A, B not in feeds_failed

async def test_overlapping_run_is_skipped(...):              # hold the lock for the integration id → result == {"skipped": True}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest app/actions/tests/test_handlers.py -q`
Expected: FAIL with `ImportError` from `app.actions.handlers`

- [ ] **Step 3: Implement `app/actions/handlers.py`**

Algorithm for `action_pull_observations` (the tests fix the rest):
1. Per-integration `asyncio.Lock` as in spidertracks; if held, return `{"skipped": True}`.
2. `feeds = dedupe_feeds(action_config.feeds)`; `now = _now()`; `state_manager = IntegrationStateManager()`.
3. For each feed: `try: sent, dropped = await _pull_feed(...)` ; `except SpotFeedError as e:` classify, `await log_action_activity(integration_id=..., action_id=ACTION_ID, title=f"SPOT feed {feed.feed_id}: {classified.default_title}", level=LogLevel.ERROR, data={"feed_id": feed.feed_id, "feed_name": feed.name, "error_type": classified.error_type, "message": classified.message})`, append to `feeds_failed`. Everything else (`httpx.HTTPError` after retries, Gundi errors) propagates; the `httpx` ones are re-raised as `IntegrationConnectionError` (transport) / `IntegrationBadResponseError` (`HTTPStatusError`). Sleep `FEED_REQUEST_SPACING_SECONDS` between feeds, not after the last.
4. If `feeds_failed` and no feed succeeded: `raise IntegrationConfigurationError(f"All {len(feeds)} SPOT feeds failed; see the feed errors in this run's activity log")`.
5. Return the summary dict.

`_pull_feed`: read state with `source_id=feed.feed_id`; `_compute_window`; `_fetch_with_retries`; keep messages with `unixTime > last_unix_time` (when there is one); `messages_to_observations`; if observations, `send_observations_to_gundi(observations=..., integration_id=str(integration_id))`; then, if the max `unixTime` seen among *valid* messages exceeds the stored mark (or there was none), `set_state(... {"last_unix_time": max_seen}, source_id=feed.feed_id)`. State is written only after a successful send.

`_fetch_with_retries`: up to `len(RETRY_BACKOFF_SECONDS)` retries on `httpx.TransportError` and `httpx.HTTPStatusError` with status ≥ 500, awaiting `_sleep(backoff)` between attempts; passes `timeout=REQUEST_TIMEOUT_SECONDS`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest app/actions/tests/test_handlers.py -q`
Expected: PASS

- [ ] **Step 5: Run the whole suite, including the template's registration tests**

Run: `pytest -q`
Expected: PASS, and `python -c "from app.actions.core import get_actions; print(get_actions())"` prints `['pull_observations']`.

- [ ] **Step 6: Commit**

```bash
git add app/actions/handlers.py app/actions/tests/test_handlers.py
git commit -m "feat: pull SPOT feed observations on a 5-minute schedule"
```

---

### Task 5: README, infra provisioning, registration, and the PR

**Files:**
- Modify: `README.md` (replace the template's first heading and "Usage" intro with SPOT-specific text; keep the rest)
- Create (other repo): `gundi-integrations-v2-infra/environments/{dev,stage,prod}/integrations/gundi-integration-spot/terragrunt.hcl`

**Interfaces:**
- Consumes: everything above; `gundi-integrations-v2-infra/create_integration.py`.
- Produces: an open PR on `PADAS/gundi-integration-spot` and one on `PADAS/gundi-integrations-v2-infra`.

- [ ] **Step 1: Rewrite the README intro**

Title `# gundi-integration-spot`, one paragraph: what SPOT is, that feeds are configured per connection (link the spec), the 5-minute schedule and the 7-day/50-feed limits, and a "Registering" subsection with the exact command:

```bash
python -m app.register --slug spot --name "SPOT" --schedule "pull_observations:*/5 * * * *"
```
plus the note that the template registers a generic description, so after the first registration set the integration type description in the Gundi admin to: `SPOT satellite messengers (Globalstar), via findmespot.com share feeds`. Keep the template's local-development and webhook sections below unchanged.

- [ ] **Step 2: Verify the app boots and registration metadata is right**

```bash
INTEGRATION_TYPE_SLUG=spot INTEGRATION_TYPE_NAME=SPOT python -c "
from app.services.self_registration import *; from app.actions.core import discover_actions
h = discover_actions('app.actions.handlers', 'action_')['pull_observations']
print(h[0].action_title, h[0].crontab_schedule, list(h[1].schema()['properties']))"
```
Expected: `Pull Observations`, a crontab object for `*/5 * * * *`, and `['run_on_schedule', 'feeds', 'subject_type', 'initial_lookback_hours']` (any order).

- [ ] **Step 3: Commit and open the integration PR**

```bash
git add README.md docs/
git commit -m "docs: describe the SPOT integration and how to register it"
git push -u origin feat/spot-integration
gh pr create --base main --title "SPOT integration: pull share-feed observations" --body "<summary of the design + link to spec; ends with the attribution line>"
```
Expected: `pr.yaml` runs the tests and goes green.

- [ ] **Step 4: Provision infra**

```bash
cd /Users/chrisdo/padas/gundi-integrations-v2-infra && git checkout main && git pull
git checkout -b feat/gundi-integration-spot
./create_integration.py spot
git add environments && git commit -m "Add gundi-integration-spot to dev, stage and prod"
git push -u origin feat/gundi-integration-spot && gh pr create --title "Provision gundi-integration-spot" --body "..."
```
Expected: three `terragrunt.hcl` files with `name = "spot"` and the base image; default inputs (the spec sizes one small request per feed every 5 minutes as light).

- [ ] **Step 5: Register in Gundi dev and capture live SPOT error envelopes (needs the user's credentials)**

With `GUNDI_API_BASE_URL`, `GUNDI_USERNAME`, `GUNDI_PASSWORD`, `GUNDI_OAUTH_CLIENT_ID=cdip-oauth2` set for dev, run the register command from Step 1. Then, with a real feed ID the user supplies, call `fetch_messages` once each for: the real feed with a wrong password, a made-up feed ID, and a window with no messages. Paste the three envelopes into Task 1's parametrised test and adjust `parse_feed_response` if the texts differ from the guesses. Commit as `test: pin SPOT error envelopes from live responses`.

- [ ] **Step 6: Final full run**

Run: `pytest -q`
Expected: PASS. Both PRs open; integration PR CI green.
