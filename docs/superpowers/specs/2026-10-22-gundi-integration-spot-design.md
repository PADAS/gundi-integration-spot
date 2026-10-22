# gundi-integration-spot — Design

**Date:** 2026-10-22
**Status:** Approved by the user on 2026-10-22; implementation plan to follow
**Replaces:** `PADAS/das-spot-provider`, the Zappa/AWS Lambda ETL that
polls SPOT share feeds and posts observations straight to EarthRanger.

## Intent

SPOT satellite messengers (findmespot.com) publish their positions on a
*share page* whose contents are also available as a public feed API, keyed
by a feed ID and optionally protected by a feed password. Today one Lambda
per site polls those feeds and posts directly to that site's EarthRanger,
reading feed IDs out of an EarthRanger source group. Each site is a separate
deployment with hand-edited secrets in a `zappa_settings.json`, and the code
is Python 3.7 era Flask + Zappa with no tests of the fetch path.

Replace it with a Gundi v2 action runner, `gundi-integration-spot`, so SPOT
becomes an ordinary provider in the Gundi portal: one connection per
customer, feeds configured in the portal, observations routed to EarthRanger
(or anywhere else) by Gundi.

## Goals

- A customer's SPOT trackers are set up entirely in the Gundi portal: a
  connection, its feed list, and a subject type. No per-site deployment.
- Behaviour parity with the legacy provider for the data that reaches
  EarthRanger: same source identifier, same subject name, same additional
  fields, same filtering of junk messages.
- Only new data is fetched each run; a fresh connection backfills a
  configurable window.
- Problems with one feed (wrong password, SPOT throttling, bad response)
  are visible in the portal activity log and do not stop the other feeds.

## Non-goals

- A SPOT *authenticate* action. SPOT share feeds have no account login;
  the only credential is the optional per-feed password, which belongs with
  the feed.
- Message types other than positions (e.g. HELP, SOS, custom text) as
  Gundi *events*. Every message with a valid position becomes an
  observation, with its message type in `additional`, exactly as today. An
  events action could be added later.
- A new EarthRanger core API for the subject subtype vocabulary. See
  "Subject type" below and "Follow-ups".
- Migrating existing sites' data or configuration automatically. Cutover
  is a manual per-site step (see "Cutover notes").

## Anti-goals

- Re-posting the same window every run and relying on the destination to
  dedupe, as the Lambda does (41 hours of data every 5 minutes). The new
  runner must track what it has already fetched.
- Carrying the legacy `source-timezone` option forward. SPOT returns Unix
  timestamps, which are UTC by definition; the option shifted observations
  by a zone offset and the legacy README already says never to use it.
- Hammering SPOT. SPOT states it blocks IPs or disables feeds for repeated
  rapid access to the same feed.

## Constraints

### SPOT public feed API (verified 2026-10-22 against findmespot.com support docs)

- Base URL:
  `https://api.findmespot.com/spot-main-web/consumer/rest-api/2.0/public/feed/{feed_id}/message.json`
- Query params: `feedPassword` (optional), `startDate` and `endDate` used
  together in the form `2012-07-03T00:00:00-0000` (only the `-0000` offset
  works), `start` for paging (`start=51`, `start=101`, ...).
- Responses hold at most 50 messages per page. `feedMessageResponse` carries
  `count`, `totalCount`, `activityCount`, a `feed` object and `messages.message`.
  **Note:** when there is exactly one message, the legacy code observed
  `message` arriving as a dict rather than a list; normalise both shapes.
- The feed only ever serves the last **7 days** of data, regardless of the
  dates requested. The initial lookback window therefore caps at 7 days.
- Rate limits, from SPOT's own guidance: at least 2.5 minutes between calls
  to the same feed, at least 2 seconds between calls to different feeds.
- Message fields seen in real responses (legacy test fixture
  `das_spot/tests/test_data.json`, 51 messages): `id`, `messengerId`,
  `messengerName`, `unixTime`, `messageType`, `latitude`, `longitude`,
  `modelId`, `showCustomMsg`, `dateTime`, `batteryState`, `hidden`,
  `altitude`, plus `@clientUnixTime`. `messageContent` appears on custom
  messages. `batteryState` and `altitude` are not in SPOT's published docs
  but are present in real responses; treat them as optional.
- Error shape: when the feed returns no data or rejects the request, the
  JSON is `{"response": {"errors": {"error": {...}}}}` with no
  `feedMessageResponse`. The legacy code only checks for the absence of
  `feedMessageResponse`. The builder should capture the real error payloads
  for "no messages in range", "bad password" and "unknown feed" during
  development and classify them; the fixture does not contain them.
- Decommissioned devices report dates around year 1000. The legacy filter
  drops messages with year <= 2010 or coordinates out of range; keep it.

### Gundi action runner

- Fork of `PADAS/gundi-integration-action-runner` (template repo), named
  `PADAS/gundi-integration-spot`, following the `er-developer:gundi-action-runner`
  skill: customise only `app/actions/handlers.py`,
  `app/actions/configurations.py`, a client module, and tests; leave the
  rest mergeable with upstream.
- Per-environment Terragrunt config in `PADAS/gundi-integrations-v2-infra`
  via `./create_integration.py spot`, default deployment inputs.
- Reference data for the subject type field uses the `gundi:reference`
  ui_schema annotation with `target: "destination"`, which the portal
  supports as of its 2026-10-08 design (`gundi-portal/docs/superpowers/specs/
  2026-10-08-reference-destination-target-design.md`). The
  `gundi-integration-africam` and `gundi-integration-inaturalist` configs
  show the runner-side idiom.

## Decisions made in the brainstorm (the user's)

1. **Feeds live in Gundi configuration**, not in an EarthRanger source group.
2. **One connection per customer, holding a list of feeds.**
3. **Subject type is configurable per connection**, chosen from a drop-down
   fed by the EarthRanger runner's existing `list_subject_types` reference
   action on the connection's destination. The full ER subject-subtype
   vocabulary is not exposed by ER's API today; retrofit when it is.
4. **Drop the timezone option.**

## Design

### Portal configuration (what a user sees)

One pull action, `pull_observations`, titled "Pull Observations", with this
configuration model:

| Field | Type | Portal label / help | Default |
|---|---|---|---|
| `feeds` | list of `SpotFeed` | "SPOT feeds" — one row per share page | required, at least one |
| `SpotFeed.feed_id` | `str` | "Feed ID" — the ID from the share page URL | required |
| `SpotFeed.feed_password` | `pydantic.SecretStr`, optional | "Feed password" — only if the share page is password protected | empty |
| `SpotFeed.name` | `str`, optional | "Name" — a label for the activity log; purely cosmetic | empty |
| `subject_type` | `str` | "Subject type" — the EarthRanger subject subtype for these trackers | `security_vehicle` |
| `initial_lookback_hours` | `int`, 1..168 | "Initial lookback (hours)" — how far back to fetch the first time a feed is seen; SPOT keeps 7 days at most | 48 |

- `subject_type` carries `gundi:reference` →
  `{"action": "list_subject_types", "target": "destination", "params": {},
  "allow_free_text": true}` set in the config class's `ui_schema` override.
  No `ui:widget`, so portals without reference support, draft connections,
  and sites with no subjects yet all degrade to a text box with the default.
- Field order: `feeds`, `subject_type`, `initial_lookback_hours`.
- Feeds are deduplicated by `feed_id` at run time (the legacy code found
  customers listing the same feed many times); a duplicate row in config
  is not an error. If duplicates carry different passwords, the first row
  wins.
- `feed_password` is nested inside the `feeds` array, and the template's
  `UISchemaModelMixin` does not recurse into array items, so the config
  class's `ui_schema` override must set
  `feeds.items.feed_password` → `{"ui:widget": "password"}` by hand, next
  to the `subject_type` reference annotation.

No auth action. No webhook.

### Pull action behaviour

Scheduled every 5 minutes (`@crontab_schedule("*/5 * * * *")`), which
satisfies SPOT's 2.5-minute-per-feed rule with margin. Decorated with
`@activity_logger()`.

For each distinct feed, in order:

1. Read this feed's state: `IntegrationStateManager.get_state(integration_id,
   "pull_observations", source_id=feed_id)` → `{"last_unix_time": int}`.
2. Compute the window. `start` is `last_unix_time + 1 s` if present, else
   `now - initial_lookback_hours`. `start` is clamped to `now - 7 days`.
   `end` is `now`.
3. Fetch all pages for that window from SPOT (client handles `start=`
   paging until a page returns fewer than 50 or `totalCount` is reached).
4. Drop invalid messages (year <= 2010, lat/lon out of range, missing
   position) and log a count of dropped messages at INFO.
5. Transform the rest to Gundi observations (below) and send them in one
   `send_observations_to_gundi` call per feed. If there are none, send
   nothing.
6. Advance the feed's state to the maximum `unixTime` seen in this fetch
   (valid or not — a junk message with a 1000 AD date never advances it
   because it is below the current value; use `max(current, seen)`).
   State is only written after the send succeeds, so a failed send is
   retried next run.
7. Sleep 2 seconds before the next feed.

**Feed-specific failures continue; systemic failures abort.** (Brainstorm
author's call, after the builder check.)

- A *feed-specific* error (`SpotFeedAuthError`, `SpotFeedNotFound`,
  `SpotBadResponse`, `SpotRateLimited`) is caught, logged with
  `log_action_activity` at ERROR including the feed ID (never the
  password) and the classified cause, and the loop continues to the next
  feed.
- A *systemic* error aborts the run and propagates: an `httpx` transport
  error that survives the retries below (SPOT or the network is down for
  every feed, so continuing only burns time), or any failure from
  `send_observations_to_gundi` (a Gundi problem, not a SPOT one; the
  template already retries it, classifies it as "Gundi API request
  failed", and PubSub redelivery handles the rest). State already written
  for earlier feeds is kept; the failed feed's state is not advanced, so it
  is re-fetched next run.
- The action returns a summary dict
  `{"feeds": n, "observations_sent": n, "feeds_failed": [feed_id, ...]}`.
  If *every* feed failed with a feed-specific error, the action raises
  `IntegrationConfigurationError` (error_type `configuration`, which the
  template treats as final, so PubSub does **not** redeliver) so the
  activity log shows the run as failed. The 5-minute schedule is the only
  thing that re-polls SPOT; a redelivered run must never re-poll inside
  SPOT's 2.5-minute window.

Retries: the handler retries a SPOT request on `httpx` transport errors
and 5xx with short exponential backoff (3 attempts, 1/2/4 s, 10 s
timeout), never on 4xx or on SPOT's own error envelope. Retry
orchestration lives in the handler, not the client, per the action-runner
policy.

Feed count: `feeds` is validated to at most **50** entries in the config
model (portal shows the error). On the happy path a run is well under a
minute for 50 feeds; the only slow case, SPOT unreachable, aborts after the
first feed's retries (~17 s) rather than repeating them per feed, so a run
never approaches the template's 9-minute execution cap. If a customer ever
needs more than 50 feeds, split them across connections; a deadline-aware
cursor loop is deliberately not built now.

Windowing: the legacy client sent `start_time`/`end_time`, which SPOT does
not document (it documents `startDate`/`endDate`), so the legacy window
filter likely never worked and the feed always returned the newest 50.
The new client sends `startDate`/`endDate`, **and** the handler filters
client-side on `unixTime > last_unix_time` regardless, so correctness does
not depend on SPOT honouring the dates.

Concurrency: guard against overlapping runs of the same integration with a
per-integration `asyncio.Lock` as `gundi-integration-spidertracks` does;
a run that finds the lock held returns `{"skipped": true}`.

### Observation mapping

| Gundi observation field | Value |
|---|---|
| `source` | `messengerId` (e.g. `0-2825446`) — same as the legacy `manufacturer_id` |
| `source_name` | `messengerName` |
| `type` | `tracking-device` |
| `subject_type` | `subject_type` from config |
| `recorded_at` | `unixTime` as an ISO-8601 UTC timestamp |
| `location.lat` / `location.lon` | `latitude` / `longitude` as floats |
| `additional.messengerId` | `messengerId` |
| `additional.messengerName` | `messengerName` |
| `additional.messageType` | `messageType` |
| `additional.modelId` | `modelId` |
| `additional.batteryState` | `batteryState` (omit if absent) |
| `additional.altitude` | `altitude` (omit if absent) |
| `additional.feedId` | the feed this came from (new; useful for support) |

Not carried over: the legacy `subject_name`, `subject_type=vehicle`,
`subject_subtype=security_vehicle`, `model_name`, `source_type` fields.
Gundi's EarthRanger dispatcher derives the subject from `source_name` and
`subject_type`; Gundi's `subject_type` is what EarthRanger matches as the
subject *subtype* first (hence the drop-down offers subtypes, with parent
types as fallback). The legacy default of `security_vehicle` is therefore
the right default for `subject_type`.

### SPOT client (`app/actions/client.py`)

A small async `httpx` client with a pure interface and no Gundi imports:

```python
async def fetch_messages(
    feed_id: str,
    start: datetime,
    end: datetime,
    *,
    feed_password: str | None = None,
    base_url: str = SPOT_API_BASE,
    client: httpx.AsyncClient | None = None,
) -> list[dict]
```

- Formats dates as `%Y-%m-%dT%H:%M:%S-0000`, sends `feedPassword` only when
  set, follows `start=` paging, normalises a single-message dict to a list,
  and returns the raw message dicts.
- Raises its own exceptions: `SpotFeedError` (base), `SpotFeedNotFound`,
  `SpotFeedAuthError` (bad or missing password), `SpotRateLimited`,
  `SpotBadResponse` (unparseable or unexpected JSON), each carrying SPOT's
  error code and text when present. HTTP transport errors propagate as
  `httpx` exceptions. The "no messages in range" error envelope is
  **not** an exception: it returns an empty list.
- No sleeping, no retries, no logging to Gundi inside the client.

The handler maps `SpotFeedAuthError` → `IntegrationAuthError`,
`SpotRateLimited` → `IntegrationRateLimitError`, `SpotBadResponse` →
`IntegrationBadResponseError`, and `httpx` transport errors →
`IntegrationConnectionError` (all from `app.services.errors`) when it logs a
feed failure, so the portal shows the human-first classification.

### Tests

- Client: paging across 3 pages, single-message dict normalisation, date
  formatting, password param presence, each error envelope → exception,
  "no messages" → empty list. Use `respx` or `httpx.MockTransport`.
- Transform: the legacy fixture (`das_spot/tests/test_data.json`, moved
  into the new repo) produces 51 observations with the mapped fields; junk
  rows (year 1000, lat 999) are dropped; optional fields omitted when
  absent.
- Handler: first run uses the lookback and writes state; second run uses
  `last_unix_time + 1`; lookback clamps to 7 days; a feed that raises is
  logged and the next feed still runs; state is not advanced when the send
  fails; all-feeds-failed raises; duplicate feed IDs fetch once.

### Repo and infra

- `PADAS/gundi-integration-spot`, created from the template repo on GitHub.
  Connector slug `spot`, display name "SPOT", portal description
  "SPOT satellite messengers (Globalstar), via findmespot.com share feeds".
  The slug is the product name, as with `inreach` and `spidertracks`;
  `findmespot` is only the website. (User's decision.)
- `requirements.in`: `httpx` is already in the template base; add
  `respx` to dev requirements for tests if not present.
- `gundi-integrations-v2-infra`: `./create_integration.py spot`, default
  inputs for dev/stage/prod (one small request per feed every 5 minutes is
  light; raise `limits` only if a customer turns up with hundreds of
  feeds). PR through that repo's normal flow.
- Register in Gundi dev with `python -m app.register` / `gundi-runner
  register`, schedule `pull_observations:*/5 * * * *`.

## Cutover notes (for the person migrating a site)

- For each legacy deployment, copy feed IDs and passwords out of the site's
  EarthRanger source group (`sourcegroup/{id}` → `feed_id`,
  `additional.feed_passwd`) into the new connection's feed list.
- If the site's `zappa_settings.json` sets `source-timezone`, its historical
  observations were shifted by that zone's offset. New observations will be
  correct UTC; expect an apparent jump at cutover.
- **Existing subjects.** The Lambda posted with provider key `spot-provider`
  (or the site's `source-provider` value); Gundi posts with the
  connection's provider key, so EarthRanger sees a *new source* for each
  `messengerId`. Whether that new source attaches to the existing subject
  or spawns a duplicate is decided by the site's ER Track configuration
  (`sensors/subject_name_change.py`, setting "new device": *Use existing*
  matches the subject by name, case-insensitively unless configured
  otherwise; *Create new* makes a fresh subject). Before cutover, check
  the site's "new device" setting is *Use existing* and that the names
  match, or plan to reassign sources in ER admin afterwards. The subject
  subtype chosen in the connection should match the existing subjects'
  subtype so the drop-down and the data agree. (Author's call: document,
  not automate.)
- Set `initial_lookback_hours` to cover the gap between stopping the Lambda
  and the first run; EarthRanger dedupes identical observations, so overlap
  is harmless.
- Undeploy the Lambda (`zappa undeploy <site>`) once the new connection has
  delivered a run.

## Left to the builder

- Whether to batch observations across feeds into one send or one send per
  feed (design says per feed for cleaner failure isolation; either is
  acceptable if the per-feed state rule holds).
- Capturing SPOT's real error envelopes during development and finalising
  the exception classification table in the client.
- Test file layout, following `gundi-integration-spidertracks/app/actions/tests/`.

## Alternatives considered

- **Library layout** (`gundi-runner new`, `gundi-action-runner` package).
  The template now ships as a PyPI package, but it is still a release
  candidate (0.1.0rc4) and no production fork uses that layout yet. Fork
  the template as every other integration does; migrate later with the
  fork migration guide when the library is stable.
- **One connection per feed.** Simpler models and per-feed error cards, but
  customers with twenty feeds would click through twenty connections.
  Rejected by the user.
- **Keep reading feeds from the EarthRanger source group.** Would couple a
  provider to one specific destination and keep secrets in ER; rejected by
  the user.

## Follow-ups (separate projects)

- EarthRanger core endpoint listing subject types and subtypes with display
  names, then update the ER runner's `list_subject_types` to use it. No
  change is needed in this integration when that lands; the annotation
  stays the same.
- SPOT HELP / SOS / custom messages as Gundi events.
