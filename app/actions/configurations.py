from typing import List, Optional

from pydantic import BaseModel, Field, SecretStr

from app.actions.core import PullActionConfiguration
from app.services.utils import GlobalUISchemaOptions


class SpotFeed(BaseModel):
    """One SPOT share page. Portal users add one row per feed."""

    feed_id: str = Field(
        ...,
        title="Feed ID",
        description="The feed ID from the SPOT share page URL.",
    )
    feed_password: Optional[SecretStr] = Field(
        None,
        title="Feed password",
        description="Only needed if the share page is password protected.",
        format="password",
    )
    name: Optional[str] = Field(
        None,
        title="Name",
        description="Optional label shown in the activity log.",
    )


class PullObservationsConfig(PullActionConfiguration):
    """Configuration for pulling positions from a customer's SPOT feeds."""

    feeds: List[SpotFeed] = Field(
        ...,
        min_items=1,
        max_items=50,
        title="SPOT feeds",
        description="One row per SPOT share page (up to 50).",
    )
    subject_type: str = Field(
        "security_vehicle",
        title="Subject type",
        description=(
            "EarthRanger subject subtype for these trackers. "
            "Pick from the connected site's list, or type one."
        ),
    )
    initial_lookback_hours: int = Field(
        48,
        ge=1,
        le=168,
        title="Initial lookback (hours)",
        description="How far back to fetch the first time a feed is seen. SPOT keeps 7 days at most.",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=["feeds", "subject_type", "initial_lookback_hours", "run_on_schedule"],
    )

    @classmethod
    def ui_schema(cls, *args, **kwargs):
        """Annotate `subject_type` with gundi:reference so the portal offers the
        connected EarthRanger site's subject subtypes (served by the ER runner's
        `list_subject_types` reference action on this connection's destination;
        spec: gundi-portal docs/superpowers/specs/2026-10-08-reference-destination-target-design.md).
        Deliberately no ui:widget: portals without reference support, draft
        connections and sites with no subjects keep a plain text field.

        `feed_password` is nested inside the `feeds` array and the template's
        UISchemaModelMixin does not recurse into array items, so its password
        widget is set here by hand.
        """
        base = super().ui_schema(*args, **kwargs)
        base["subject_type"] = {"gundi:reference": _reference("list_subject_types", target="destination")}
        base["feeds"] = {"items": {"feed_password": {"ui:widget": "password"}}}
        return base


def _reference(action: str, params: Optional[dict] = None, *, target: str = "self") -> dict:
    """Build a gundi:reference ui_schema annotation (same helper as
    gundi-integration-africam; spec: gundi-integration-cmore
    docs/superpowers/specs/2026-07-31-reference-data-config-ui-design.md §2)."""
    return {
        "action": action,
        "target": target,
        "params": params or {},
        "allow_free_text": True,
    }


def dedupe_feeds(feeds: List[SpotFeed]) -> List[SpotFeed]:
    """Drop repeated feed IDs, keeping the first row (customers often list one feed many times)."""
    seen = set()
    unique = []
    for feed in feeds:
        if feed.feed_id in seen:
            continue
        seen.add(feed.feed_id)
        unique.append(feed)
    return unique
