import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def spot_feed_payload() -> dict:
    """A real SPOT feed response (51 messages) captured by the legacy provider."""
    with open(FIXTURES / "spot_feed_response.json") as f:
        return json.load(f)


@pytest.fixture
def spot_messages(spot_feed_payload) -> list:
    return spot_feed_payload["response"]["feedMessageResponse"]["messages"]["message"]
