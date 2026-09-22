"""Pendo server-side track event utility."""

import os
import time
import logging
import requests as _requests

logger = logging.getLogger(__name__)

PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY", "")
PENDO_DATA_HOST = os.environ.get("PENDO_DATA_HOST", "https://data.pendo.io")


def pendo_track(event_name, properties=None, visitor_id="system", account_id="system"):
    """Send a track event to Pendo's server-side Track API.

    Failures are logged but never raised so tracking cannot
    break application flow.
    """
    if not PENDO_INTEGRATION_KEY:
        return

    try:
        body = {
            "type": "track",
            "event": event_name,
            "visitorId": visitor_id,
            "accountId": account_id,
            "timestamp": int(time.time() * 1000),
        }
        if properties:
            body["properties"] = properties

        _requests.post(
            f"{PENDO_DATA_HOST}/data/track",
            json=body,
            headers={
                "Content-Type": "application/json",
                "x-pendo-integration-key": PENDO_INTEGRATION_KEY,
            },
            timeout=5,
        )
    except Exception:
        logger.debug("Failed to send Pendo track event: %s", event_name, exc_info=True)
