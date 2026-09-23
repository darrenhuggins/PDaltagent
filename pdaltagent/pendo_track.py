"""
Pendo server-side Track Events utility.

Sends track events to Pendo's /data/track API endpoint.

TODO: Set the PENDO_INTEGRATION_KEY environment variable with your Pendo
trackEventSecret to enable server-side track events.
TODO: If your Pendo data host differs from the default, set PENDO_DATA_HOST
(e.g. https://data.eu.pendo.io).
"""

import os
import time
import logging

import requests as _requests

logger = logging.getLogger(__name__)

PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY")
PENDO_DATA_HOST = os.environ.get("PENDO_DATA_HOST", "https://data.pendo.io")


def track(event, visitor_id="system", account_id="system", properties=None):
    """Send a track event to Pendo.

    Args:
        event: Event name string (must match exactly what Pendo expects).
        visitor_id: Unique user identifier. Defaults to "system" for
            automated / non-user-initiated actions.
        account_id: Unique account identifier. Defaults to "system".
        properties: Optional dict of event metadata properties.
    """
    if not PENDO_INTEGRATION_KEY:
        logger.debug(
            "Pendo track event '%s' not sent: PENDO_INTEGRATION_KEY not set",
            event,
        )
        return

    payload = {
        "type": "track",
        "event": event,
        "visitorId": visitor_id,
        "accountId": account_id,
        "timestamp": int(time.time() * 1000),
    }
    if properties:
        payload["properties"] = properties

    try:
        _requests.post(
            f"{PENDO_DATA_HOST}/data/track",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "x-pendo-integration-key": PENDO_INTEGRATION_KEY,
            },
            timeout=5,
        )
    except Exception:
        logger.debug(
            "Failed to send Pendo track event '%s'", event, exc_info=True
        )
