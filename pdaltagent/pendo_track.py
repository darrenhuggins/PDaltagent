"""
Pendo server-side Track Event utility.

Sends track events to the Pendo Track API via HTTP POST.

TODO: Set the PENDO_TRACK_EVENT_SECRET environment variable with your
Pendo integration key (x-pendo-integration-key) for server-side tracking.
TODO: If your Pendo data host differs from the default, set
PENDO_DATA_HOST (e.g. "https://data.eu.pendo.io").
"""

import os
import time
import logging
import requests as _requests

logger = logging.getLogger(__name__)

PENDO_TRACK_EVENT_SECRET = os.environ.get("PENDO_TRACK_EVENT_SECRET")
PENDO_DATA_HOST = os.environ.get("PENDO_DATA_HOST", "https://data.pendo.io")
PENDO_TRACK_URL = f"{PENDO_DATA_HOST}/data/track"


def track(event, visitor_id="system", account_id="system", properties=None):
    """
    Send a server-side track event to Pendo.

    Args:
        event: The event name (string).
        visitor_id: Unique user identifier. Defaults to "system" for
                    automated/system events with no user context.
        account_id: Unique account identifier. Defaults to "system".
        properties: Optional dict of event properties.
    """
    if not PENDO_TRACK_EVENT_SECRET:
        logger.warning(
            "Pendo track event not sent (PENDO_TRACK_EVENT_SECRET is not "
            "set). Event: %s", event
        )
        return None

    body = {
        "type": "track",
        "event": event,
        "visitorId": visitor_id,
        "accountId": account_id,
        "timestamp": int(time.time() * 1000),
    }
    if properties:
        body["properties"] = properties

    try:
        response = _requests.post(
            PENDO_TRACK_URL,
            json=body,
            headers={
                "Content-Type": "application/json",
                "x-pendo-integration-key": PENDO_TRACK_EVENT_SECRET,
            },
            timeout=5,
        )
        response.raise_for_status()
        return response
    except Exception as exc:
        logger.error("Failed to send Pendo track event '%s': %s", event, exc)
        return None
