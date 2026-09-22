"""Pendo server-side Track Event utility.

Sends track events to the Pendo data API via HTTP POST.
Requires the PENDO_INTEGRATION_KEY environment variable to be set.
Failures are logged but never allowed to disrupt application flow.
"""

import os
import time
import logging
import requests

logger = logging.getLogger(__name__)

PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY")
PENDO_DATA_HOST = os.environ.get("PENDO_DATA_HOST", "data.pendo.io")


def track(event_name, properties=None, visitor_id="system", account_id="system"):
    """Send a track event to the Pendo data API.

    Args:
        event_name: The exact event name registered in Pendo.
        properties: Optional dict of event metadata properties.
        visitor_id: Unique user identifier. Defaults to "system" for
                    automated / non-user-initiated events.
        account_id: Unique account identifier. Defaults to "system".
    """
    if not PENDO_INTEGRATION_KEY:
        return

    try:
        payload = {
            "type": "track",
            "event": event_name,
            "visitorId": visitor_id,
            "accountId": account_id,
            "timestamp": int(time.time() * 1000),
            "properties": properties or {},
        }
        requests.post(
            f"https://{PENDO_DATA_HOST}/data/track",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "x-pendo-integration-key": PENDO_INTEGRATION_KEY,
            },
            timeout=5,
        )
    except Exception as e:
        logger.warning("Pendo track event '%s' failed: %s", event_name, e)
