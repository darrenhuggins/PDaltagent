import os
import time
import logging
import requests

logger = logging.getLogger(__name__)

PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY")
PENDO_TRACK_URL = "https://{}/data/track".format(
    os.environ.get("PENDO_DATA_HOST", "data.pendo.io")
)


def pendo_track(event_name, properties=None, visitor_id="system", account_id="system"):
    """Send a server-side track event to Pendo.

    Silently no-ops when the integration key is not configured.
    Never raises; tracking failures must not break application flow.
    """
    if not PENDO_INTEGRATION_KEY:
        return

    body = {
        "type": "track",
        "event": event_name,
        "visitorId": visitor_id,
        "accountId": account_id,
        "timestamp": int(time.time() * 1000),
    }
    if properties:
        body["properties"] = properties

    try:
        requests.post(
            PENDO_TRACK_URL,
            json=body,
            headers={
                "Content-Type": "application/json",
                "x-pendo-integration-key": PENDO_INTEGRATION_KEY,
            },
            timeout=5,
        )
    except Exception:
        logger.debug("Failed to send Pendo track event: %s", event_name)
