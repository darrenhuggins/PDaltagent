"""
Pendo server-side Track Event helper.

Sends track events to Pendo's /data/track endpoint.

TODO: Set the PENDO_INTEGRATION_KEY environment variable to your Pendo
      server-side integration key (x-pendo-integration-key) before
      server-side track events will be delivered.
TODO: If your Pendo data host differs from the default (data.pendo.io),
      set the PENDO_DATA_HOST environment variable accordingly.
"""

import os
import time
import requests as _requests
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

PENDO_INTEGRATION_KEY = os.environ.get("PENDO_INTEGRATION_KEY")
PENDO_DATA_HOST = os.environ.get("PENDO_DATA_HOST", "data.pendo.io")
PENDO_TRACK_URL = f"https://{PENDO_DATA_HOST}/data/track"


def track(event_name, properties=None, visitor_id="system", account_id="system"):
	"""Send a server-side track event to Pendo.

	Args:
		event_name: The event name (must match the registered track type exactly).
		properties: Optional dict of event properties.
		visitor_id: Unique user identifier. Defaults to "system" for automated tasks.
		account_id: Unique account identifier. Defaults to "system" for automated tasks.
	"""
	if not PENDO_INTEGRATION_KEY:
		# TODO: PENDO_INTEGRATION_KEY is required for server-side tracking.
		# Set it as an environment variable to enable Pendo track events.
		logger.debug(
			"Pendo track event '%s' skipped: PENDO_INTEGRATION_KEY not set",
			event_name,
		)
		return

	payload = {
		"type": "track",
		"event": event_name,
		"visitorId": visitor_id,
		"accountId": account_id,
		"timestamp": int(time.time() * 1000),
	}
	if properties:
		payload["properties"] = properties

	headers = {
		"Content-Type": "application/json",
		"x-pendo-integration-key": PENDO_INTEGRATION_KEY,
	}

	try:
		_requests.post(PENDO_TRACK_URL, json=payload, headers=headers, timeout=5)
	except Exception:
		logger.debug(
			"Failed to send Pendo track event '%s'", event_name, exc_info=True
		)
