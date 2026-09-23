#!/usr/bin/env python3

from pdaltagent.tasks import send_to_pd
from pdaltagent.scrubber import scrub
import pdaltagent.pd as pd
import pdaltagent.pendo_track as pendo
import os
import json
import re

from flask import Flask, request
app = Flask(__name__)

SCRUB = True if os.environ.get("PDAGENTD_SCRUB_PII") and os.environ.get("PDAGENTD_SCRUB_PII").lower != 'false' else False

# Pattern to detect PII placeholder types injected by the scrubber
_PII_PLACEHOLDER_RE = re.compile(r'\{\{([A-Z_]+)\}\}')


def _track_pii_scrub(original_str, scrubbed_str, endpoint_path, destination_type):
	"""Track a pii_scrub_applied event if PII was found and replaced."""
	if scrubbed_str == original_str:
		return
	pii_types = sorted(set(_PII_PLACEHOLDER_RE.findall(scrubbed_str)))
	# Pendo Track Event: pii_scrub_applied
	pendo.track("pii_scrub_applied", properties={
		"endpoint_path": endpoint_path,
		"destination_type": destination_type,
		"pii_types_found": ",".join(pii_types),
		"original_payload_len": len(original_str),
		"scrubbed_payload_len": len(scrubbed_str),
	})


@app.route('/integration/<routing_key>/enqueue', methods=['POST'])
def enqueue_integration(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	pii_scrubbed = False
	if SCRUB:
		original = json.dumps(body)
		scrubbed = scrub(original)
		body = json.loads(scrubbed)
		if scrubbed != original:
			pii_scrubbed = True
		_track_pii_scrub(original, scrubbed, "/integration/<key>/enqueue", "v1")

	send_to_pd.delay(routing_key, body, destination_type="v1")

	# Pendo Track Event: event_enqueued
	pendo.track("event_enqueued", properties={
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"destination_type": "v1",
		"pii_scrubbed": pii_scrubbed,
		"payload_size_bytes": len(json.dumps(body)),
		"endpoint_path": "/integration/<key>/enqueue",
	})

	return "Message enqueued\n"

@app.route('/x-ere/<routing_key>', methods=['POST'])
def enqueue_x_ere(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	pii_scrubbed = False
	if SCRUB:
		original = json.dumps(body)
		scrubbed = scrub(original)
		body = json.loads(scrubbed)
		if scrubbed != original:
			pii_scrubbed = True
		_track_pii_scrub(original, scrubbed, "/x-ere/<key>", "x-ere")

	send_to_pd.delay(routing_key, body, destination_type="x-ere")

	# Pendo Track Event: event_enqueued
	pendo.track("event_enqueued", properties={
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"destination_type": "x-ere",
		"pii_scrubbed": pii_scrubbed,
		"payload_size_bytes": len(json.dumps(body)),
		"endpoint_path": "/x-ere/<key>",
	})

	return "Message enqueued\n"

@app.route('/v2/enqueue', methods=['POST'])
def enqueue_v2():
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	if not pd.is_valid_v2_payload(body):
		# Pendo Track Event: event_ingestion_validation_failed
		pendo.track("event_ingestion_validation_failed", properties={
			"failure_reason": "invalid_v2_payload",
			"endpoint_path": "/v2/enqueue",
			"has_routing_key": "routing_key" in body,
			"has_event_action": "event_action" in body,
		})
		return "Invalid PD events v2 payload\n", 400

	try:
		routing_key = body['routing_key']
	except:
		# Pendo Track Event: event_ingestion_validation_failed
		pendo.track("event_ingestion_validation_failed", properties={
			"failure_reason": "missing_routing_key",
			"endpoint_path": "/v2/enqueue",
			"has_routing_key": False,
			"has_event_action": "event_action" in body,
		})
		return "No routing key found in payload\n", 400

	if not pd.is_valid_integration_key(routing_key):
		# Pendo Track Event: event_ingestion_validation_failed
		pendo.track("event_ingestion_validation_failed", properties={
			"failure_reason": "invalid_routing_key",
			"endpoint_path": "/v2/enqueue",
			"has_routing_key": True,
			"has_event_action": "event_action" in body,
		})
		return "Invalid routing key found in payload\n", 400

	pii_scrubbed = False
	if SCRUB:
		original = json.dumps(body)
		scrubbed = scrub(original)
		body = json.loads(scrubbed)
		if scrubbed != original:
			pii_scrubbed = True
		_track_pii_scrub(original, scrubbed, "/v2/enqueue", "v2")

	send_to_pd.delay(routing_key, body, destination_type="v2")

	# Pendo Track Event: event_enqueued
	pendo.track("event_enqueued", properties={
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"destination_type": "v2",
		"pii_scrubbed": pii_scrubbed,
		"payload_size_bytes": len(json.dumps(body)),
		"endpoint_path": "/v2/enqueue",
	})

	return "Message enqueued\n"
