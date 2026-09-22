#!/usr/bin/env python3

from pdaltagent.tasks import send_to_pd
from pdaltagent.scrubber import scrub
from pdaltagent.pendo import pendo_track
import pdaltagent.pd as pd
import os
import json

from flask import Flask, request
app = Flask(__name__)

SCRUB = True if os.environ.get("PDAGENTD_SCRUB_PII") and os.environ.get("PDAGENTD_SCRUB_PII").lower != 'false' else False

@app.route('/integration/<routing_key>/enqueue', methods=['POST'])
def enqueue_integration(routing_key):
	body = request.get_json(force=True)
	if not body:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "empty_body",
			"endpoint_path": "/integration/enqueue",
			"destination_type": "v1",
			"has_routing_key": True,
			"response_status_code": 400,
		})
		return "Bad request\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="v1")
	pendo_track("event_ingested", {
		"destination_type": "v1",
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"scrub_enabled": SCRUB,
		"payload_size_bytes": request.content_length or 0,
	})
	return "Message enqueued\n"

@app.route('/x-ere/<routing_key>', methods=['POST'])
def enqueue_x_ere(routing_key):
	body = request.get_json(force=True)
	if not body:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "empty_body",
			"endpoint_path": "/x-ere",
			"destination_type": "x-ere",
			"has_routing_key": True,
			"response_status_code": 400,
		})
		return "Bad request\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="x-ere")
	pendo_track("event_ingested", {
		"destination_type": "x-ere",
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"scrub_enabled": SCRUB,
		"payload_size_bytes": request.content_length or 0,
	})
	return "Message enqueued\n"

@app.route('/v2/enqueue', methods=['POST'])
def enqueue_v2():
	body = request.get_json(force=True)
	if not body:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "empty_body",
			"endpoint_path": "/v2/enqueue",
			"destination_type": "v2",
			"has_routing_key": False,
			"response_status_code": 400,
		})
		return "Bad request\n", 400

	if not pd.is_valid_v2_payload(body):
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "invalid_v2_payload",
			"endpoint_path": "/v2/enqueue",
			"destination_type": "v2",
			"has_routing_key": "routing_key" in body,
			"response_status_code": 400,
		})
		return "Invalid PD events v2 payload\n", 400

	try:
		routing_key = body['routing_key']
	except:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "missing_routing_key",
			"endpoint_path": "/v2/enqueue",
			"destination_type": "v2",
			"has_routing_key": False,
			"response_status_code": 400,
		})
		return "No routing key found in payload\n", 400

	if not pd.is_valid_integration_key(routing_key):
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "invalid_routing_key",
			"endpoint_path": "/v2/enqueue",
			"destination_type": "v2",
			"has_routing_key": True,
			"response_status_code": 400,
		})
		return "Invalid routing key found in payload\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="v2")
	pendo_track("event_ingested", {
		"destination_type": "v2",
		"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		"event_action": body.get("event_action", ""),
		"severity": body.get("payload", {}).get("severity", ""),
		"scrub_enabled": SCRUB,
		"payload_size_bytes": request.content_length or 0,
	})
	return "Message enqueued\n"
