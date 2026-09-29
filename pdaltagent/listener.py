#!/usr/bin/env python3

from pdaltagent.tasks import send_to_pd
from pdaltagent.scrubber import scrub
import pdaltagent.pd as pd
import pdaltagent.pendo_track as pendo_track
import os
import json

from flask import Flask, request
app = Flask(__name__)

SCRUB = True if os.environ.get("PDAGENTD_SCRUB_PII") and os.environ.get("PDAGENTD_SCRUB_PII").lower != 'false' else False

@app.route('/integration/<routing_key>/enqueue', methods=['POST'])
def enqueue_integration(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="v1")

	# Pendo Track Event: event_ingested_v1
	try:
		pendo_track.track("event_ingested_v1", {
			"destination_type": "v1",
			"pii_scrubbed": str(SCRUB),
			"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		})
	except Exception:
		pass

	return "Message enqueued\n"

@app.route('/x-ere/<routing_key>', methods=['POST'])
def enqueue_x_ere(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="x-ere")

	# Pendo Track Event: event_ingested_routing
	try:
		pendo_track.track("event_ingested_routing", {
			"destination_type": "x-ere",
			"pii_scrubbed": str(SCRUB),
			"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
		})
	except Exception:
		pass

	return "Message enqueued\n"

@app.route('/v2/enqueue', methods=['POST'])
def enqueue_v2():
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	if not pd.is_valid_v2_payload(body):
		return "Invalid PD events v2 payload\n", 400

	try:
		routing_key = body['routing_key']
	except:
		return "No routing key found in payload\n", 400

	if not pd.is_valid_integration_key(routing_key):
		return "Invalid routing key found in payload\n", 400

	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))

	send_to_pd.delay(routing_key, body, destination_type="v2")

	# Pendo Track Event: event_ingested_v2
	try:
		pendo_track.track("event_ingested_v2", {
			"event_action": body.get("event_action", ""),
			"severity": body.get("payload", {}).get("severity", ""),
			"routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
			"pii_scrubbed": str(SCRUB),
			"has_dedup_key": str(bool(body.get("dedup_key"))),
			"payload_source": body.get("payload", {}).get("source", ""),
		})
	except Exception:
		pass

	return "Message enqueued\n"
