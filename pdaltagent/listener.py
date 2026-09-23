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

	# Pendo Track Event: v1 event ingested and enqueued
	try:
		pendo_track.track("event_ingested_v1", properties={
			"pii_scrubbed": str(SCRUB),
			"destination_type": "v1",
			"payload_size": len(json.dumps(body)),
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

	# Pendo Track Event: routing (x-ere) event ingested and enqueued
	try:
		pendo_track.track("event_ingested_routing", properties={
			"pii_scrubbed": str(SCRUB),
			"destination_type": "x-ere",
			"payload_size": len(json.dumps(body)),
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

	# Pendo Track Event: v2 event ingested and enqueued
	try:
		rk_type = "rules_engine" if routing_key.startswith("R") else "classic"
		pendo_track.track("event_ingested_v2", properties={
			"routing_key_type": rk_type,
			"event_action": body.get("event_action", ""),
			"severity": (body.get("payload") or {}).get("severity", ""),
			"pii_scrubbed": str(SCRUB),
			"destination_type": "v2",
			"payload_size": len(json.dumps(body)),
		})
	except Exception:
		pass

	return "Message enqueued\n"
