#!/usr/bin/env python3

from pdaltagent.tasks import send_to_pd
from pdaltagent.scrubber import scrub
from pdaltagent.pendo_track import track as pendo_track
import pdaltagent.pd as pd
import os
import json
import sys

from flask import Flask, request
app = Flask(__name__)

SCRUB = True if os.environ.get("PDAGENTD_SCRUB_PII") and os.environ.get("PDAGENTD_SCRUB_PII").lower != 'false' else False

@app.route('/integration/<routing_key>/enqueue', methods=['POST'])
def enqueue_integration(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	scrub_applied = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		scrub_applied = True

	send_to_pd.delay(routing_key, body, destination_type="v1")

	# Pendo Track: v1 (classic integration) event ingested
	pendo_track("event_ingested_v1", {
		"routing_key": "***" + routing_key[-4:] if len(routing_key) >= 4 else "***",
		"destination_type": "v1",
		"scrub_applied": scrub_applied,
		"payload_size": sys.getsizeof(json.dumps(body)),
	})

	return "Message enqueued\n"

@app.route('/x-ere/<routing_key>', methods=['POST'])
def enqueue_x_ere(routing_key):
	body = request.get_json(force=True)
	if not body:
		return "Bad request\n", 400

	scrub_applied = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		scrub_applied = True

	send_to_pd.delay(routing_key, body, destination_type="x-ere")

	# Pendo Track: routing (Global Event Routing) event ingested
	pendo_track("event_ingested_routing", {
		"routing_key": "***" + routing_key[-4:] if len(routing_key) >= 4 else "***",
		"destination_type": "x-ere",
		"scrub_applied": scrub_applied,
		"payload_size": sys.getsizeof(json.dumps(body)),
	})

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

	scrub_applied = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		scrub_applied = True

	send_to_pd.delay(routing_key, body, destination_type="v2")

	# Pendo Track: v2 (Events API v2) event ingested
	pendo_track("event_ingested_v2", {
		"routing_key": "***" + routing_key[-4:] if len(routing_key) >= 4 else "***",
		"destination_type": "v2",
		"event_action": body.get("event_action", ""),
		"severity": (body.get("payload") or {}).get("severity", ""),
		"scrub_applied": scrub_applied,
		"payload_size": sys.getsizeof(json.dumps(body)),
	})

	return "Message enqueued\n"
