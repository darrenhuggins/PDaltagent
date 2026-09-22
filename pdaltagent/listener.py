#!/usr/bin/env python3

from pdaltagent.tasks import send_to_pd
from pdaltagent.scrubber import scrub
from pdaltagent.pendo_tracking import pendo_track
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
			"rejection_reason": "Bad request",
			"endpoint_path": "/integration/<key>/enqueue",
			"http_status_code": "400",
		})
		return "Bad request\n", 400

	pii_scrubbed = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		pii_scrubbed = True
		pendo_track("pii_scrub_applied", {
			"endpoint_path": "/integration/<key>/enqueue",
			"destination_type": "v1",
		})

	send_to_pd.delay(routing_key, body, destination_type="v1")
	pendo_track("event_enqueued", {
		"destination_type": "v1",
		"endpoint_path": "/integration/<key>/enqueue",
		"pii_scrubbed": pii_scrubbed,
	})
	return "Message enqueued\n"

@app.route('/x-ere/<routing_key>', methods=['POST'])
def enqueue_x_ere(routing_key):
	body = request.get_json(force=True)
	if not body:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "Bad request",
			"endpoint_path": "/x-ere/<key>",
			"http_status_code": "400",
		})
		return "Bad request\n", 400

	pii_scrubbed = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		pii_scrubbed = True
		pendo_track("pii_scrub_applied", {
			"endpoint_path": "/x-ere/<key>",
			"destination_type": "x-ere",
		})

	send_to_pd.delay(routing_key, body, destination_type="x-ere")
	pendo_track("event_enqueued", {
		"destination_type": "x-ere",
		"endpoint_path": "/x-ere/<key>",
		"pii_scrubbed": pii_scrubbed,
	})
	return "Message enqueued\n"

@app.route('/v2/enqueue', methods=['POST'])
def enqueue_v2():
	body = request.get_json(force=True)
	if not body:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "Bad request",
			"endpoint_path": "/v2/enqueue",
			"http_status_code": "400",
		})
		return "Bad request\n", 400

	if not pd.is_valid_v2_payload(body):
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "Invalid v2 payload",
			"endpoint_path": "/v2/enqueue",
			"http_status_code": "400",
		})
		return "Invalid PD events v2 payload\n", 400

	try:
		routing_key = body['routing_key']
	except:
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "No routing key found",
			"endpoint_path": "/v2/enqueue",
			"http_status_code": "400",
		})
		return "No routing key found in payload\n", 400

	if not pd.is_valid_integration_key(routing_key):
		pendo_track("event_ingestion_rejected", {
			"rejection_reason": "Invalid routing key",
			"endpoint_path": "/v2/enqueue",
			"http_status_code": "400",
		})
		return "Invalid routing key found in payload\n", 400

	pii_scrubbed = False
	if SCRUB:
		body = json.loads(scrub(json.dumps(body)))
		pii_scrubbed = True
		pendo_track("pii_scrub_applied", {
			"endpoint_path": "/v2/enqueue",
			"destination_type": "v2",
		})

	send_to_pd.delay(routing_key, body, destination_type="v2")
	pendo_track("event_enqueued", {
		"destination_type": "v2",
		"endpoint_path": "/v2/enqueue",
		"pii_scrubbed": pii_scrubbed,
	})
	return "Message enqueued\n"
