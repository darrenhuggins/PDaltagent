import os
import json
import time
import datetime
import pdaltagent.pd as pd
import requests
import sqlite3
from requests import HTTPError
from pdaltagent.config import app
from celery.utils.log import get_task_logger
from celery import chain
from pdaltagent.pendo import pendo_track

PD_API_TOKEN = os.environ.get("PDAGENTD_API_TOKEN")
WEBHOOK_DEST_URL = os.environ.get("PDAGENTD_WEBHOOK_DEST_URL")
IS_OVERVIEW = 'false' if os.environ.get("PDAGENTD_GET_ALL_LOG_ENTRIES") and os.environ.get("PDAGENTD_GET_ALL_LOG_ENTRIES").lower != 'false' else 'true'

POLLING_INTERVAL_SECONDS = 10
if os.environ.get("PDAGENTD_POLLING_INTERVAL_SECONDS"):
    try:
        POLLING_INTERVAL_SECONDS = int(os.environ.get("PDAGENTD_POLLING_INTERVAL_SECONDS"))
    except:
        pass

# keep activity db rows for 30 days
KEEP_ACTIVITY_SECONDS = 30*24*60*60
if os.environ.get("PDAGENTD_KEEP_ACTIVITY_SECONDS"):
    try:
        KEEP_ACTIVITY_SECONDS = int(os.environ.get("PDAGENTD_KEEP_ACTIVITY_SECONDS"))
    except:
        pass

@app.on_after_finalize.connect
def check_activity_store(sender, **kwargs):
    conn = sqlite3.connect('/tmp/activity_store.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()
    c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='log_entries'")
    if c.fetchone()[0]==0:
        print('Activity store table does not exist. Creating...')
        c.executescript("""
            BEGIN TRANSACTION;
            CREATE TABLE log_entries (
                            id TEXT NOT NULL,
                            created_at timestamp);
            COMMIT;
        """)

    conn.commit()
    conn.close()

@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    if not PD_API_TOKEN:
        print(f"Can't get log entries because no token is set. Please set PDAGENTD_API_TOKEN environment variable if you want to poll PD log entries")
        return

    if not WEBHOOK_DEST_URL:
        print(f"Can't send webhooks because no destination URL is set. Please set PDAGENTD_WEBHOOK_DEST_URL environment variable if you want to send webhooks")
        return

    sender.add_periodic_task(float(POLLING_INTERVAL_SECONDS), poll_pd_log_entries.s())
    sender.add_periodic_task(float(KEEP_ACTIVITY_SECONDS), clean_activity_store.s())

class _SendToPdBase(app.Task):
    """Base task for send_to_pd that tracks delivery failures to Pendo."""
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        routing_key = args[0] if args else ""
        dest_type = (kwargs or {}).get("destination_type", "v2")
        try:
            pendo_track("event_delivery_failed", {
                "routing_key_type": "rules_engine" if routing_key and routing_key.startswith("R") else "classic",
                "destination_type": dest_type,
                "retry_count": self.request.retries,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:100],
            })
        except Exception:
            pass

@app.task(base=_SendToPdBase, bind=True,
          autoretry_for=(HTTPError,),
          retry_kwargs={'max_retries': 10},
          retry_backoff=15,
          retry_backoff_max=60*60*2,
          acks_late=True)
def send_to_pd(self, routing_key, payload, base_url="https://events.pagerduty.com", destination_type="v2"):
    result = pd.send_event(routing_key, payload, base_url, destination_type)
    # Track successful delivery to PagerDuty
    try:
        pendo_track("event_delivery_completed", {
            "routing_key_type": "rules_engine" if routing_key.startswith("R") else "classic",
            "destination_type": destination_type,
            "retry_count": self.request.retries,
            "dedup_key": str(payload.get("dedup_key", ""))[:50] if isinstance(payload, dict) else "",
        })
    except Exception:
        pass
    return (routing_key, result)

@app.task(bind=True, autoretry_for=(HTTPError,),
          retry_kwargs={'max_retries': 10},
          retry_backoff=15)
def send_webhook(self, url, payload):
    response = requests.post(url, json=payload)
    # Track webhook delivery completion
    try:
        incident_id = ""
        event_type = ""
        if isinstance(payload, dict) and "messages" in payload:
            msgs = payload.get("messages", [])
            if msgs:
                msg = msgs[0]
                incident_id = msg.get("incident", {}).get("id", "")
                event_type = msg.get("event", "")
        pendo_track("webhook_delivery_completed", {
            "incident_id": incident_id,
            "event_type": event_type,
            "retry_count": self.request.retries,
            "response_status": response.status_code,
        })
    except Exception:
        pass
    return (url, response)

@app.task()
def poll_pd_log_entries():
    conn = sqlite3.connect('/tmp/activity_store.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()
    r = c.execute('select * from log_entries order by created_at desc limit 1')
    a = r.fetchone()
    now = datetime.datetime.utcnow()
    last_poll = a[1] if a else (now - datetime.timedelta(seconds=POLLING_INTERVAL_SECONDS))

    since = last_poll.replace(microsecond=0).isoformat()
    until = now.replace(microsecond=0).isoformat()

    params = {'since': since, 'until': until, 'is_overview': IS_OVERVIEW}
    iles = pd.fetch_log_entries(token=PD_API_TOKEN, params=params)
    iles.reverse()
    new_iles = []
    dups = 0
    ile_chains = {}
    for ile in iles:
        ile_id = ile['id']
        r = c.execute("select count(*) from log_entries where id = ?", (ile_id,))
        if r.fetchone()[0]:
            dups += 1
            continue

        incident_id = ile['incident']['id']
        if not ile_chains.get(incident_id):
            ile_chains[incident_id] = []

        sig = send_webhook.si(WEBHOOK_DEST_URL, pd.ile_to_webhook(ile))
        ile_chains[incident_id].append(sig)
        created_at = datetime.datetime.fromisoformat(ile['created_at'].rstrip('Z'))
        new_iles.append((ile_id, created_at))

    c.executemany('insert into log_entries values (?, ?)', new_iles)
    conn.commit()
    c.close()
    conn.close()
    for incident_id, ile_chain in ile_chains.items():
        chain(ile_chain).delay()
    # Track poll completion
    try:
        pendo_track("log_entries_poll_completed", {
            "entries_fetched": len(iles),
            "entries_processed": len(new_iles),
            "duplicates_skipped": dups,
            "incidents_count": len(ile_chains),
            "polling_interval_sec": POLLING_INTERVAL_SECONDS,
            "since_timestamp": since,
            "until_timestamp": until,
            "is_overview_mode": IS_OVERVIEW == 'true',
        })
    except Exception:
        pass
    return f"{len(iles)} fetched, {len(new_iles)} processed, {dups} duplicates (since {since})"

@app.task()
def clean_activity_store():
    start_time = time.time()
    conn = sqlite3.connect('/tmp/activity_store.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()
    d = datetime.datetime.utcnow() - datetime.timedelta(seconds=KEEP_ACTIVITY_SECONDS)
    c.execute("delete from log_entries where created_at < ?", (d,))
    conn.commit()
    r = c.rowcount
    c.close()
    conn.close()
    # Track cleanup completion
    duration_ms = int((time.time() - start_time) * 1000)
    try:
        pendo_track("activity_store_cleanup_completed", {
            "rows_deleted": r,
            "retention_period_sec": KEEP_ACTIVITY_SECONDS,
            "cleanup_duration_ms": duration_ms,
        })
    except Exception:
        pass
    return f"{r} rows deleted"

def consume():
    app.worker_main(['worker', '-n', 'events', '-A', 'pdaltagent.tasks', '-Q', 'pd_events', '-E', '-l', 'info'])

def poll():
    app.worker_main(['worker', '-n', 'poller', '-A', 'pdaltagent.tasks', '-B', '-Q', 'pd_poller,pd_webhooks', '-E', '-l', 'info'])