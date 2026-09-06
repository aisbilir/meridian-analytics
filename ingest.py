#!/usr/bin/env python3
"""Meridian Air ingester.

Polls OpenSky for aircraft state vectors in a bounding box and writes them
to a partitioned bronze layout as newline-delimited JSON.

Config comes from environment variables so the same image runs unchanged
in every environment:

    OPENSKY_CLIENT_ID       (required)
    OPENSKY_CLIENT_SECRET   (required)
    MERIDIAN_OUT            output directory        (default ./bronze)
    MERIDIAN_BBOX           min_lat,max_lat,min_lon,max_lon
    MERIDIAN_INTERVAL       seconds between polls   (default 30)
    MERIDIAN_CREDITS        daily credit quota      (default 4000)
    MERIDIAN_ONCE           set to 1 to poll once and exit

Run locally:
    pip install "git+https://github.com/openskynetwork/opensky-api.git#subdirectory=python"
    export OPENSKY_CLIENT_ID=... OPENSKY_CLIENT_SECRET=...
    MERIDIAN_ONCE=1 MERIDIAN_OUT=./tmp python ingest.py
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from opensky_api import OpenSkyApi

FEED = "air"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,  # containers log to stdout, nothing else
)
log = logging.getLogger("meridian-air")

# Kubernetes sends SIGTERM before SIGKILL. Handling it means the pod exits
# cleanly on a rollout or a spot eviction instead of being killed mid-write.
_shutdown = False


def _handle_signal(signum, _frame):
    global _shutdown
    log.info("received signal %s, finishing current cycle", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def parse_bbox(raw):
    """OpenSky's Python client wants (min_lat, max_lat, min_lon, max_lon).

    Note this is NOT the order the REST API takes its query parameters in.
    Getting it wrong returns an empty result rather than an error, which is
    an annoying way to lose an afternoon.
    """
    parts = [float(p) for p in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox needs 4 values: min_lat,max_lat,min_lon,max_lon")
    min_lat, max_lat, min_lon, max_lon = parts
    if max_lat <= min_lat or max_lon <= min_lon:
        raise ValueError("max bounds must be greater than min bounds")
    return (min_lat, max_lat, min_lon, max_lon)


def credit_cost(bbox):
    """Cost of one /states/all call, priced on bounding box area in sq degrees."""
    min_lat, max_lat, min_lon, max_lon = bbox
    area = (max_lat - min_lat) * (max_lon - min_lon)
    if area <= 25:
        return 1
    if area <= 100:
        return 2
    if area <= 400:
        return 3
    return 4


def to_record(state, snapshot_time, ingested_at):
    """Flatten a StateVector into a dict. Bronze keeps everything as received."""
    return {
        "icao24": state.icao24,
        "callsign": (state.callsign or "").strip() or None,
        "origin_country": state.origin_country,
        "time_position": state.time_position,
        "last_contact": state.last_contact,
        "longitude": state.longitude,
        "latitude": state.latitude,
        "baro_altitude": state.baro_altitude,
        "geo_altitude": state.geo_altitude,
        "on_ground": state.on_ground,
        "velocity": state.velocity,
        "true_track": state.true_track,
        "vertical_rate": state.vertical_rate,
        "squawk": state.squawk,
        "spi": state.spi,
        "position_source": state.position_source,
        "category": state.category,
        "_feed": FEED,
        "_snapshot_time": snapshot_time,
        "_ingested_at": ingested_at,
    }


def write_bronze(out_dir, snapshot_time, records):
    """Write one snapshot to /{feed}/dt=YYYY-MM-DD/hr=HH/.

    The file is named for the snapshot timestamp and skipped if it already
    exists, so a restart or a re-run produces no duplicates. Bronze is
    partitioned by ingestion time, not event time.
    """
    if not records:
        return False

    ts = datetime.fromtimestamp(snapshot_time, tz=timezone.utc)
    part = Path(out_dir) / FEED / f"dt={ts:%Y-%m-%d}" / f"hr={ts:%H}"
    part.mkdir(parents=True, exist_ok=True)

    final = part / f"states-{snapshot_time}.ndjson"
    if final.exists():
        return False

    # Write to a temp file and rename, so a crash never leaves a partial
    # file that a downstream reader would treat as complete.
    tmp = final.with_suffix(".ndjson.tmp")
    with tmp.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    tmp.rename(final)
    return True


def main():
    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        log.error("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set")
        sys.exit(1)

    out_dir = os.environ.get("MERIDIAN_OUT", "./bronze")
    interval = int(os.environ.get("MERIDIAN_INTERVAL", "30"))
    daily_credits = int(os.environ.get("MERIDIAN_CREDITS", "4000"))
    once = os.environ.get("MERIDIAN_ONCE") == "1"
    bbox = parse_bbox(os.environ.get("MERIDIAN_BBOX", "41.0,43.5,-73.5,-70.0"))

    cost = credit_cost(bbox)
    calls_per_day = 86400 // interval
    log.info(
        "bbox=%s cost=%d credits/call interval=%ds -> ~%d calls/day, %d credits/day (quota %d)",
        bbox, cost, interval, calls_per_day, calls_per_day * cost, daily_credits,
    )
    if calls_per_day * cost > daily_credits:
        log.warning("this cadence will exhaust the daily quota - raise MERIDIAN_INTERVAL")

    spent = 0
    budget_day = datetime.now(timezone.utc).date()

    # The client handles the OAuth2 client-credentials flow and refreshes
    # the 30-minute token on its own.
    with OpenSkyApi(client_id=client_id, client_secret=client_secret) as api:
        while True:
            today = datetime.now(timezone.utc).date()
            if today != budget_day:
                budget_day, spent = today, 0
                log.info("credit budget reset for %s", today)

            if spent + cost > daily_credits:
                log.warning("daily credit budget exhausted (%d/%d), idling", spent, daily_credits)
            else:
                try:
                    states = api.get_states(bbox=bbox)
                    spent += cost

                    if states is None or not states.states:
                        log.warning("no states returned (request failed or box is empty)")
                    else:
                        ingested_at = datetime.now(timezone.utc).isoformat()
                        records = [
                            to_record(s, states.time, ingested_at) for s in states.states
                        ]
                        new = write_bronze(out_dir, states.time, records)
                        log.info(
                            "records=%d new_file=%s credits_used=%d/%d",
                            len(records), new, spent, daily_credits,
                        )
                except Exception as exc:  # keep the pod alive through transient errors
                    log.exception("poll failed: %s", exc)

            if once or _shutdown:
                break

            # Sleep in short slices so SIGTERM is honoured promptly.
            for _ in range(interval):
                if _shutdown:
                    break
                time.sleep(1)
            if _shutdown:
                break

    log.info("shutdown complete, %d credits used today", spent)


if __name__ == "__main__":
    main()