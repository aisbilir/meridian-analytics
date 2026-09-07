#!/usr/bin/env python3
"""Meridian Air ingester.

Polls OpenSky for aircraft state vectors in a bounding box and writes them
to PostgreSQL.

Config comes from environment variables so the same image runs unchanged
in every environment:

    OPENSKY_CLIENT_ID       (required)
    OPENSKY_CLIENT_SECRET   (required)
    PGHOST                  (required) database host
    PGDATABASE              (required) database name
    PGUSER                  (required) database user
    PGPASSWORD              (required) database password
    MERIDIAN_BBOX           min_lat,max_lat,min_lon,max_lon
    MERIDIAN_INTERVAL       seconds between polls   (default 30)
    MERIDIAN_CREDITS        daily credit quota      (default 4000)
    MERIDIAN_ONCE           set to 1 to poll once and exit

The PG* variables are read directly by libpq, so psycopg.connect() takes no
arguments - the connection details never have to appear in this file.

Run locally:
    pip install "git+https://github.com/openskynetwork/opensky-api.git#subdirectory=python" "psycopg[binary]"
    export OPENSKY_CLIENT_ID=... OPENSKY_CLIENT_SECRET=...
    export PGHOST=10.10.20.18 PGDATABASE=meridian PGUSER=meridian PGPASSWORD=...
    MERIDIAN_ONCE=1 python ingest.py
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg
from opensky_api import OpenSkyApi

FEED = "air"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,  # containers log to stdout, nothing else
)
log = logging.getLogger("meridian-air")

# Kubernetes sends SIGTERM before SIGKILL. Handling it means the pod exits
# cleanly on a rollout instead of being killed mid-write.
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


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO aircraft_states (
    icao24, snapshot_time, callsign, origin_country,
    time_position, last_contact, longitude, latitude,
    baro_altitude, geo_altitude, on_ground, velocity,
    true_track, vertical_rate, squawk, spi,
    position_source, category
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s
)
ON CONFLICT (icao24, snapshot_time) DO NOTHING
"""


def to_timestamp(unix_seconds):
    """Unix seconds to an aware datetime, or None."""
    if unix_seconds is None:
        return None
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)


def to_row(state, snapshot_time):
    """Flatten a StateVector into a tuple matching INSERT_SQL's column order."""
    return (
        state.icao24,
        to_timestamp(snapshot_time),
        (state.callsign or "").strip() or None,
        state.origin_country,
        to_timestamp(state.time_position),
        to_timestamp(state.last_contact),
        state.longitude,
        state.latitude,
        state.baro_altitude,
        state.geo_altitude,
        state.on_ground,
        state.velocity,
        state.true_track,
        state.vertical_rate,
        state.squawk,
        state.spi,
        state.position_source,
        state.category,
    )


def connect():
    """Open a connection using the standard PG* environment variables."""
    conn = psycopg.connect()
    log.info("connected to postgres at %s", os.environ.get("PGHOST"))
    return conn


def write_states(conn, rows):
    """Insert one snapshot. Returns (attempted, inserted).

    The primary key is (icao24, snapshot_time), so re-inserting a snapshot
    the database already holds conflicts and does nothing rather than
    duplicating. That is what makes a pod restart safe - it may re-poll a
    snapshot it already stored, and the second write is a no-op.
    """
    if not rows:
        return 0, 0

    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
        inserted = cur.rowcount
    conn.commit()
    return len(rows), inserted


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        log.error("OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set")
        sys.exit(1)

    for var in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        if not os.environ.get(var):
            log.error("%s must be set", var)
            sys.exit(1)

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
    conn = connect()

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
                        rows = [to_row(s, states.time) for s in states.states]
                        attempted, inserted = write_states(conn, rows)
                        log.info(
                            "records=%d inserted=%d credits_used=%d/%d",
                            attempted, inserted, spent, daily_credits,
                        )
                except psycopg.Error as exc:
                    # A dropped connection is expected over a long run - the
                    # database restarts, the network blips. Reconnect and carry
                    # on rather than crashing the pod.
                    log.warning("database error, reconnecting: %s", exc)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    try:
                        conn = connect()
                    except Exception as reconnect_exc:
                        log.error("reconnect failed: %s", reconnect_exc)
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

    try:
        conn.close()
    except Exception:
        pass
    log.info("shutdown complete, %d credits used today", spent)


if __name__ == "__main__":
    main()