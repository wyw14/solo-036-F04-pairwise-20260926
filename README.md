PigFlow is a local HTTP control service for long-distance pipeline pigging operations.

## Route queue

A route is leased to a single run at a time. Instead of failing a dispatch
against an occupied route, duty personnel can queue a launch application and
wait for the handoff:

- `POST /api/routes/{routeID}/queue` — queue an application
  (`{"station_id": "...", "target_pressure": 10.0, "expires_in_seconds": 600}`;
  TTL defaults to 1800 s, capped at 86400 s). Returns the entry with its
  position; if the route is free the application is granted immediately.
- `GET /api/routes/{routeID}/queue` — show the current lease holder and the
  expected turn order of the effective (waiting, unexpired) candidates.
- `DELETE /api/routes/{routeID}/queue/{entryID}` — withdraw a waiting
  application; its loaded run is cancelled.

When the occupying run completes or is cancelled, the route is handed to the
first valid candidate in FIFO order. Applications that expired or whose launch
conditions no longer hold (station disabled, pressure out of limits, path
unavailable, run no longer waiting) are decided `expired`/`invalid`, recorded
as incidents, and never occupy the route; if no valid candidate remains, the
route stays free. Queue state is in-memory; journal events
(`queue_enqueue`/`queue_withdraw`/`queue_grant`/`queue_expire`/`queue_reject`)
provide the audit trail.
