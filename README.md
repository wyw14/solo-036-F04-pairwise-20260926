PigFlow is a local HTTP control service for long-distance pipeline pigging operations.

## Route reservations and queuing

A route can only be fenced to one running operation at a time. Attendants can
line runs up for a route instead of hitting the conflict on launch:

- `POST /api/routes/{routeID}/queue` — submit an application
  (`station_id`, `target_pressure`, optional RFC3339 `expires_at`). When the
  route is free the run starts immediately (`201`, `status: "started"`);
  otherwise it joins the FIFO queue (`202`, `status: "queued"` with its
  position).
- `GET /api/routes/{routeID}/queue` — owner plus the predicted turn order;
  every waiting run carries its `position`, whether it is still a `valid`
  candidate (and why not), and `next_run_id` names the next effective
  candidate.
- `GET /api/routes` — queue overview for all routes.
- `DELETE /api/routes/{routeID}/queue/{runID}` — withdraw a waiting
  application and cancel its run (`204`); an application that already holds
  the route must be cancelled as a normal operation instead (`409`).

When the running operation completes or is cancelled, the route is released
and handed to the first queued application **only if it is still a valid
candidate**. Expired applications and applications whose run ended, advanced
to another epoch, or whose launch conditions changed are skipped (and their
lingering runs cancelled) — they never automatically occupy the route. The
queue, like the leases, is persisted in the snapshot and pruned on recovery.

The direct `POST /api/operations` launch keeps its fail-fast behaviour: a busy
route answers `409` with a pointer to the queuing endpoint.
