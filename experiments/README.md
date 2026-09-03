# Experiments

These scripts are exploratory notes and are not part of the released package or
its default quality checks. They may contact public services or a disposable
Neon database; do not use production credentials.

## Safe invocation

- `httpx_scheme.py`: compare HTTP/1.1 and HTTP/2 negotiation against
  `https://example.com`; run with `uv run --with httpx python experiments/httpx_scheme.py`.
- `greenlet-requests/gevent_http_experiment.py`: compare synchronous and
  greenlet-based HTTP concurrency; run with `uv run --with geventhttpclient --with requests --with gevent python experiments/greenlet-requests/gevent_http_experiment.py`.
- `inspect_ws_traffic.py` and `ws_pipelining.py`: inspect PostgreSQL WebSocket
  frames using `NEON_DATABASE_URL`; run only against a disposable database.
- `qqqqq.py` and `wwwwww.py`: investigate SQLAlchemy session and telemetry
  behavior using `NEON_DATABASE_URL`; these scripts require their referenced
  development dependencies and a disposable database.

Experiment telemetry is disabled by default. To explicitly enable scrubbed
Logfire telemetry for the two SQLAlchemy experiments, set both
`ENABLE_EXPERIMENT_TELEMETRY=1` and `LOGFIRE_TOKEN`. No script enables payload
capture or disables scrubbing.
