# matrix-dag-vizualizer

A Matrix client (Go, [mautrix-go](https://github.com/mautrix/go)) that renders the
**event DAG** of a room — every event with its `prev_events` and `auth_events` —
in a web UI (TypeScript + [Cytoscape.js](https://js.cytoscape.org/) + [ELK](https://eclipse.dev/elk/)).

- Live updates over Server-Sent Events as new events arrive via `/sync`
- Forks, merges and forward extremities highlighted; missing (unknown) parents shown as placeholders
- Two layouts: **depth** (one row per event depth, fast for any size) and **ELK layered**
- Click an event for its full PDU JSON, parents, children and auth events
- Colour by event type or sender, optional auth-edge overlay, dark/light theme
- Shareable URLs (`#room=!id&event=$id`)

## How it gets the DAG

The regular client-server API strips `prev_events`, `auth_events` and `depth`
from events. dagviz requests them with a filter of
`{"event_format": "federation"}` on `/sync` and `/rooms/{id}/messages`, which
Synapse honours (tested on Synapse 1.162). Other homeserver implementations may
not support it.

### Synapse admin mode (automatic)

At startup (and every `admin_check_interval`) dagviz checks whether its account
is a Synapse server admin. If so, it additionally:

| Feature | Admin API |
|---|---|
| Fill in missing events by walking `prev`/`auth` references backwards (**Resolve missing**) | `GET /_synapse/admin/v1/fetch_event/{event_id}` |
| Browse and backfill **any room on the server**, not just joined ones | `GET /_synapse/admin/v1/rooms`, `.../rooms/{id}/messages` |
| Show the server's real forward extremities (double ring) | `GET /_synapse/admin/v1/rooms/{id}/forward_extremities` |

Without admin rights everything else works; events the account can't see
(before it joined, hidden by history visibility, timeline gaps) appear as
dashed "missing" placeholders.

## Running with Docker

Images are published to GHCR on every push (`latest` = `main`, plus branch, short-SHA and semver tags):

```sh
cp sample.config.yaml config.yaml   # edit homeserver / user / password
docker compose up -d
open http://localhost:8080
```

See [`docker-compose.yaml`](docker-compose.yaml). The config is read from
`/config/config.yaml`; the login session is persisted in the `/data` volume so
restarts reuse the same device. If you bind-mount a host directory to `/data`
instead of the named volume, make it writable by UID 65532 (the image runs as
non-root).

## Configuration

All options, with defaults and explanations, are in
[`sample.config.yaml`](sample.config.yaml). Secrets can be passed via
environment variables instead: `DAGVIZ_MATRIX_HOMESERVER`,
`DAGVIZ_MATRIX_USER_ID`, `DAGVIZ_MATRIX_PASSWORD`, `DAGVIZ_MATRIX_ACCESS_TOKEN`,
`DAGVIZ_WEB_BASIC_AUTH_USER`, `DAGVIZ_WEB_BASIC_AUTH_PASSWORD`.

> The UI exposes room contents to anyone who can reach it. Set `web.basic_auth`
> or put it behind an authenticating reverse proxy.

## Development

Requirements: Go 1.26+, Node.js LTS.

```sh
cd web && npm ci && npm run build && cd ..   # builds web/dist, embedded into the binary
go run ./cmd/dagviz -config config.yaml

# Frontend hot reload against a running backend on :8080
cd web && npm run dev
```

```sh
go vet ./... && go test ./...
docker build -t dagviz .
```

### Layout

```
cmd/dagviz/           entrypoint
internal/config/      YAML config, defaults, env overrides, validation
internal/matrix/      login/session, federation-format sync, backfill, Synapse admin helpers
internal/dag/         in-memory per-room event graph + live subscriptions
internal/web/         REST + SSE API, serves the embedded SPA
web/                  Vite + TypeScript frontend (embedded via go:embed)
```

### HTTP API

| Method | Path | |
|---|---|---|
| GET | `/api/config` | UI options, account, admin status |
| GET | `/api/rooms` | known rooms |
| GET | `/api/server-rooms?search=&from=` | all server rooms (admin) |
| GET | `/api/rooms/{room}/dag?limit=` | nodes, missing refs, extremities |
| POST | `/api/rooms/{room}/backfill?count=` | paginate older history |
| POST | `/api/rooms/{room}/resolve?budget=` | fetch missing events (admin) |
| GET | `/api/rooms/{room}/event?id=` | raw PDU |
| GET | `/api/rooms/{room}/stream` | SSE stream of new events |
| GET | `/healthz` | liveness |
