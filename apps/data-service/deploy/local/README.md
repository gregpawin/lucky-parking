# Workflow 1: local website, local API, local database

Run all components on your computer. Docker Desktop must be running. No host
Python, Node.js, MongoDB, or Socrata token is needed for this Docker workflow.
The Mapbox basemap and address search still need internet access and a public
Mapbox access token; this is local hosting, not an offline map application.

## Set up

From the repository root:

```powershell
cd apps/data-service
scripts\preflight.cmd
```

On macOS/Linux use `bash scripts/preflight.sh`. The preflight generates `.env`
with a database password if it does not already exist. Add these settings to
that file before starting:

```dotenv
MAPBOX_ACCESS_TOKEN=your_public_mapbox_token
# Optional: reuse an existing directory instead of copying your large CSV.
CITATIONS_DATA_DIR="C:/datasets/parking"
```

The selected directory must contain `Parking_Citations_*.csv`. Without
`CITATIONS_DATA_DIR`, the default is `apps/data-service/raw_data/`. If the
initial preflight reports a missing CSV, set the directory and run it again.
Use forward slashes and quotes in Windows paths. Keep `.env` private and uncommitted.

```powershell
scripts\start.cmd
```

On macOS/Linux: `bash scripts/start.sh`.

This merges `docker-compose.yml` with `deploy/local/compose.yml`. The stack is:

```text
Browser -> localhost:3000 (Next.js) -> api:8000 (FastAPI) -> postgis:5432
```

| URL | Purpose |
| --- | --- |
| http://localhost:3000 | Main Lucky Parking website |
| http://localhost:8000/docs | API reference |
| http://localhost:8080 | Separate citation sheet/map explorer |

All published ports bind to localhost. The API and website wait for database
initialization. A full CSV import may take hours. Track progress with
`scripts\local.cmd logs -f postgis` (or `bash scripts/local.sh logs -f postgis`).

In the website, select a neighborhood council (e.g. Westwood) or a geocoded
location, and choose dates covered by your CSV. Points use a 500-meter radius;
polygons use their full shape. Two selections are queried as a union. The map
shows up to the newest 1,000 records, with a visible truncation notice. Totals
and average fines use all matching records, not just the displayed markers.
Average fine excludes records with an unknown fine. The API also accepts
`limit`/`offset` for programmatic paging; the map deliberately loads one page.

Use `scripts\local.cmd ps`, `scripts\local.cmd logs`, and `scripts\local.cmd down` to manage the whole local stack (macOS/Linux: `bash scripts/local.sh`). Stopping the stack preserves the database volume.

## Faster verification import

For a fresh database only, set `CITATIONS_LOAD_LIMIT=200000` in `.env` before
starting. This is a partial dataset, suitable for checking installation.
Changing the setting later does not reload an existing database volume.

To load the full dataset without deleting a sample database: stop the sample
with `scripts\local.cmd down`, clear `CITATIONS_LOAD_LIMIT` in `.env`, and set
`COMPOSE_PROJECT_NAME=lucky-parking-local-full` before running `scripts\start.cmd`.
This creates a separate volume and preserves the sample. Keep enough free disk
space for both. On macOS/Linux use the equivalent `.sh` helpers.

To keep a complete database separate from a disposable verification instance,
use `docker compose -p lucky-parking-local-check -f docker-compose.yml -f
deploy/local/compose.yml up -d --build` for verification. Stop one stack before
running another on the same ports. Use the same project name for its logs and
checks. Do not remove a populated volume unless you intend to discard its data.

## Verify

After the load finishes, from `apps/data-service`:

```powershell
scripts\smoke_test.cmd
```

This checks tables, spatial queries, API, explorer, and the main website proxy.
`GET /health` is liveness; `GET /ready` verifies access to the citations table.

## Develop the website without rebuilding its container

Start the data services with `docker compose up -d --build`. Copy
`apps/web/.env.example` to `apps/web/.env.local`, set Mapbox, and keep
`PARKING_API_URL=http://127.0.0.1:8000`. From the repository root run
`pnpm install` then `pnpm --filter @lucky-parking/web dev`. Stop the website
container first if it already occupies port 3000. The same server proxy is
used in Docker and development mode.

## Current scope

This machine's verification configuration may use alternate ports in `.env`
to avoid existing services. Use `scripts\local.cmd ps` to see its actual ports.

The initial load uses the existing CSV timestamp convention (stored as UTC).
Changing that convention for historical data requires a separate migration.
Socrata incremental sync is experimental and is not started automatically.
The main website does not fall back to Socrata when PostGIS is unavailable.
