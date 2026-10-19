# Lucky Parking

Explore Los Angeles parking citations with the main website in `apps/web` and
the shared Python/PostGIS data service in `apps/data-service`.

| Workflow | Status | Start here |
| --- | --- | --- |
| Local website + local database | Active | [Local setup](apps/data-service/deploy/local/README.md) |
| Local website + cloud database | Planned | [Workflow 2](apps/data-service/deploy/local-web-cloud-data/README.md) |
| Hosted website + cloud database | Planned | [Workflow 3](apps/data-service/deploy/cloud/README.md) |

The first workflow runs Next.js, FastAPI, PostGIS, and an optional-use diagnostic
explorer on your computer. The browser calls the website server; that server
calls the parking data API. The website never receives database credentials.
The two cloud workflows are reserved for later implementation.

## Start the fully local workflow

Install and start Docker Desktop. From the repository root:

```powershell
cd apps/data-service
scripts\preflight.cmd
# Configure the CSV directory and MAPBOX_ACCESS_TOKEN in .env.
scripts\start.cmd
```

macOS/Linux: `bash scripts/preflight.sh`, then `bash scripts/start.sh`.
The website normally runs at http://localhost:3000 after the database import.
Mapbox still requires a token and internet access. See the local guide for
sample imports, existing CSV directories, verification, and host development.

## Layout

- `apps/web`: main Lucky Parking Next.js website.
- `apps/data-service`: CSV loading, PostGIS, FastAPI, tests, diagnostic explorer,
  and the three workflow entry points. Formerly `projects/data-science/postgis_db`.
- `apps/api`: alternate legacy Express/MongoDB service; not part of these workflows.
- `packages`: shared JavaScript tooling and design components.
- `projects/data-science`: analysis, historical pipelines, and local raw data.
- `docs`: contributor documentation; see [the workflow overview](apps/data-service/deploy/README.md)
  for the current PostGIS architecture.
