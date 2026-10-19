# Deployment workflows

The database builder, PostGIS queries, and FastAPI service are shared by all
three workflows. Only the first workflow is implemented in this phase.

| Workflow | Website | Data service | Status |
| --- | --- | --- | --- |
| [Fully local](local/README.md) | Local Next.js | Local API + PostGIS | Implemented; verified with a 200,000-row sample |
| [Local website, cloud data](local-web-cloud-data/README.md) | Local Next.js | Cloud API + PostGIS | Planned; configuration template only |
| [Fully hosted](cloud/README.md) | Hosted Next.js | Cloud API + PostGIS | Planned; configuration template only |

The browser calls its own website's `/api/v1/citations` route. The Next.js
server calls `PARKING_API_URL`, adding a server-only `PARKING_API_KEY` when
configured. PostgreSQL credentials never belong in the website environment.

`web_sheet/` is a separate diagnostic explorer; `apps/web` is the main website.
The Express/MongoDB service in `apps/api` is an alternate legacy backend and
is not used by these workflows.

`VPS.md`, `Caddyfile`, and the service's production Compose file are earlier
cloud deployment drafts. They have not been validated for the new website
integration and are not the active local workflow.
