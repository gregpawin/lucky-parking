# Workflow 3: hosted website, cloud data service

Planned; not implemented or deployment-tested in this phase.

The hosted Next.js server calls the hosted parking API, which accesses private
PostGIS. These components may share a VPS or run on separate hosts. The browser
still calls its website's same-origin API route. The API key stays server-side.

Bootstrap data by creating a local `pg_dump` and restoring it on the server.
The earlier `../VPS.md` and `../../docker-compose.prod.yml` describe the data
tier only; a complete website deployment still needs to be integrated and
tested. Do not use the localhost environment or unauthenticated settings.

Still required: supported database image, deployment/restore tests, domains and
TLS, restricted database credentials, backups and recovery verification,
monitoring, and a validated data-refresh strategy.
