# Workflow 2: local website, cloud data service

Planned; not implemented or deployment-tested in this phase.

The developer runs only `apps/web` locally. Its Next.js server sends requests
to a hosted parking API. No CSV import or local database is required. Copy the
environment template to the website's private environment file only after a
cloud API is deployed and verified. Never expose its API key as NEXT_PUBLIC.

Still required: deploy and validate the cloud data service, provision consumer
keys, test HTTPS access, rate limiting, and website behavior against that host.
