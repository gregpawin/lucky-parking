# Lucky Parking website

The main Next.js website uses the Python parking data API through its own
`/api/v1/citations` server route. It does not connect to PostgreSQL directly.

Start with the [three deployment workflows](../data-service/deploy/README.md).
The [fully local workflow](../data-service/deploy/local/README.md) is active;
cloud workflows are planned for a later phase.

For host development, copy `.env.example` to `.env.local`, set
`MAPBOX_ACCESS_TOKEN`, and point `PARKING_API_URL` at the local API. Run
`pnpm --filter @lucky-parking/web dev` from the repository root. Keep
`PARKING_API_KEY` server-side. No Socrata token or MongoDB service is needed.
