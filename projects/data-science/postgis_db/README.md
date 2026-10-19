# This project moved

The shared backend now lives in [`apps/data-service`](../../../apps/data-service/).
Its local `.env` moved with it. Citation CSVs elsewhere in `projects/data-science/raw_data`
remain in place and can be mounted through `CITATIONS_DATA_DIR`.

Start with the [three workflows](../../../apps/data-service/deploy/README.md).
Only the [fully local workflow](../../../apps/data-service/deploy/local/README.md)
is active in this phase. This directory is a navigation pointer, not a second
copy of the database or API.
