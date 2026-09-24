# Azure Data Factory definitions

- **`pipelines/PL_Ingest_Bronze_Daily.json`** — the daily orchestration pipeline: copies the three source extracts into Bronze, then triggers the Databricks Silver and Gold notebooks in sequence, with explicit `dependsOn` conditions.
- **`linkedServices/`** — connections to ADLS Gen2 (storage) and Azure Databricks (compute), authenticated via managed identity rather than stored secrets.
- **`datasets/`** — dataset definitions for the landing and Bronze layers. `DS_Landing_Orders_CSV` / `DS_Bronze_Orders` are included as the representative pair; `Customers` and `Products` datasets follow the identical pattern (same linked service, different `folderPath`) and were omitted here to keep the repo readable rather than repeating near-identical JSON three times.

**Trigger:** a tumbling window trigger (daily, 06:00 UTC) would be attached to `PL_Ingest_Bronze_Daily` in the deployed environment, with a 3-retry policy and failure alert routed to Azure Monitor / email action group.
