# fabric-medallion-dbt-community

> **Community edition.** The same gold layer on five community engines, run from GitHub
> Actions or deployed to Fabric. The Microsoft-supported version — Fabric Warehouse
> (`dbt-fabric`) and Fabric Spark (`dbt-fabricspark`) — is
> [fabric-medallion-dbt](https://github.com/djouallah/fabric-medallion-dbt).

A medallion architecture on Microsoft Fabric: one dbt project that builds the **same AEMO gold
layer** on five engines, every one of them writing into one shared OneLake lakehouse.

![Medallion architecture on Microsoft Fabric. Ingest with any tool (PySpark notebook, Dataflow Gen2, Python notebook) into Bronze: raw files and Delta tables in OneLake, as ingested. Transform in SQL with dbt into Silver (typed, deduplicated, conformed) and Gold (facts and dimensions, business-ready). Serve Power BI semantic models with Direct Lake (reads Delta in OneLake, no data copy). The product: well-built Delta tables that Direct Lake reads fast.](docs/medallion-fabric-dbt.svg)

## Engines

| target | adapter | writes |
|---|---|---|
| `duckrun` | [`duckrun`](https://github.com/djouallah/duckrun) | Delta Lake on OneLake, via delta-rs |
| `iceberg` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | Iceberg, through the OneLake Iceberg REST catalog |
| `ducklake` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB |
| `spark` | [`dbt-spark`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-spark) (`method: session`) | Delta on OneLake, via local PySpark + delta-spark on the runner |
| `polars` | [`dbt-polars`](https://github.com/datamindedbe/incubator-dbt-polars), from [a fork](https://github.com/djouallah/incubator-dbt-polars/tree/aemo) until its fixes are released | Iceberg, through the OneLake Iceberg REST catalog (pyiceberg). Timestamps are tz-naive: Polars SQL has no zoned type |

None is supported by Microsoft; they are here to show that the business logic is portable.

## Two ways to run

- **local** (default): `pipeline.yml` builds every engine on the GitHub runner.
- **fabric**: CI deploys the items with fabric-cicd and runs the deployed pipeline in Fabric
  (duckrun, iceberg, ducklake, polars); spark builds on the runner either way.

## Docs

[The thesis](docs/thesis.md), how to [run it](docs/run.md), and the rest in [docs/](docs/README.md).

## License

MIT
