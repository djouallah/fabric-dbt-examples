# CI, and what each run cost

What the four workflows do and why none of them but `ci.yml` runs on push, plus the run
record: the Fabric items each leg touched, what it wrote, and what it cost in capacity units.

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a five-way matrix
  (one environment per engine). Runs on every push. The matrix cannot be collapsed into one
  job: `dbt-fabric` and `dbt-fabricspark` shadow each other under `dbt.adapters`, and dbt OSS
  2 and `duckrun` both want to own the `dbt` console script.
- `pipeline.yml` — manual only, and the whole pipeline in one file: it lands ONCE in a shared
  `land` job, then runs all five engines in parallel as a matrix of `build` legs, then the
  **layout** job reads every engine's tables back and the **record** job compares the
  fingerprints and commits one run record to `history/runs/`. Each leg does `dbt build`
  (models and tests) → fingerprint. **Two modes, chosen by the `mode` input.** `local` (the
  default): every engine builds on the runner — DuckDB and Polars in-process, dwh and spark as
  clients of the Warehouse / Livy. `process_limit` defaults to 50, because the runner has 7 GB.
  `fabric`: CI deploys with fabric-cicd and runs the deployed pipeline once per in-process
  engine, waiting for each (next section); dwh and spark still build from the runner. Both
  modes run the same `.github/scripts/run_dbt.py` — build, the 40613 loop that waits out ducklake's auto-paused
  catalog SQL DB, a guarded `dbt retry`, then the fingerprint — so a local leg and a Fabric run
  are one sequence on different compute. A `compact` job folds the Iceberg catalog's small files afterwards, before
  `layout` measures them. `process_limit` is a dispatch input: files each fact model folds per
  run, **newest first**, on every engine — so a partial load is recent data, and the backlog
  shows up as test WARNINGS that fall run by run rather than as a failed build. `deploy` (`none` / `no_model` / `full`) deploys the
  in-Fabric demo after the build (next section).
- `capacity.yml` — fires after every pipeline run and once a day: reads capacity units per
  run and engine from the Fabric Capacity Metrics model and commits `history/cu.json`.
- `docs.yml` — fires after every pipeline run: `dbt docs generate --static` and deploys the
  one self-contained page to GitHub Pages — **[the DAG and the catalog](https://djouallah.github.io/fabric-medallion-dbt-community/)**.
  It builds nothing and spends no Fabric compute. **duckrun of the five**, because the DAG is
  the same on all of them and the *catalog* is not: duckrun reports `num_rows`, `bytes` and
  `last_modified` out of the Delta log, where dbt-fabric's catalog query gives an approximate
  row count and nothing else. It is also the only engine whose models guard their parse-time
  `run_query` on `flags.WHICH`, so generating its docs fires no query; on dwh the same command
  would run real `OPENROWSET` queries against the Warehouse.

Manual only because deploying Fabric items and spending capacity is a deliberate act, and
the record job commits to `history/`, so a push trigger would make the commit start the
next run.

## What it cost, and what it wrote

Ported from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout)
and adapted to shared, persistent items. Every pipeline run leaves one record in
`history/runs/` — the Fabric item GUIDs it touched, each leg's compute window, the parquet
layout of every engine's tables (files, row groups, `fct_summary`'s per-column encodings and
physical row order) and the parity fingerprints — and the `Capacity units` workflow keeps
`history/cu.json`: **compute** capacity units per run and engine, read from the Capacity Metrics
model for each leg's items inside its own hours. Storage transactions are deliberately not
attributed: the lakehouse is shared, so its OneLake operations in any window belong to
everybody. [`history/README.md`](../history/README.md) has the schemas and the caveats.

Note: cancelling a GitHub job does **not** stop Fabric — the notebook or Livy session keeps
running, and billing.

## Running it inside Fabric

```bash
gh workflow run pipeline.yml -f mode=fabric -f engines=iceberg   # deploy, then run in Fabric
gh workflow run pipeline.yml -f engines=iceberg -f deploy=full    # local build, then deploy
```

`mode=fabric` runs `deploy.py` (repo + items) with fabric-cicd after `land`, then
`.github/scripts/fabric_run.py` per engine: it starts the deployed `run_pipeline` with that
engine (and `land=False` — CI landed once already), polls the job instance, and copies the
notebook's fingerprint from `Files/dbt_runs/<run_id>/` into `history/parity/`, so parity grades
both modes the same way. Every job logs in with `azure/login`; the scripts' tokens come from
`az` (`.github/scripts/tokens.py`).

`deploy` (`none` by default; `no_model` skips the semantic models and their reframe, the slow
part) runs `deploy.py` after the build: the git-tracked repo into the `dbt` lakehouse's
`Files/dbt`, `fabric_items/` (a notebook, its `deploy_config` variable library, a pipeline),
one semantic model per engine built, and the pipeline's 12-hourly schedule.

The notebook is the Fabric form of one leg: it runs the same `provision.py` →
`download_aemo.py` → `run_dbt.py` from that copy, on the engine the pipeline passes, else
`dbt_target` in `fabric_items/deploy_config.VariableLibrary/variables.json`. The pipeline runs
it at 2 vCores and again at 8 if that fails (`pipelinecore`).

The semantic model deploys as `aemo_<engine>`, bound to `<engine>_mart`. Direct Lake has no
schema parameter — a partition's schema is a literal in the model — so `deploy.py` writes it
in per engine, with the OneLake GUIDs (`deploy.bind_model`). A Direct Lake model reframes on
deploy, which is why it deploys after the build. dwh's model reads the Warehouse item through
Direct Lake on OneLake.
