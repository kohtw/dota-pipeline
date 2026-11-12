# Contributing

How this repo is laid out, how to run it, and the few rules that keep it easy to work on.

## Layout

```
dags/                      Airflow DAGs: orchestration only (schedule, retries, task wiring)
  dota_daily_download.py   daily 00:00 SGT: ranked All Pick replays -> Garage
  replay_parse.py          parse one replay in Garage -> parsed Parquet tables in Garage
  dota/                    the actual logic, plain Python, no Airflow
    config.py              constants only (cheap to import)
    steam.py               Valve Steam Web API
    opendota.py            OpenDota (replay download URL)
    storage.py             Garage / S3
spark_jobs/                PySpark jobs
  replay_spec.py           which raw tables/columns to extract from a replay (edit this, not Java)
  parse_replays.py         replays -> one Parquet dataset per table
parser/                    Java replay extractor (Clarity), called by parse_replays.py; Dockerfile = dota-parser image
docs/data-contract.md      raw layer: what lands in Garage, table by table and column by column
docs/clean-contract.md     clean layer + warehouse: grain, key and columns of each clean table and mart
warehouse/                 Postgres DDL (init/), a fixture match, and make_dev_db.py for local tests
dbt/                       dbt project: marts built from the clean tables
scripts/                   one-off / manual tools (ingest, clean)
airflow/, garage/          Docker images for the local stack
```

## Running locally

1. Get a Steam API key (https://steamcommunity.com/dev/apikey) and generate an Airflow Fernet key. Put both in `.env` at the repo root:
   ```
   python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
   ```
   ```
   STEAM_API_KEY=your-key
   AIRFLOW_FERNET_KEY=the-generated-key
   ```
   `.env` is gitignored. Never commit it. Compose refuses to start without `AIRFLOW_FERNET_KEY`. Keep the key stable: a new key makes stored Airflow Variables unreadable.
2. Build the parser and clean images, then start everything:
   ```
   docker build -t dota-parser -f parser/Dockerfile .
   docker build -t dota-spark-clean -f spark_jobs/Dockerfile .
   docker compose up -d --build --remove-orphans
   ```
   Airflow starts these two images as containers; `docker compose --build` does not rebuild them. Rebuild an image after pulling changes to `parser/` or `spark_jobs/`, or Airflow keeps running the old code.
3. Open:
   - Airflow: http://localhost:8080 (no login)
   - Garage (file browser): http://localhost:3909

To parse a replay, trigger `replay_parse` in the Airflow UI with config `{"match_id": 1234567890}`. Its replay must already be in Garage (`raw/replays/{match_id}.dem`).

To parse replays on your machine without Airflow (Spark needs Linux, so use the image; rebuild it after changing `parser/` or `spark_jobs/`):
```
docker run --rm -v "$PWD/data/output:/demos:ro" -v "$PWD/data:/out" dota-parser "/demos/*.dem" /out/parsed
```
Output: `data/parsed/{table}/match_id={id}/*.parquet`.

To add or change a raw column or table, edit `spark_jobs/replay_spec.py` and update `docs/data-contract.md` in the same PR.

Don't trigger `dota_daily_download` by hand. Each run saves a bookmark that the next run starts from (see the file's docstring), so a manual run shifts the next day's window.

### Upgrading an existing stack

Follow these steps once if you started the stack before the Airflow database service was renamed from `postgres` to `airflow-db`. Do them in this order.

1. Add `AIRFLOW_FERNET_KEY` to `.env`. `.env.example` has the command that generates one. Until the key is set, every `docker compose` command stops with an error, including `ps` and `logs`.
2. Run `docker compose up -d --build --remove-orphans`. The rename leaves the old `postgres` container running, and it uses the same `pg-data` volume as `airflow-db`. Two Postgres servers on one data directory corrupt it (run history, Variables, the download bookmark). `--remove-orphans` removes the old container only, not the volume.
3. Set the Airflow Variables again (`dota_seq_bookmark`, `dem_retention`, `steam_pages_per_run`, `steam_fetch_pages_per_run`, `matches_per_run`) in the Airflow UI under Admin > Variables. Variables saved before the shared key cannot be decrypted. Tasks then fall back to defaults without an alert: a missing `dota_seq_bookmark` makes the next `dota_daily_download` run behave like a first run, so it saves a new bookmark and downloads nothing that day. `dem_retention` falls back to `keep`.
4. Rebuild your dev DuckDB file with `python warehouse/make_dev_db.py ~/.cache/dota/dev.duckdb` before the next local `dbt build` (rule 10). The project now has models that read `stg.steam_*`.

### Pools, the Steam fetch and failed matches

- **Pools.** `airflow-init` creates the pools `spark` (2 slots, parser and Spark clean containers), `steam_api` (1) and `opendota_api` (2). A task whose pool does not exist waits in "scheduled" forever, so on a stack started before the pools existed, run `docker compose up -d` once more.
- **Steam fetch budget.** The Variable `steam_fetch_pages_per_run` sets how many pages of 100 matches `steam_fetch` saves each hour (default 10). Keep it at 50 or below: `steam_summary_elt` loads 50 pages per run, so more pages pile up unloaded.
- **Alerts.** Failures and retries are logged to `/data/alerts/failures.jsonl`. To also get failures in Discord, set `ALERT_WEBHOOK_URL` in `.env` to a webhook URL. It is optional; blank turns it off.
- **Quarantined matches.** A match whose parser or clean container crashes in 3 sweeps is skipped from then on. A crash is a non-zero exit other than 137 (a kill for memory, which Airflow retries) and, for clean, 2 (a rejection, see `clean/_rejected/`). A failure where no container ran (image missing, docker-proxy down) is not counted. To retry a quarantined match after a fix, delete `parsed/_failed/match_id={id}/` or `clean/_failed/match_id={id}/` in Garage.

## Rules

### 1. DAG files only orchestrate

A DAG file says *when* and *in what order*. The *how* lives in `dags/dota/`. A `@task` should be a few lines that call into `dota.*`.

Why: logic in plain modules can be run and tested without Airflow, and reused by other DAGs or scripts.

```python
# Good: the task just wires modules together
@task(pool="opendota_api", **API_RETRY)
def download_replay(match_id: int) -> str:
    from dota import opendota, storage

    key = f"replays/{match_id}.dem"
    if not storage.exists(key):
        storage.upload_replay(opendota.replay_url(match_id), key)
    return key

# Bad: HTTP calls, S3 setup, error handling, decompression all inside the task
@task
def download_replay(match_id: int) -> str:
    s3 = boto3.client("s3", endpoint_url=..., aws_access_key_id=..., ...)
    r = requests.get(f"https://api.opendota.com/api/matches/{match_id}")
    ...  # 30 more lines
```

### 2. One module per outside service

`steam.py`, `opendota.py`, `storage.py`. If you add a new service, give it a new module. Keep function names plain and descriptive (`replay_url`, `exists`, `upload_replay`).

### 3. Keep DAG files cheap to import

The Airflow scheduler re-reads every DAG file roughly every 30 seconds. At the top of a DAG file, import only `airflow`, `dota.config`, `dota.alerts`, `dota.assets` and `dota.jobs`. Import everything else (`dota.steam`, `dota.storage`, `boto3`, ...) inside the task function.

### 4. Config and secrets

| What | Where |
|---|---|
| Fixed values (API base URLs, game mode IDs, bucket name) | Constants in code |
| Values that differ per environment (S3 endpoint) | Environment variables (set in `docker-compose.yml`) |
| Secrets (API keys) | `.env`, never in code or git |

### 5. Tasks must be safe to re-run

A task that runs twice should give the same result. Check whether the output already exists, and write in a way that never leaves half a file. `download_replay` is the pattern to copy.

### 6. Follow the data contract

Paths, file names and columns in Garage are defined in [docs/data-contract.md](docs/data-contract.md). If you change what gets written, update the contract in the same PR and tell everyone downstream.

### 7. Comments explain why

Write comments for things the code can't say: why a value was chosen, or a quirk of an outside API (e.g. "Valve serves zstd despite the .bz2 name"). Don't narrate what the code does. Delete code instead of commenting it out; git keeps history.

### 8. Respect the outside services' limits

- **Steam:** leave 2 seconds between calls, and on a 429 or 5xx back off for 15, 30, 60, then 120 seconds (`dota.steam`). A 40-page run at 2 seconds finished with no 429s; at 1 second Valve returned 429 after about 10-20 calls. Never loop without a delay.
- **OpenDota:** free tier has a daily cap. Never add a call inside a loop without a cap. Cache what you fetch.
- A failure in one match must not stop the others. Log it, skip it, carry on.

### 9. Data layers

| Layer | Where | What goes in | Who writes it |
|---|---|---|---|
| Raw | Garage `raw/` | Exactly what the source or parser produced. Never edited | Ingestion, parser |
| Clean | Garage `clean/` (parquet) | Typed, deduplicated, one table per event type, game-clock time | Spark |
| Warehouse | Postgres schemas `clean`, `marts` | `clean`: Spark output loaded as-is. `marts`: built by dbt for the dashboard | Load step, dbt |

- Data only moves forward. A later layer must be rebuildable from the one before it, with no outside API calls.
- Missing data is `null`, never `0`.
- Every table has a key. State it in the contract (e.g. `match_id` + `player_slot`), and test it.
- Don't read raw files from the dashboard. It reads `marts` only.
- Every clean table's grain (what one row means) and key are written in [docs/clean-contract.md](docs/clean-contract.md).

### 10. Database and dbt (owner: Chris)

- Schema changes are made in code (a migration or a dbt model), never by hand in a running database.
- Every dbt model has a description and tests: not null and unique on its key, plus accepted values or ranges where they make sense.
- Don't use the Airflow metadata database for project data. Analytics data lives in its own database.
- Document thresholds and assumptions in the model's description (e.g. the time window used to link damage to a death).
- Run `dbt build` before opening a PR that touches a model. No Docker needed; it runs on a local DuckDB file with a fixture match:
  ```
  python warehouse/make_dev_db.py ~/.cache/dota/dev.duckdb      # build the dev DB from warehouse/init + the fixture
  DUCKDB_PATH=~/.cache/dota/dev.duckdb DBT_TARGET_PATH=~/.cache/dota/target DBT_LOG_PATH=~/.cache/dota/logs \
    dbt build --project-dir dbt --profiles-dir dbt
  ```
  (`--project-dir` is where `dbt_project.yml` is; `--profiles-dir` is where `profiles.yml` is. The `DBT_*_PATH` variables keep dbt's output out of the repo. Install with `pip install duckdb dbt-core dbt-duckdb` in a venv outside the repo.)
- Write portable SQL (plain joins and window functions) so the same models run on DuckDB and Postgres.

### 11. Tests

- New logic gets a test. Fix a bug by writing the failing test first.
- Python tests must run without the network, Docker or real API keys. Mock Steam, OpenDota and S3. (dbt tests run on the local DuckDB file, see rule 10.)
- Don't change a test just to make it pass. Change it only when the contract changed, and say so in the PR.

### 12. Secrets and big files

- Never commit `.env`, API keys, replays (`.dem`, `.dem.zst`), Steam page dumps, or anything under `data/` and `output/`, except the reference demos in `data/output/` and parsed output in `data/parsed/`. Put other samples in the shared Drive folder instead.
- The dev S3 keys in `docker-compose.yml` are local-only. Never reuse them anywhere else.
- Replays and Steam pages live in the shared Drive folder. Don't re-download what is already there.

### 13. Ownership

| Area | Owner |
|---|---|
| Orchestration (DAGs, retries, alerts) | Winston |
| Ingestion, Garage, replay parser | Ting Wei |
| Spark clean layer | Maximus |
| Clean-layer contract, Postgres schema, dbt | Chris |
| Dashboard | Brennan |

Anyone can open a PR on any area. Tag the owner as the reviewer, and tell them in the group chat if the change affects their work. If you change a contract, tag everyone downstream.

### 14. Shared files

Edit `CONTRIBUTING.md`, `.gitignore`, `docker-compose.yml` and `docs/data-contract.md` only in a small PR of their own, with a line in the group chat. Don't mix them into a feature PR.

## Testing a module without Airflow

Run inside the Airflow container, where the env vars and dependencies are already set up:

```
docker compose exec -w /opt/airflow/dags airflow-scheduler python -c "from dota import steam; print(steam.newest_seq())"
```

## Git

- Branch off `master` as `feat/<name>-<topic>` (or `fix/...`, `docs/...`). Never commit straight to `master`.
- Commit messages: `type(scope): what changed`, e.g. `feat(dbt): add mart_deaths`. Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`. Keep each commit to one finished idea.
- Open a PR into `master` and get one review. Keep PRs small: one idea each.
- PR description: what changed, why, how you tested it, and which contract or table is affected.
- Don't commit `data/` (except the reference demos in `data/output/` and parsed output in `data/parsed/`), `output/`, other replays, Steam dumps or `.env`.
- Never force-push a shared branch, and never rewrite commits that others have pulled.

## Definition of done

A change is done when:

1. It works from a clean `docker compose up -d --build --remove-orphans`.
2. Its tests pass, and it re-runs safely (rule 5).
3. `docs/data-contract.md` matches what it writes (rule 6).
4. Comments explain the why (rule 7), and there is no commented-out code.
5. It is reviewed and merged, and the people downstream know.
