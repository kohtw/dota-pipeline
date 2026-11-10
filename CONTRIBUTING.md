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
docs/data-contract.md      what lands in Garage, table by table and column by column
scripts/                   one-off / manual tools (ingest, clean)
airflow/, garage/          Docker images for the local stack
```

## Running locally

1. Get a Steam API key (https://steamcommunity.com/dev/apikey) and put it in `.env` at the repo root:
   ```
   STEAM_API_KEY=your-key
   ```
   `.env` is gitignored. Never commit it.
2. Build the parser image, then start everything:
   ```
   docker build -t dota-parser -f parser/Dockerfile .
   docker compose up -d --build
   ```
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

## Rules

### 1. DAG files only orchestrate

A DAG file says *when* and *in what order*. The *how* lives in `dags/dota/`. A `@task` should be a few lines that call into `dota.*`.

Why: logic in plain modules can be run and tested without Airflow, and reused by other DAGs or scripts.

```python
# Good: the task just wires modules together
@task(retries=5, retry_delay=timedelta(minutes=1))
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

The Airflow scheduler re-reads every DAG file roughly every 30 seconds. At the top of a DAG file, import only `airflow` and `dota.config`. Import everything else (`dota.steam`, `dota.storage`, `boto3`, ...) inside the task function.

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

## Testing a module without Airflow

Run inside the Airflow container, where the env vars and dependencies are already set up:

```
docker compose exec -w /opt/airflow/dags airflow-scheduler python -c "from dota import steam; print(steam.newest_seq())"
```

## Git

- Branch off `master`, open a PR, get one review.
- Don't commit `data/` (except the reference demos in `data/output/` and parsed output in `data/parsed/`), other replays or `.env`.
