# Dota 2 match dashboard

The Streamlit app reads dbt marts from a local DuckDB file. It shows the match scoreboard, final player stats, kill participation, net worth and XP over time, and hero damage by target and source. Purchase history shows as unavailable until a mart supplies it.

## Install dependencies

Run these PowerShell commands from the repository root. The dashboard was verified with Python 3.14. Use your existing `.venv` if it has already been created.

```powershell
if (-not (Test-Path .\.venv\Scripts\python.exe)) { python -m venv .venv }
.\.venv\Scripts\python.exe -m pip install -r dashboard/requirements.txt
```

## Build the local fixture

Skip this section if you already have a DuckDB file with the marts built. The fixture builder recreates `output/dashboard-dev.duckdb` with one fictional match. dbt then builds and tests the marts in that file.

```powershell
New-Item -ItemType Directory -Force .\output | Out-Null
$env:DUCKDB_PATH = Join-Path $PWD "output\dashboard-dev.duckdb"
$env:DBT_TARGET_PATH = Join-Path $PWD "output\dbt-target"
$env:DBT_LOG_PATH = Join-Path $PWD "output\dbt-logs"

.\.venv\Scripts\python.exe -m pip install dbt-duckdb
.\.venv\Scripts\python.exe warehouse/make_dev_db.py $env:DUCKDB_PATH
.\.venv\Scripts\dbt.exe build --project-dir dbt --profiles-dir dbt
```

## Run the dashboard

From the repository root, set `DUCKDB_PATH` to the built file and start Streamlit in the same PowerShell window. If you built the database at a different location, use its absolute path instead.

```powershell
$env:DUCKDB_PATH = Join-Path $PWD "output\dashboard-dev.duckdb"
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Open the local URL printed by Streamlit. The fixture opens match `9000000001`. Select a hero to see the player page and use **Compare with** to add a second player. Keep the generated database and dbt output in the Git-ignored `output/` directory.

## Run the dashboard tests

Install the dashboard dependencies above, then run these commands from the repository root. The tests build their own temporary DuckDB fixture with dbt and exercise the player view with Streamlit's `AppTest`.

```powershell
.\.venv\Scripts\python.exe -m pip install pytest dbt-duckdb
.\.venv\Scripts\python.exe -m pytest tests/test_dashboard_db.py tests/test_dashboard_app.py
```
