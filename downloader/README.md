# Downloader

The downloader collects Dota 2 match data and replays while the Airflow pipeline is built. It runs on one machine and uploads files to Google Drive with `rclone`. It is separate from the pipeline: Drive is a stockpile, not a pipeline source.

## Routes

The downloader has two routes. Run them as separate processes.

- `steam` reads Valve's match feed (`GetMatchHistoryBySequenceNum`, 100 matches per call) forward from a saved cursor. It reads one page, skips the next three, and so covers every hour of the day. It moves faster than real time until it is 3 hours behind live games. It saves every page it reads, gzipped. It queues completed ranked All Pick matches for the replay route. Ranked All Pick matches with no duration yet go on a callback list and are fetched again later.
- `replays` draws queued matches at random from the newest day of the queue (matches at least a day old). It gets each replay URL from OpenDota and downloads the replay from Valve, 8 at a time. A download slower than 100 KB/s after a minute, or one that cannot connect, is retried 30 minutes later.

State lives in SQLite, so both routes resume where they stopped and never repeat work.

## Commands

Run these from the repo root.

```
python3 downloader/downloader.py check      # config only, no API calls
python3 downloader/downloader.py steam      # one pass of the Steam route
python3 downloader/downloader.py replays    # one pass of the replay route
python3 downloader/downloader.py status     # progress, no API calls
```

`steam` and `replays` stop after 55 minutes. Change this with `--max-minutes`. `steam` also takes `--days-back`, which sets where the first run starts (default 7 days).

## Configuration

The downloader reads environment variables, or a `.env` file at the repo root. Copy `.env.example` to `.env` and fill it in.

| Variable | Meaning |
| --- | --- |
| `STEAM_API_KEY` | Required. |
| `OPENDOTA_API_KEY` | Optional. With a key, every OpenDota call is billed (about US$0.0001 each). |
| `OPENDOTA_DAILY_CAP` | Hard cap on OpenDota calls per UTC day, with or without a key. Default 3000. |
| `DRIVE_DEST` | The `rclone` destination. Default `Google Drive:dota-pipeline/raw`. |
| `DOWNLOADER_HOME` | State, staging and logs. Default `~/.local/share/dota-pipeline`. |

Run `check` after you edit the config. It reports problems without calling any API.

## Output on Drive

- `steam/seq_{start_seq}.json.gz` is one Steam page: 100 consecutive matches, every mode, unfiltered.
- `steam/callback_{match_id}.json.gz` is a match fetched again after it finished.
- `replays/{match_id}.dem.zst` is one replay, compressed as Valve serves it.

`rclone move` deletes each local file after it uploads.

## Install the launchd jobs

On macOS, launchd runs each route every 10 minutes while the Mac is awake. It skips a try while a run is still going. Install both routes from the repo root. Each starts a first run at once.

```
mkdir -p ~/.local/share/dota-pipeline/logs
for route in steam replays; do
  sed -e "s|__REPO__|$PWD|g" -e "s|__HOME__|$HOME|g" -e "s|__ROUTE__|$route|g" \
    downloader/launchd/dota-downloader.plist.template > ~/Library/LaunchAgents/dota-$route.plist
  launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/dota-$route.plist
done
```

Stop them with:

```
launchctl bootout gui/$(id -u)/dota-steam
launchctl bootout gui/$(id -u)/dota-replays
```

The plists hold the absolute path to `downloader/downloader.py`. If you move the repo or the script, stop the jobs, regenerate the plists and bootstrap them again.

Launchd logs go to `~/.local/share/dota-pipeline/logs/launchd-steam.*.log` and `launchd-replays.*.log`. Each route also writes `steam.log` and `replays.log` in that directory.

## Limits

- Steam allows 100,000 calls a day. The downloader stops at 95,000 (`STEAM_DAILY_CAP`) to leave headroom.
- OpenDota's free tier allows 60 calls a minute and 3,000 calls a day. The downloader stops with 20 daily calls left.
- OpenDota does not document when its daily limit resets. It reset at 00:00 UTC on 2026-11-08 and 2026-11-09, but not by 03:23 UTC on 2026-11-10. When the free tier reports the day as spent, the downloader checks again every hour.
- The downloader checks free disk space and expects at least 20 GB (`MIN_FREE_GB`).

## Tests

```
python3 -m pytest downloader/tests
```
