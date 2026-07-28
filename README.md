# Self Station

Self Station is a self-hosted collaborative jukebox for a home server, LAN, studio, hackerspace, or small community room.

It runs a Python HTTP server, a static web UI, SQLite persistence, YouTube audio lookup through `yt-dlp`, live radio discovery through Radio Browser, and an FFmpeg relay for browser-friendly live radio playback.

## Features

- Shared music queue with server-synchronized playback state.
- YouTube search, queueing, playlist preview, and playlist import.
- Live radio search via the public Radio Browser catalog.
- Single FFmpeg relay process for the currently playing live radio station.
- Local play history and play counts stored in SQLite.
- Browser device names and active client presence.
- Docker Compose deployment with read-only container filesystem, dropped capabilities, and healthcheck.

## Quick Start

```bash
git clone https://github.com/lucasnc/self-station.git
cd self-station
docker compose up -d --build
```

Open:

```text
http://localhost:8090
```

Browsers need one initial click on the audio enable button because modern browsers block autoplay until the user interacts with the page.

## Requirements

For Docker deployment:

- Docker
- Docker Compose

For direct local execution:

- Python 3.11+
- FFmpeg available on `PATH`
- Node.js available on `PATH` for `yt-dlp` JavaScript challenges

## Local Development

```bash
python -m venv .venv
. .venv/bin/activate
pip install --pre -r requirements.txt
python server.py
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --pre -r requirements.txt
python server.py
```

## Configuration

Self Station is configured with environment variables.

| Variable                                   |                              Default | Description                                |
| ------------------------------------------ | -----------------------------------: | ------------------------------------------ |
| `SELF_STATION_HOST`                        |                            `0.0.0.0` | Bind address.                              |
| `SELF_STATION_PORT`                        |                               `8090` | HTTP port.                                 |
| `SELF_STATION_DB`                          |                  `./self_station.db` | SQLite database path.                      |
| `SELF_STATION_SEARCH_LIMIT`                |                                 `12` | Maximum YouTube search results.            |
| `SELF_STATION_PLAYLIST_IMPORT_LIMIT`       |                                `100` | Maximum playlist items imported per job.   |
| `SELF_STATION_BROWSER_URL`                 | `https://all.api.radio-browser.info` | Radio Browser API base URL.                |
| `SELF_STATION_LIVE_SEARCH_LIMIT`           |                                 `24` | Maximum live radio search results.         |
| `SELF_STATION_RELAY_BITRATE`               |                                `128` | MP3 bitrate for live radio relay, in kbps. |
| `SELF_STATION_FORCE_IPV4`                  |                                  `1` | Prefer IPv4 DNS resolution.                |
| `SELF_STATION_NODE_PATH`                   |                        auto-detected | Node.js binary used by `yt-dlp`.           |
| `SELF_STATION_STREAM_RESOLVE_WAIT_SECONDS` |                                 `30` | Timeout window for stream URL resolution.  |

## API

The web UI uses these endpoints:

- `GET /api/health`
- `GET /api/state`
- `GET /api/search?q=...`
- `GET /api/radio/search?q=...`
- `GET /api/playlist/preview?url=...`
- `GET /api/playlist/import/{job_id}`
- `GET /stream/{video_id}`
- `GET /relay/radio/{queue_id}`
- `POST /api/queue`
- `POST /api/radio/queue`
- `POST /api/queue/remove`
- `POST /api/playlist/import`
- `POST /api/control`
- `POST /api/settings`
- `POST /api/ended`

## Legal Notes

Self Station does not host or ship third-party media. You are responsible for how you use external services and media sources in your jurisdiction and environment.

YouTube access is provided through `yt-dlp`, and live radio discovery uses Radio Browser. Their availability and behavior may change independently of this project.

## License

MIT. See [LICENSE](LICENSE).
