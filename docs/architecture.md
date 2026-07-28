# Architecture

Self Station is a compact web application:

- `server.py` provides the HTTP API, static file serving, queue state, SQLite persistence, YouTube lookup, Radio Browser lookup, and live radio relay orchestration.
- `index.html` contains the browser UI and client-side playback logic.
- `compose.yml` and `Dockerfile` define the production container.

## Runtime Flow

1. Browsers poll `/api/state` for playback state, queue contents, active clients, and radio status.
2. Users add YouTube videos, playlists, or live radio stations through API calls.
3. Queue and playback state are persisted in SQLite.
4. YouTube audio URLs are resolved on demand through `yt-dlp`.
5. Live radio streams are received by one FFmpeg process and relayed to browser clients as MP3.
6. Playback position is anchored to server time so clients stay roughly synchronized.

## Persistence

The SQLite database stores:

- known tracks and live radio metadata;
- pending, playing, played, skipped, or errored queue items;
- playback state;
- application settings such as automatic radio mode.

Docker deployments should mount `/data` to a persistent host path.

## Trust Boundary

The app assumes a trusted audience. There is no built-in authentication, per-user permission model, or abuse protection beyond basic request validation and concurrency limits.
