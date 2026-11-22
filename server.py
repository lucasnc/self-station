#!/usr/bin/env python3
"""Self Station with SQLite, synchronized playback, and a server-side radio relay."""

from __future__ import annotations

import json
import html
import hashlib
import ipaddress
import mimetypes
import os
import queue as queue_module
import re
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import tempfile
import uuid
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError


BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
APP_CSS_FILE = BASE_DIR / "app.css"
VIDEO_FILE = BASE_DIR / "gradient-motion.webm"
DB_FILE = Path(os.environ.get("SELF_STATION_DB", BASE_DIR / "self_station.db"))
HOST = os.environ.get("SELF_STATION_HOST", "0.0.0.0")
PORT = int(os.environ.get("SELF_STATION_PORT", "8090"))
SEARCH_LIMIT = max(1, min(int(os.environ.get("SELF_STATION_SEARCH_LIMIT", "12")), 25))
PODCAST_SEARCH_LIMIT = max(1, min(int(os.environ.get("SELF_STATION_PODCAST_SEARCH_LIMIT", "12")), 25))
PODCAST_EPISODE_LIMIT = 25
LIVE_RADIO_SEARCH_LIMIT = max(
    1, min(int(os.environ.get("SELF_STATION_LIVE_SEARCH_LIMIT", "24")), 50)
)
RADIO_BROWSER_BASE_URL = os.environ.get(
    "SELF_STATION_BROWSER_URL", "https://all.api.radio-browser.info"
).rstrip("/")
RADIO_BROWSER_USER_AGENT = "self-station/2.0"
LYRICS_BASE_URL = os.environ.get("SELF_STATION_LYRICS_URL", "https://lrclib.net").rstrip("/")
LYRICS_USER_AGENT = "self-station/2.0"
LASTFM_API_URL = "https://ws.audioscrobbler.com/2.0/"
LASTFM_API_KEY = os.environ.get("SELF_STATION_LASTFM_API_KEY", "").strip()
LASTFM_USER_AGENT = "self-station/2.0"
MUSICBRAINZ_API_URL = os.environ.get(
    "SELF_STATION_MUSICBRAINZ_URL", "https://musicbrainz.org/ws/2"
).rstrip("/")
COVER_ART_ARCHIVE_URL = os.environ.get(
    "SELF_STATION_COVER_ART_ARCHIVE_URL", "https://coverartarchive.org"
).rstrip("/")
MUSICBRAINZ_USER_AGENT = os.environ.get(
    "SELF_STATION_MUSICBRAINZ_USER_AGENT",
    "self-station/2.0 (self-hosted metadata resolver)",
).strip()
RADIO_RELAY_BITRATE = max(
    64, min(int(os.environ.get("SELF_STATION_RELAY_BITRATE", "128")), 320)
)
ACOUSTID_API_URL = "https://api.acoustid.org/v2/lookup"
ACOUSTID_CLIENT = os.environ.get("SELF_STATION_ACOUSTID_CLIENT", "").strip()
RADIO_RECOGNITION_COOLDOWN = max(
    10, min(int(os.environ.get("SELF_STATION_RECOGNITION_COOLDOWN", "30")), 300)
)
PLAYLIST_IMPORT_LIMIT = max(
    1, int(os.environ.get("SELF_STATION_PLAYLIST_IMPORT_LIMIT", "1000000"))
)
SYNC_GRACE_SECONDS = 1.5
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
PLAYLIST_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,100}$")
RADIO_STATION_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
PODCAST_ID_RE = re.compile(r"^podcast:[0-9a-f]{24}$")
FORCE_IPV4 = os.environ.get("SELF_STATION_FORCE_IPV4", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
STREAM_RESOLVE_WAIT_SECONDS = max(
    5.0, float(os.environ.get("SELF_STATION_STREAM_RESOLVE_WAIT_SECONDS", "30"))
)
NODE_PATH = os.environ.get("SELF_STATION_NODE_PATH") or shutil.which("node")

SYSTEM_GETADDRINFO = socket.getaddrinfo


def ipv4_preferred_getaddrinfo(
    host: str | bytes | None,
    port: str | int | None,
    family: int = socket.AF_UNSPEC,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[Any, ...]]:
    """Avoid advertised IPv6 when no working route is available."""
    if family == socket.AF_UNSPEC:
        family = socket.AF_INET
    return SYSTEM_GETADDRINFO(host, port, family, type, proto, flags)


if FORCE_IPV4:
    socket.getaddrinfo = ipv4_preferred_getaddrinfo

DB_LOCK = threading.RLock()
CACHE_LOCK = threading.RLock()
STREAM_RESOLVE_LOCK = threading.RLock()
CLIENT_LOCK = threading.RLock()
RADIO_LOCK = threading.RLock()
RADIO_RECOGNITION_LOCK = threading.Lock()
PLAYLIST_LOCK = threading.RLock()
RECOMMENDATION_SEMAPHORE = threading.BoundedSemaphore(1)
SEARCH_SEMAPHORE = threading.BoundedSemaphore(2)
MUSICBRAINZ_SEMAPHORE = threading.BoundedSemaphore(1)
PLAYLIST_EXTRACT_SEMAPHORE = threading.BoundedSemaphore(1)
PLAYLIST_IMPORT_SEMAPHORE = threading.BoundedSemaphore(1)
STOP_EVENT = threading.Event()

SEARCH_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
LYRICS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
LIVE_RADIO_SEARCH_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
PODCAST_SEARCH_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
PODCAST_EPISODE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
LASTFM_CACHE: dict[str, tuple[float, Any]] = {}
MUSIC_METADATA_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
STREAM_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
RECOMMENDATION_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
CLIENTS: dict[str, dict[str, Any]] = {}
PLAYLIST_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
PLAYLIST_JOBS: dict[str, dict[str, Any]] = {}
RADIO_STATE: dict[str, Any] = {
    "searching": False,
    "last_error": "",
    "last_added_at": None,
    "next_attempt_at": 0.0,
}
RADIO_RECOGNITION_CACHE: dict[int, tuple[float, dict[str, Any]]] = {}


def youtube_dl_options(**overrides: Any) -> dict[str, Any]:
    options: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "ignoreconfig": True,
        "extractor_retries": 1,
    }
    if FORCE_IPV4:
        options["source_address"] = "0.0.0.0"
    if NODE_PATH:
        options["js_runtimes"] = {"node": {"path": NODE_PATH}}
    options.update(overrides)
    return options


def db_connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_FILE, timeout=15)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 15000")
    return connection


def init_db() -> None:
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DB_LOCK, db_connect() as db:
        db.execute("PRAGMA journal_mode = WAL")
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS tracks (
                video_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                uploader TEXT NOT NULL DEFAULT '',
                duration REAL NOT NULL DEFAULT 0,
                view_count INTEGER,
                local_plays INTEGER NOT NULL DEFAULT 0,
                last_played_at REAL
            );

            CREATE TABLE IF NOT EXISTS favorites (
                video_id TEXT PRIMARY KEY REFERENCES tracks(video_id) ON DELETE CASCADE,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT NOT NULL REFERENCES tracks(video_id),
                title TEXT NOT NULL,
                uploader TEXT NOT NULL DEFAULT '',
                duration REAL NOT NULL DEFAULT 0,
                view_count INTEGER,
                added_by TEXT NOT NULL,
                added_client_id TEXT NOT NULL,
                added_at REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                started_at REAL,
                finished_at REAL,
                error_message TEXT
            );

            CREATE INDEX IF NOT EXISTS ix_queue_status_id ON queue(status, id);

            CREATE TABLE IF NOT EXISTS playback (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                status TEXT NOT NULL DEFAULT 'idle',
                current_queue_id INTEGER REFERENCES queue(id),
                base_position REAL NOT NULL DEFAULT 0,
                anchor_started_at REAL,
                revision INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        track_columns = {
            str(row["name"])
            for row in db.execute("PRAGMA table_info(tracks)").fetchall()
        }
        migrations = {
            "media_type": "TEXT NOT NULL DEFAULT 'youtube'",
            "station_uuid": "TEXT",
            "stream_url": "TEXT",
            "thumbnail": "TEXT NOT NULL DEFAULT ''",
            "country": "TEXT NOT NULL DEFAULT ''",
            "state": "TEXT NOT NULL DEFAULT ''",
            "tags": "TEXT NOT NULL DEFAULT ''",
            "codec": "TEXT NOT NULL DEFAULT ''",
            "bitrate": "INTEGER",
        }
        for column, definition in migrations.items():
            if column not in track_columns:
                db.execute(f"ALTER TABLE tracks ADD COLUMN {column} {definition}")
        db.execute(
            "CREATE INDEX IF NOT EXISTS ix_tracks_media_type ON tracks(media_type)"
        )
        db.execute(
            """
            INSERT OR IGNORE INTO playback
                (id, status, current_queue_id, base_position, anchor_started_at, revision, updated_at)
            VALUES (1, 'idle', NULL, 0, NULL, 0, ?)
            """,
            (time.time(),),
        )
        db.execute(
            """
            INSERT OR IGNORE INTO app_settings(key, value, updated_at)
            VALUES ('auto_radio', '0', ?)
            """,
            (time.time(),),
        )
        db.commit()


def bool_setting(db: sqlite3.Connection, key: str, default: bool = False) -> bool:
    row = db.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    if row is None:
        return default
    return str(row["value"]).strip().lower() in {"1", "true", "yes", "on"}


def set_auto_radio(enabled: bool) -> None:
    now = time.time()
    with DB_LOCK, db_connect() as db:
        db.execute(
            """
            INSERT INTO app_settings(key, value, updated_at)
            VALUES ('auto_radio', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            ("1" if enabled else "0", now),
        )
        if not enabled:
            db.execute(
                "DELETE FROM queue WHERE status = 'pending' AND added_client_id = 'auto-radio'"
            )
        db.commit()
    with RADIO_LOCK:
        RADIO_STATE["last_error"] = ""
        RADIO_STATE["next_attempt_at"] = 0.0


def clean_text(value: Any, limit: int, fallback: str = "") -> str:
    text = " ".join(str(value or "").split())
    return (text[:limit] or fallback).strip()


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return default


def as_int_or_none(value: Any) -> int | None:
    try:
        number = int(value)
        return max(0, number)
    except (TypeError, ValueError):
        return None


def playback_position(playback: sqlite3.Row, now: float) -> float:
    position = as_float(playback["base_position"])
    if playback["status"] == "playing" and playback["anchor_started_at"] is not None:
        position += max(0.0, now - float(playback["anchor_started_at"]))
    return position


def start_next(db: sqlite3.Connection, now: float) -> None:
    next_item = db.execute(
        """
        SELECT * FROM queue
         WHERE status = 'pending'
         ORDER BY CASE WHEN added_client_id = 'auto-radio' THEN 1 ELSE 0 END, id
         LIMIT 1
        """
    ).fetchone()
    if not next_item:
        db.execute(
            """
            UPDATE playback
               SET status = 'idle', current_queue_id = NULL, base_position = 0,
                   anchor_started_at = NULL, revision = revision + 1, updated_at = ?
             WHERE id = 1
            """,
            (now,),
        )
        return

    db.execute(
        "UPDATE queue SET status = 'playing', started_at = ?, error_message = NULL WHERE id = ?",
        (now, next_item["id"]),
    )
    db.execute(
        """
        UPDATE tracks
           SET local_plays = local_plays + 1, last_played_at = ?
         WHERE video_id = ?
        """,
        (now, next_item["video_id"]),
    )
    db.execute(
        """
        UPDATE playback
           SET status = 'playing', current_queue_id = ?, base_position = 0,
               anchor_started_at = ?, revision = revision + 1, updated_at = ?
         WHERE id = 1
        """,
        (next_item["id"], now, now),
    )


def finish_current(db: sqlite3.Connection, now: float, status: str) -> None:
    playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
    if playback and playback["current_queue_id"] is not None:
        db.execute(
            """
            UPDATE queue
               SET status = ?, finished_at = ?
             WHERE id = ? AND status = 'playing'
            """,
            (status, now, playback["current_queue_id"]),
        )
    start_next(db, now)


def advance_if_finished(db: sqlite3.Connection, now: float) -> None:
    playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
    if not playback or playback["status"] != "playing" or playback["current_queue_id"] is None:
        return
    current = db.execute(
        "SELECT duration FROM queue WHERE id = ?", (playback["current_queue_id"],)
    ).fetchone()
    if not current:
        start_next(db, now)
        return
    duration = as_float(current["duration"])
    if duration > 0 and playback_position(playback, now) >= duration + SYNC_GRACE_SECONDS:
        finish_current(db, now, "played")


def row_to_item(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = set(row.keys())
    return {
        "id": row["id"],
        "video_id": row["video_id"],
        "title": row["title"],
        "uploader": row["uploader"],
        "duration": row["duration"],
        "view_count": row["view_count"],
        "added_by": row["added_by"],
        "added_at": row["added_at"],
        "status": row["status"],
        "local_plays": row["local_plays"],
        "media_type": row["media_type"] if "media_type" in keys else "youtube",
        "station_uuid": row["station_uuid"] if "station_uuid" in keys else None,
        "stream_url": row["stream_url"] if "stream_url" in keys else None,
        "thumbnail": row["thumbnail"] if "thumbnail" in keys else "",
        "country": row["country"] if "country" in keys else "",
        "state": row["state"] if "state" in keys else "",
        "tags": row["tags"] if "tags" in keys else "",
        "codec": row["codec"] if "codec" in keys else "",
        "bitrate": row["bitrate"] if "bitrate" in keys else None,
    }


def favorite_items(db: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = db.execute(
        """
        SELECT f.video_id, f.created_at AS favorited_at,
               t.title, t.uploader, t.duration, t.view_count, t.local_plays,
               t.media_type, t.station_uuid, t.thumbnail, t.country, t.state,
               t.tags, t.codec, t.bitrate, t.stream_url
          FROM favorites f
          JOIN tracks t ON t.video_id = f.video_id
         ORDER BY f.created_at DESC, f.video_id
        """
    ).fetchall()
    return [
        {
            "video_id": row["video_id"],
            "title": row["title"],
            "uploader": row["uploader"],
            "duration": row["duration"],
            "view_count": row["view_count"],
            "local_plays": row["local_plays"],
            "media_type": row["media_type"],
            "station_uuid": row["station_uuid"],
            "stream_url": row["stream_url"],
            "thumbnail": row["thumbnail"],
            "country": row["country"],
            "state": row["state"],
            "tags": row["tags"],
            "codec": row["codec"],
            "bitrate": row["bitrate"],
            "favorited_at": row["favorited_at"],
        }
        for row in rows
    ]


def paginated_favorite_items(
    db: sqlite3.Connection, page: int = 1, limit: int = 25
) -> dict[str, Any]:
    page = max(1, min(int(page or 1), 10_000))
    limit = max(1, min(int(limit or 25), 50))
    offset = (page - 1) * limit
    rows = db.execute(
        """
        SELECT f.video_id, f.created_at AS favorited_at,
               t.title, t.uploader, t.duration, t.view_count, t.local_plays,
               t.media_type, t.station_uuid, t.thumbnail, t.country, t.state,
               t.tags, t.codec, t.bitrate, t.stream_url
          FROM favorites f
          JOIN tracks t ON t.video_id = f.video_id
         ORDER BY f.created_at DESC, f.video_id
         LIMIT ? OFFSET ?
        """,
        (limit + 1, offset),
    ).fetchall()
    items = [
        {
            "video_id": row["video_id"],
            "title": row["title"],
            "uploader": row["uploader"],
            "duration": row["duration"],
            "view_count": row["view_count"],
            "local_plays": row["local_plays"],
            "media_type": row["media_type"],
            "station_uuid": row["station_uuid"],
            "stream_url": row["stream_url"],
            "thumbnail": row["thumbnail"],
            "country": row["country"],
            "state": row["state"],
            "tags": row["tags"],
            "codec": row["codec"],
            "bitrate": row["bitrate"],
            "favorited_at": row["favorited_at"],
        }
        for row in rows[:limit]
    ]
    return {
        "results": items,
        "page": page,
        "limit": limit,
        "has_more": len(rows) > limit,
    }


def paginated_recent_items(
    db: sqlite3.Connection, page: int = 1, limit: int = 25
) -> dict[str, Any]:
    page = max(1, min(int(page or 1), 10_000))
    limit = max(1, min(int(limit or 25), 50))
    offset = (page - 1) * limit
    rows = db.execute(
        """
        SELECT q.id, q.video_id, q.title, q.uploader, q.duration,
               q.view_count, q.added_by, q.added_at, q.started_at, q.status,
               t.local_plays, t.media_type, t.station_uuid,
               t.thumbnail, t.country, t.state, t.tags, t.codec, t.bitrate
          FROM queue q
          JOIN tracks t ON t.video_id = q.video_id
         WHERE q.started_at IS NOT NULL
         ORDER BY q.started_at DESC, q.id DESC
         LIMIT ? OFFSET ?
        """,
        (limit + 1, offset),
    ).fetchall()
    return {
        "results": [
            {**(row_to_item(row) or {}), "started_at": row["started_at"]}
            for row in rows[:limit]
        ],
        "page": page,
        "limit": limit,
        "has_more": len(rows) > limit,
    }


def toggle_favorite(video_id: Any) -> dict[str, Any]:
    video_id = clean_text(video_id, 100)
    if not (VIDEO_ID_RE.fullmatch(video_id) or video_id.startswith("radio:") or PODCAST_ID_RE.fullmatch(video_id)):
        raise ValueError("Invalid track")
    with DB_LOCK, db_connect() as db:
        track = db.execute(
            "SELECT video_id FROM tracks WHERE video_id = ?", (video_id,)
        ).fetchone()
        if track is None:
            raise ValueError("Track not found")
        existing = db.execute(
            "SELECT 1 FROM favorites WHERE video_id = ?", (video_id,)
        ).fetchone()
        if existing:
            db.execute("DELETE FROM favorites WHERE video_id = ?", (video_id,))
            favorited = False
        else:
            db.execute(
                "INSERT INTO favorites(video_id, created_at) VALUES (?, ?)",
                (video_id, time.time()),
            )
            favorited = True
        db.commit()
    return {"video_id": video_id, "favorited": favorited}


def register_client(client_id: str, device_name: str, source_ip: str = "") -> None:
    if not client_id:
        return
    now = time.time()
    key = client_id[:80]
    with CLIENT_LOCK:
        existing = CLIENTS.get(key)
        CLIENTS[key] = {
            "id": key,
            "name": clean_text(device_name, 40, "Browser"),
            "ip": clean_text(source_ip, 120, "unknown"),
            "connected_at": existing.get("connected_at", now) if existing else now,
            "last_seen": now,
        }
        stale = [key for key, value in CLIENTS.items() if now - value["last_seen"] > 25]
        for key in stale:
            CLIENTS.pop(key, None)


def active_clients() -> list[dict[str, Any]]:
    now = time.time()
    with CLIENT_LOCK:
        return [
            {
                "id": value["id"],
                "name": value["name"],
                "ip": value["ip"],
                "connected_at": value["connected_at"],
            }
            for value in CLIENTS.values()
            if now - value["last_seen"] <= 25
        ]


def get_state(client_id: str = "", device_name: str = "", source_ip: str = "") -> dict[str, Any]:
    register_client(client_id, device_name, source_ip)
    now = time.time()
    with DB_LOCK, db_connect() as db:
        advance_if_finished(db, now)
        db.commit()
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        current_row = None
        if playback and playback["current_queue_id"] is not None:
            current_row = db.execute(
                """
                SELECT q.*, t.local_plays, t.media_type, t.station_uuid,
                       t.thumbnail, t.country, t.state, t.tags, t.codec, t.bitrate
                  FROM queue q JOIN tracks t ON t.video_id = q.video_id
                 WHERE q.id = ?
                """,
                (playback["current_queue_id"],),
            ).fetchone()
        queue_rows = db.execute(
            """
            SELECT q.*, t.local_plays, t.media_type, t.station_uuid,
                   t.thumbnail, t.country, t.state, t.tags, t.codec, t.bitrate
              FROM queue q JOIN tracks t ON t.video_id = q.video_id
             WHERE q.status = 'pending'
             ORDER BY CASE WHEN q.added_client_id = 'auto-radio' THEN 1 ELSE 0 END, q.id
             LIMIT 200
            """
        ).fetchall()
        current = row_to_item(current_row)
        if current:
            current["favorite"] = db.execute(
                "SELECT 1 FROM favorites WHERE video_id = ? LIMIT 1",
                (current["video_id"],),
            ).fetchone() is not None
        if current and current["media_type"] == "radio":
            radio_title = LIVE_RADIO_RELAY.current_title(int(current["id"]))
            if radio_title:
                current["radio_now_playing"] = radio_title
        position = playback_position(playback, now) if playback else 0.0
        if current and current["duration"]:
            position = min(position, float(current["duration"]))
        auto_radio = bool_setting(db, "auto_radio")
        with RADIO_LOCK:
            radio_status = {
                "searching": bool(RADIO_STATE["searching"]),
                "last_error": RADIO_STATE["last_error"],
                "last_added_at": RADIO_STATE["last_added_at"],
            }
        recent_revision = db.execute(
            "SELECT COUNT(*) AS count, MAX(id) AS last_id FROM queue WHERE started_at IS NOT NULL"
        ).fetchone()
        favorite_revision = db.execute(
            "SELECT COUNT(*) AS count, MAX(created_at) AS last_added_at FROM favorites"
        ).fetchone()
        return {
            "server_time": now,
            "status": playback["status"] if playback else "idle",
            "position": position,
            "revision": playback["revision"] if playback else 0,
            "current": current,
            "queue": [row_to_item(row) for row in queue_rows],
            "auto_radio": auto_radio,
            "radio": radio_status,
            # Kept as empty arrays for older clients; paginated tabs load from
            # their own endpoints instead of receiving full lists every 750 ms.
            "recent_tracks": [],
            "favorites": [],
            "list_revisions": {
                "recent": f"{recent_revision['count']}:{recent_revision['last_id'] or 0}",
                "favorites": f"{favorite_revision['count']}:{favorite_revision['last_added_at'] or 0}",
            },
            "clients": active_clients(),
        }


def add_to_queue(payload: dict[str, Any], radio_seed_id: str | None = None) -> dict[str, Any]:
    video_id = clean_text(payload.get("video_id"), 20)
    if not VIDEO_ID_RE.fullmatch(video_id):
        raise ValueError("Invalid video ID")
    title = clean_text(payload.get("title"), 240, "Video do YouTube")
    uploader = clean_text(payload.get("uploader"), 160)
    duration = as_float(payload.get("duration"))
    view_count = as_int_or_none(payload.get("view_count"))
    added_by = clean_text(payload.get("device_name"), 40, "Browser")
    client_id = clean_text(payload.get("client_id"), 80, "unknown")
    now = time.time()

    with DB_LOCK, db_connect() as db:
        if radio_seed_id is not None:
            if not bool_setting(db, "auto_radio"):
                return {"queue_id": None, "added": False}
            if db.execute("SELECT 1 FROM queue WHERE status = 'pending' LIMIT 1").fetchone():
                return {"queue_id": None, "added": False}
            playback = db.execute("SELECT current_queue_id FROM playback WHERE id = 1").fetchone()
            active_seed = None
            if playback and playback["current_queue_id"] is not None:
                row = db.execute(
                    "SELECT video_id FROM queue WHERE id = ?",
                    (playback["current_queue_id"],),
                ).fetchone()
                active_seed = row["video_id"] if row else None
            else:
                row = db.execute(
                    """
                    SELECT video_id FROM queue
                     WHERE started_at IS NOT NULL
                     ORDER BY started_at DESC, id DESC LIMIT 1
                    """
                ).fetchone()
                active_seed = row["video_id"] if row else None
            if active_seed != radio_seed_id:
                return {"queue_id": None, "added": False}
        db.execute(
            """
            INSERT INTO tracks(video_id, title, uploader, duration, view_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                title = excluded.title,
                uploader = CASE WHEN excluded.uploader <> '' THEN excluded.uploader ELSE tracks.uploader END,
                duration = CASE WHEN excluded.duration > 0 THEN excluded.duration ELSE tracks.duration END,
                view_count = COALESCE(excluded.view_count, tracks.view_count)
            """,
            (video_id, title, uploader, duration, view_count),
        )
        cursor = db.execute(
            """
            INSERT INTO queue(
                video_id, title, uploader, duration, view_count,
                added_by, added_client_id, added_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (video_id, title, uploader, duration, view_count, added_by, client_id, now),
        )
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if playback and playback["status"] == "idle" and playback["current_queue_id"] is None:
            start_next(db, now)
        db.commit()
        return {"queue_id": cursor.lastrowid, "added": True}


def remove_from_queue(queue_id: int) -> dict[str, Any]:
    if queue_id <= 0:
        raise ValueError("Invalid queue item")
    now = time.time()
    with DB_LOCK, db_connect() as db:
        row = db.execute("SELECT status FROM queue WHERE id = ?", (queue_id,)).fetchone()
        if row is None:
            raise ValueError("Track not found in queue")
        if row["status"] != "pending":
            raise ValueError("Only pending tracks can be removed")
        cursor = db.execute(
            """
            UPDATE queue
               SET status = 'removed', finished_at = ?, error_message = 'Removida da fila'
             WHERE id = ? AND status = 'pending'
            """,
            (now, queue_id),
        )
        db.commit()
        if cursor.rowcount != 1:
            raise ValueError("The track is no longer in the queue")
        return {"removed": True, "queue_id": queue_id}


def clear_queue() -> dict[str, Any]:
    """Remove all pending tracks while preserving the current track and history."""
    now = time.time()
    with DB_LOCK, db_connect() as db:
        cursor = db.execute(
            """
            UPDATE queue
               SET status = 'removed', finished_at = ?, error_message = 'Fila limpa'
             WHERE status = 'pending'
            """,
            (now,),
        )
        db.commit()
        return {"cleared": True, "removed_count": cursor.rowcount}


def move_queue_item(queue_id: int, direction: str) -> dict[str, Any]:
    if queue_id <= 0:
        raise ValueError("Invalid queue item")
    if direction not in {"up", "down"}:
        raise ValueError("Invalid move direction")
    with DB_LOCK, db_connect() as db:
        row = db.execute(
            "SELECT id, status FROM queue WHERE id = ?",
            (queue_id,),
        ).fetchone()
        if row is None:
            raise ValueError("Track not found in queue")
        if row["status"] != "pending":
            raise ValueError("Only pending tracks can be moved")

        pending_ids = [
            int(item["id"])
            for item in db.execute(
                """
                SELECT id FROM queue
                 WHERE status = 'pending'
                 ORDER BY CASE WHEN added_client_id = 'auto-radio' THEN 1 ELSE 0 END, id
                """
            ).fetchall()
        ]
        current_index = pending_ids.index(queue_id)
        target_index = current_index - 1 if direction == "up" else current_index + 1
        if target_index < 0 or target_index >= len(pending_ids):
            return {"moved": False, "queue_id": queue_id}

        target_id = pending_ids[target_index]
        temporary_id = -max(queue_id, target_id) - 1
        db.execute("UPDATE queue SET id = ? WHERE id = ?", (temporary_id, queue_id))
        db.execute("UPDATE queue SET id = ? WHERE id = ?", (queue_id, target_id))
        db.execute("UPDATE queue SET id = ? WHERE id = ?", (target_id, temporary_id))
        db.commit()
        return {"moved": True, "queue_id": queue_id, "direction": direction}


def control_playback(action: str) -> None:
    now = time.time()
    with DB_LOCK, db_connect() as db:
        advance_if_finished(db, now)
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if action == "pause" and playback and playback["status"] == "playing":
            position = playback_position(playback, now)
            db.execute(
                """
                UPDATE playback
                   SET status = 'paused', base_position = ?, anchor_started_at = NULL,
                       revision = revision + 1, updated_at = ?
                 WHERE id = 1
                """,
                (position, now),
            )
        elif action == "play" and playback:
            if playback["status"] == "paused" and playback["current_queue_id"] is not None:
                db.execute(
                    """
                    UPDATE playback
                       SET status = 'playing', anchor_started_at = ?,
                           revision = revision + 1, updated_at = ?
                     WHERE id = 1
                    """,
                    (now, now),
                )
            elif playback["status"] == "idle":
                start_next(db, now)
        elif action == "skip":
            finish_current(db, now, "skipped")
        else:
            if action not in {"play", "pause", "skip"}:
                raise ValueError("Invalid playback command")
        db.commit()
    if action in {"pause", "skip"}:
        LIVE_RADIO_RELAY.stop()


def handle_ended(queue_id: int) -> None:
    now = time.time()
    with DB_LOCK, db_connect() as db:
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if not playback or playback["current_queue_id"] != queue_id:
            return
        current = db.execute(
            """
            SELECT q.duration, t.media_type
              FROM queue q JOIN tracks t ON t.video_id = q.video_id
             WHERE q.id = ?
            """,
            (queue_id,),
        ).fetchone()
        if current and current["media_type"] == "radio":
            return
        duration = as_float(current["duration"]) if current else 0.0
        position = playback_position(playback, now)
        if duration <= 0 or position >= max(0.0, duration - 3.0):
            finish_current(db, now, "played")
            db.commit()


def search_youtube(
    query: str, page: int = 1, limit: int = SEARCH_LIMIT, page_size: int | None = None
) -> list[dict[str, Any]]:
    query = clean_text(query, 120)
    if len(query) < 2:
        raise ValueError("Enter at least 2 characters")
    page = max(1, min(int(page or 1), 100))
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_LIMIT + 1))
    page_size = max(1, min(int(page_size or limit), SEARCH_LIMIT))
    start = (page - 1) * page_size + 1
    end = start + limit - 1
    cache_key = f"{query.casefold()}:{page}:{limit}:{page_size}"
    now = time.time()
    with CACHE_LOCK:
        cached = SEARCH_CACHE.get(cache_key)
        if cached and now - cached[0] < 600:
            return cached[1]

    if not SEARCH_SEMAPHORE.acquire(blocking=False):
        raise RuntimeError("Search is busy. Try again shortly")
    try:
        options = youtube_dl_options(
            extract_flat=True,
            skip_download=True,
            playliststart=start,
            playlistend=end,
            socket_timeout=12,
            retries=1,
        )
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"ytsearch{end}:{query}", download=False)
        results: list[dict[str, Any]] = []
        seen_video_ids: set[str] = set()
        for entry in (info or {}).get("entries") or []:
            if not entry:
                continue
            video_id = clean_text(entry.get("id"), 20)
            if not VIDEO_ID_RE.fullmatch(video_id) or video_id in seen_video_ids:
                continue
            seen_video_ids.add(video_id)
            results.append(
                {
                    "video_id": video_id,
                    "title": clean_text(entry.get("title"), 240, "Video do YouTube"),
                    "uploader": clean_text(
                        entry.get("channel") or entry.get("uploader"), 160
                    ),
                    "duration": as_float(entry.get("duration")),
                    "view_count": as_int_or_none(entry.get("view_count")),
                    "thumbnail": f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
                }
            )
        with CACHE_LOCK:
            SEARCH_CACHE[cache_key] = (now, results)
        return results
    finally:
        SEARCH_SEMAPHORE.release()


def search_youtube_page(
    query: str, page: int = 1, limit: int = SEARCH_LIMIT
) -> dict[str, Any]:
    page = max(1, min(int(page or 1), 100))
    limit = max(1, min(int(limit or SEARCH_LIMIT), SEARCH_LIMIT))
    results = search_youtube(query, page, limit + 1, page_size=limit)
    return {
        "results": results[:limit],
        "page": page,
        "limit": limit,
        "has_more": page < 100 and len(results) > limit,
    }


def lastfm_request(method: str, parameters: dict[str, Any], ttl: int = 600) -> dict[str, Any]:
    if not LASTFM_API_KEY:
        raise RuntimeError("Last.fm is not configured")
    cache_key = json.dumps([method, parameters], ensure_ascii=False, sort_keys=True)
    now = time.time()
    with CACHE_LOCK:
        cached = LASTFM_CACHE.get(cache_key)
        if cached and now - cached[0] < ttl:
            return cached[1]
    query = {"method": method, "api_key": LASTFM_API_KEY, "format": "json", **parameters}
    request = urllib.request.Request(
        f"{LASTFM_API_URL}?{urllib.parse.urlencode(query)}",
        headers={"Accept": "application/json", "User-Agent": LASTFM_USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(2 * 1024 * 1024 + 1)
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("Last.fm is unavailable") from exc
    if len(body) > 2 * 1024 * 1024:
        raise RuntimeError("Unexpected response from Last.fm")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid response from Last.fm") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid response from Last.fm")
    if payload.get("error"):
        raise RuntimeError(clean_text(payload.get("message"), 180, "Last.fm rejected the request"))
    with CACHE_LOCK:
        LASTFM_CACHE[cache_key] = (now, payload)
    return payload


def strip_track_decorations(title: str, artist: str = "") -> str:
    value = clean_text(title, 240)
    clean_artist = clean_text(artist, 160)
    if clean_artist:
        value = re.sub(
            rf"^{re.escape(clean_artist)}\s*[\-–—|:]\s*",
            "",
            value,
            flags=re.IGNORECASE,
        )
    decoration = re.compile(
        r"\s*(?:\([^)]*\b(?:official|music\s+video|audio|lyrics?|visualizer|"
        r"remaster(?:ed)?|live(?:\s+version)?|topic|vevo)\b[^)]*\)"
        r"|\[[^\]]*\b(?:official|music\s+video|audio|lyrics?|visualizer|"
        r"remaster(?:ed)?|live(?:\s+version)?|topic|vevo)\b[^\]]*\])\s*$",
        flags=re.IGNORECASE,
    )
    trailing_label = re.compile(
        r"\s*[-|]\s*(?:official\s+(?:\d{3,4}p|[248]k\s+)?(?:music\s+)?video|"
        r"official\s+audio|lyrics?|audio|visualizer|remaster(?:ed)?|live)\s*$",
        flags=re.IGNORECASE,
    )
    previous = ""
    while value and value != previous:
        previous = value
        value = decoration.sub("", value).strip()
        value = trailing_label.sub("", value).strip()
    return value or clean_text(title, 240)


def music_identity(title: str, artist: str = "") -> tuple[str, str]:
    clean_title = clean_text(title, 240)
    clean_artist = clean_text(artist, 160)
    clean_artist = re.sub(r"\s+(?:-\s*)?(?:topic|vevo|official)$", "", clean_artist, flags=re.IGNORECASE).strip()
    if clean_artist and clean_artist.casefold() not in {"youtube", "video do youtube", "music"}:
        return strip_track_decorations(clean_title, clean_artist), clean_artist
    match = re.match(r"^(.+?)\s+[\-–—|]\s+(.+)$", clean_title)
    if match:
        parsed_artist = match.group(1).strip()
        return strip_track_decorations(match.group(2), parsed_artist), parsed_artist
    return strip_track_decorations(clean_title), ""


def musicbrainz_request(query: str, limit: int = 5) -> dict[str, Any]:
    params = {
        "query": query,
        "fmt": "json",
        "limit": max(1, min(int(limit), 10)),
    }
    request = urllib.request.Request(
        f"{MUSICBRAINZ_API_URL}/recording?{urllib.parse.urlencode(params)}",
        headers={
            "Accept": "application/json",
            "User-Agent": MUSICBRAINZ_USER_AGENT,
        },
    )
    with MUSICBRAINZ_SEMAPHORE:
        try:
            with urllib.request.urlopen(request, timeout=12) as response:
                body = response.read(2 * 1024 * 1024 + 1)
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError("MusicBrainz is unavailable") from exc
    if len(body) > 2 * 1024 * 1024:
        raise RuntimeError("Unexpected response from MusicBrainz")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid response from MusicBrainz") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid response from MusicBrainz")
    return payload


def musicbrainz_artist_credit(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    names: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = clean_text(
            (item.get("artist") or {}).get("name") or item.get("name"),
            160,
        )
        if name and name not in names:
            names.append(name)
        joinphrase = clean_text(item.get("joinphrase"), 20)
        if joinphrase and names:
            names[-1] += joinphrase
    return "".join(names).strip()


def resolve_track_metadata(title: str, artist: str = "") -> dict[str, Any]:
    raw_title, raw_artist = music_identity(title, artist)
    cache_key = f"{raw_title.casefold()}|{raw_artist.casefold()}"
    now = time.time()
    with CACHE_LOCK:
        cached = MUSIC_METADATA_CACHE.get(cache_key)
        if cached and now - cached[0] < 86400:
            return cached[1]
    empty = {
        "found": False,
        "title": raw_title,
        "artist": raw_artist,
        "album": "",
        "recording_mbid": "",
        "release_mbid": "",
        "release_group_mbid": "",
        "cover_art": "",
        "source": "heuristic",
    }
    if len(raw_title) < 2:
        return empty
    query_title = raw_title.replace('"', "")
    query_artist = raw_artist.replace('"', "")
    clauses = [f'recording:"{query_title}"']
    if raw_artist:
        clauses.append(f'artist:"{query_artist}"')
    try:
        payload = musicbrainz_request(" AND ".join(clauses))
    except (RuntimeError, ValueError):
        with CACHE_LOCK:
            MUSIC_METADATA_CACHE[cache_key] = (now, empty)
        return empty
    recordings = payload.get("recordings") or []
    best: dict[str, Any] | None = None
    best_score = -1
    for candidate in recordings:
        if not isinstance(candidate, dict):
            continue
        score = as_int_or_none(candidate.get("score")) or 0
        candidate_title = clean_text(candidate.get("title"), 240)
        candidate_artist = musicbrainz_artist_credit(candidate.get("artist-credit"))
        if not candidate_title:
            continue
        if raw_artist and candidate_artist and candidate_artist.casefold() == raw_artist.casefold():
            score += 20
        if candidate_title.casefold() == raw_title.casefold():
            score += 20
        if score > best_score:
            best, best_score = candidate, score
    if not best:
        with CACHE_LOCK:
            MUSIC_METADATA_CACHE[cache_key] = (now, empty)
        return empty
    releases = best.get("releases") or []
    release = releases[0] if isinstance(releases, list) and releases and isinstance(releases[0], dict) else {}
    release_group = release.get("release-group") or {}
    release_mbid = clean_text(release.get("id"), 64)
    release_group_mbid = clean_text(release_group.get("id"), 64)
    cover_id = release_group_mbid or release_mbid
    cover_art = f"{COVER_ART_ARCHIVE_URL}/release-group/{cover_id}/front-500" if release_group_mbid else (
        f"{COVER_ART_ARCHIVE_URL}/release/{cover_id}/front-500" if release_mbid else ""
    )
    result = {
        "found": True,
        "title": clean_text(best.get("title"), 240, raw_title),
        "artist": musicbrainz_artist_credit(best.get("artist-credit")) or raw_artist,
        "album": clean_text(release.get("title"), 240),
        "recording_mbid": clean_text(best.get("id"), 64),
        "release_mbid": release_mbid,
        "release_group_mbid": release_group_mbid,
        "cover_art": cover_art,
        "source": "MusicBrainz + Cover Art Archive",
        "score": best_score,
    }
    with CACHE_LOCK:
        MUSIC_METADATA_CACHE[cache_key] = (now, result)
    return result


def lastfm_image(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    for item in reversed(value):
        if isinstance(item, dict) and item.get("#text"):
            return clean_text(item.get("#text"), 2048)
    return ""


def lastfm_context(title: str, artist: str = "") -> dict[str, Any]:
    metadata = resolve_track_metadata(title, artist)
    track_title = metadata.get("title") or music_identity(title, artist)[0]
    track_artist = metadata.get("artist") or music_identity(title, artist)[1]
    if len(track_title) < 2:
        raise ValueError("Invalid track")
    candidates = [track_title]
    raw_title = clean_text(title, 240)
    if raw_title not in candidates:
        candidates.append(raw_title)
    track: dict[str, Any] = {}
    for candidate in candidates:
        payload = lastfm_request(
            "track.getInfo",
            {
                "track": candidate,
                **({"artist": track_artist} if track_artist else {}),
                "autocorrect": 1,
            },
            ttl=1800,
        )
        track = payload.get("track") or {}
        wiki = track.get("wiki") or {}
        if track and (wiki.get("summary") or wiki.get("content")):
            break
    artist_data = track.get("artist") or {}
    wiki = track.get("wiki") or {}
    return {
        "found": bool(track),
        "title": clean_text(track.get("name"), 240, track_title),
        "artist": clean_text(artist_data.get("name") or track_artist, 160),
        "album": clean_text((track.get("album") or {}).get("title"), 240),
        "listeners": as_int_or_none(track.get("listeners")),
        "playcount": as_int_or_none(track.get("playcount")),
        "summary": clean_text(wiki.get("summary"), 4000),
        "content": clean_text(wiki.get("content"), 8000),
        "url": clean_text(track.get("url"), 2048),
        "image": lastfm_image(track.get("image")) or metadata.get("cover_art", ""),
        "cover_art": metadata.get("cover_art", ""),
        "musicbrainz": metadata,
        "source": "Last.fm",
    }


def lastfm_similar(title: str, artist: str = "", limit: int = 25) -> list[dict[str, Any]]:
    metadata = resolve_track_metadata(title, artist)
    track_title = metadata.get("title") or music_identity(title, artist)[0]
    track_artist = metadata.get("artist") or music_identity(title, artist)[1]
    if len(track_title) < 2 or not track_artist:
        return []
    payload = lastfm_request(
        "track.getSimilar",
        {"track": track_title, "artist": track_artist, "limit": max(1, min(limit, 25))},
        ttl=300,
    )
    results: list[dict[str, Any]] = []
    for item in (payload.get("similartracks") or {}).get("track") or []:
        if not isinstance(item, dict):
            continue
        artist_data = item.get("artist") or {}
        results.append({
            "title": clean_text(item.get("name"), 240),
            "artist": clean_text(artist_data.get("name"), 160),
            "match": as_float(item.get("match")),
            "url": clean_text(item.get("url"), 2048),
            "image": lastfm_image(item.get("image")),
        })
    return results


def lastfm_discover(
    tag: str = "rock",
    page: int = 1,
    seed_title: str = "",
    seed_artist: str = "",
) -> dict[str, Any]:
    page = max(1, min(int(page or 1), 50))
    payload = lastfm_request(
        "chart.getTopTracks",
        {"limit": 25, "page": page},
        ttl=300,
    )
    tracks = (payload.get("tracks") or {}).get("track") or []
    results: list[dict[str, Any]] = []
    for item in tracks[:25]:
        if not isinstance(item, dict):
            continue
        artist_data = item.get("artist") or {}
        results.append({
            "title": clean_text(item.get("name"), 240),
            "artist": clean_text(artist_data.get("name"), 160),
            "listeners": as_int_or_none(item.get("listeners")),
            "url": clean_text(item.get("url"), 2048),
            "image": lastfm_image(item.get("image")),
            "source": "daily",
        })
    return {
        "tag": "daily",
        "page": page,
        "has_more": page < 50 and len(results) == 25,
        "seed": None,
        "mixed": False,
        "results": results[:25],
    }


ITUNES_API_URL = "https://itunes.apple.com"
PODCAST_USER_AGENT = "self-station/2.0 podcast-search"
PODCAST_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"


def itunes_request(path: str, parameters: dict[str, Any]) -> dict[str, Any]:
    query = urllib.parse.urlencode(parameters)
    request = urllib.request.Request(
        f"{ITUNES_API_URL}/{path.lstrip('/')}?{query}",
        headers={"Accept": "application/json", "User-Agent": PODCAST_USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(4 * 1024 * 1024 + 1)
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("The podcast catalog is unavailable") from exc
    if len(body) > 4 * 1024 * 1024:
        raise RuntimeError("Unexpected response from the podcast catalog")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid response from the podcast catalog") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid response from the podcast catalog")
    return payload


def search_podcasts(query: str) -> list[dict[str, Any]]:
    query = clean_text(query, 120)
    if len(query) < 2:
        raise ValueError("Enter at least 2 characters")
    cache_key = query.casefold()
    now = time.time()
    with CACHE_LOCK:
        cached = PODCAST_SEARCH_CACHE.get(cache_key)
        if cached and now - cached[0] < 900:
            return cached[1]
    payload = itunes_request(
        "/search",
        {
            "media": "podcast",
            "entity": "podcast",
            "term": query,
            "country": "BR",
            "limit": PODCAST_SEARCH_LIMIT,
        },
    )
    results: list[dict[str, Any]] = []
    for entry in payload.get("results") or []:
        if not isinstance(entry, dict) or not entry.get("collectionId"):
            continue
        results.append(
            {
                "podcast_id": int(entry["collectionId"]),
                "title": clean_text(entry.get("collectionName"), 240, "Untitled podcast"),
                "uploader": clean_text(entry.get("artistName"), 160, "Podcast"),
                "thumbnail": clean_text(entry.get("artworkUrl600") or entry.get("artworkUrl100"), 2048),
                "feed_url": clean_text(entry.get("feedUrl"), 2048),
                "episode_count": as_int_or_none(entry.get("trackCount")),
                "genre": clean_text(entry.get("primaryGenreName"), 120),
                "country": clean_text(entry.get("country"), 80),
            }
        )
    with CACHE_LOCK:
        PODCAST_SEARCH_CACHE[cache_key] = (now, results)
    return results


def xml_local_name(tag: Any) -> str:
    return str(tag or "").rsplit("}", 1)[-1].casefold()


def xml_child(parent: ET.Element, name: str) -> ET.Element | None:
    for child in list(parent):
        if xml_local_name(child.tag) == name.casefold():
            return child
    return None


def xml_value(parent: ET.Element, name: str, limit: int = 2048) -> str:
    child = xml_child(parent, name)
    return clean_text(child.text if child is not None else "", limit)


def parse_podcast_duration(value: Any) -> float:
    text = clean_text(value, 40)
    if not text:
        return 0.0
    try:
        parts = [int(part) for part in text.split(":")]
    except ValueError:
        return as_float(text)
    if len(parts) == 3:
        return float(parts[0] * 3600 + parts[1] * 60 + parts[2])
    if len(parts) == 2:
        return float(parts[0] * 60 + parts[1])
    return float(parts[0]) if len(parts) == 1 else 0.0


def podcast_episodes(collection_id: Any) -> dict[str, Any]:
    try:
        collection_id = int(collection_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid podcast") from exc
    if collection_id <= 0:
        raise ValueError("Invalid podcast")
    cache_key = str(collection_id)
    now = time.time()
    with CACHE_LOCK:
        cached = PODCAST_EPISODE_CACHE.get(cache_key)
        if cached and now - cached[0] < 900:
            return cached[1]
    lookup = itunes_request("/lookup", {"id": collection_id, "entity": "podcast"})
    podcast = next(
        (item for item in lookup.get("results") or [] if isinstance(item, dict) and item.get("kind") == "podcast"),
        None,
    )
    if not podcast:
        raise ValueError("Podcast not found")
    feed_url = clean_text(podcast.get("feedUrl"), 2048)
    if not feed_url:
        raise ValueError("This podcast has no available RSS feed")
    feed_url = public_stream_url(feed_url)
    request = urllib.request.Request(
        feed_url,
        headers={"Accept": "application/rss+xml, application/xml, text/xml", "User-Agent": PODCAST_USER_AGENT},
    )
    podcast_title = clean_text(podcast.get("collectionName"), 240, "Podcast")
    author = clean_text(podcast.get("artistName"), 160, "Podcast")
    thumbnail = clean_text(podcast.get("artworkUrl600"), 2048)
    episodes: list[dict[str, Any]] = []
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            for _, item in ET.iterparse(response, events=("end",)):
                if xml_local_name(item.tag) != "item":
                    continue
                enclosure = xml_child(item, "enclosure")
                stream_url = clean_text(enclosure.attrib.get("url") if enclosure is not None else "", 2048)
                if stream_url:
                    try:
                        stream_url = public_stream_url(stream_url)
                    except ValueError:
                        stream_url = ""
                if stream_url:
                    title = xml_value(item, "title", 240) or "Untitled episode"
                    guid = xml_value(item, "guid", 500) or stream_url
                    episode_id = "podcast:" + hashlib.sha256(f"{collection_id}:{guid}".encode("utf-8")).hexdigest()[:24]
                    image = xml_child(item, "image")
                    episode_thumbnail = thumbnail
                    if image is not None:
                        episode_thumbnail = clean_text(image.attrib.get("href") or image.attrib.get("url"), 2048) or episode_thumbnail
                    duration = parse_podcast_duration(xml_value(item, "duration", 40))
                    published = xml_value(item, "pubDate", 120)
                    episodes.append(
                        {
                            "episode_id": episode_id,
                            "title": title,
                            "uploader": author or podcast_title,
                            "podcast_title": podcast_title,
                            "duration": duration,
                            "thumbnail": episode_thumbnail,
                            "stream_url": stream_url,
                            "published_at": published,
                            "media_type": "podcast",
                        }
                    )
                item.clear()
                if len(episodes) >= PODCAST_EPISODE_LIMIT:
                    break
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("Could not read the podcast RSS feed") from exc
    except ET.ParseError as exc:
        raise RuntimeError("Invalid podcast RSS feed") from exc
    result = {
        "podcast_id": collection_id,
        "title": podcast_title,
        "uploader": author,
        "thumbnail": thumbnail,
        "feed_url": feed_url,
        "episodes": episodes,
        "count": len(episodes),
    }
    with CACHE_LOCK:
        PODCAST_EPISODE_CACHE[cache_key] = (now, result)
    return result


def add_podcast_episode_to_queue(payload: dict[str, Any]) -> dict[str, Any]:
    video_id = clean_text(payload.get("episode_id"), 40).lower()
    if not PODCAST_ID_RE.fullmatch(video_id):
        raise ValueError("Invalid podcast episode")
    stream_url = public_stream_url(payload.get("stream_url"))
    title = clean_text(payload.get("title"), 240, "Episodio de podcast")
    uploader = clean_text(payload.get("uploader"), 160, "Podcast")
    duration = as_float(payload.get("duration"))
    thumbnail = clean_text(payload.get("thumbnail"), 2048)
    added_by = clean_text(payload.get("device_name"), 40, "Browser")
    client_id = clean_text(payload.get("client_id"), 80, "unknown")
    now = time.time()
    with DB_LOCK, db_connect() as db:
        db.execute(
            """
            INSERT INTO tracks(video_id, title, uploader, duration, view_count, media_type, stream_url, thumbnail)
            VALUES (?, ?, ?, ?, NULL, 'podcast', ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                title = excluded.title,
                uploader = excluded.uploader,
                duration = excluded.duration,
                media_type = 'podcast',
                stream_url = excluded.stream_url,
                thumbnail = excluded.thumbnail
            """,
            (video_id, title, uploader, duration, stream_url, thumbnail),
        )
        cursor = db.execute(
            """
            INSERT INTO queue(video_id, title, uploader, duration, view_count, added_by, added_client_id, added_at, status)
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?, 'pending')
            """,
            (video_id, title, uploader, duration, added_by, client_id, now),
        )
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if playback and playback["status"] == "idle" and playback["current_queue_id"] is None:
            start_next(db, now)
        db.commit()
        return {"queue_id": cursor.lastrowid, "added": True}


def normalize_lyrics(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    return text[:24000]


def strip_lyric_timestamps(value: Any) -> str:
    text = normalize_lyrics(value)
    return re.sub(r"\[\d{1,3}:\d{2}(?:[.:]\d{1,3})?\]\s*", "", text).strip()


def lyrics_query_candidates(title: str, artist: str = "") -> list[tuple[str, str]]:
    raw_title = clean_text(title, 240)
    raw_artist = clean_text(artist, 160)
    if len(raw_title) < 2:
        raise ValueError("A song title is required")

    identity_title, identity_artist = music_identity(raw_title, raw_artist)
    cleaned_title = identity_title.strip(" -–—")
    preferred_artist = identity_artist or raw_artist
    candidates: list[tuple[str, str]] = []

    def add_candidate(track: str, candidate_artist: str) -> None:
        track = clean_text(track, 240)
        candidate_artist = clean_text(candidate_artist, 160)
        candidate = (track, candidate_artist)
        if len(track) >= 2 and candidate not in candidates:
            candidates.append(candidate)

    embedded = re.match(
        r"^\s*(?P<artist>[^:–—-]{2,80})\s*(?::|\s+[–—-]\s+)\s*(?P<title>.+?)\s*$",
        cleaned_title,
    )
    if embedded:
        add_candidate(embedded.group("title"), embedded.group("artist"))
        add_candidate(embedded.group("title"), preferred_artist)
        cleaned_title = embedded.group("title").strip()
    add_candidate(cleaned_title, preferred_artist)
    add_candidate(cleaned_title, "")
    add_candidate(raw_title, raw_artist)
    return candidates


def fetch_lyrics_candidate(title: str, artist: str) -> dict[str, Any]:
    cache_key = f"{title.casefold()}|{artist.casefold()}"
    now = time.time()
    with CACHE_LOCK:
        cached = LYRICS_CACHE.get(cache_key)
        if cached and now - cached[0] < 3600:
            return cached[1]

    parameters = {"track_name": title}
    if artist:
        parameters["artist_name"] = artist
    url = f"{LYRICS_BASE_URL}/api/get?{urllib.parse.urlencode(parameters)}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": LYRICS_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            body = response.read(2 * 1024 * 1024 + 1)
    except urllib.error.HTTPError as exc:
        if exc.code == HTTPStatus.NOT_FOUND:
            result = {"found": False, "lyrics": "", "source": "LRCLIB"}
            with CACHE_LOCK:
                LYRICS_CACHE[cache_key] = (now, result)
            return result
        raise RuntimeError("The lyrics service is unavailable") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("The lyrics service is unavailable") from exc

    if len(body) > 2 * 1024 * 1024:
        raise RuntimeError("The lyrics response was too large")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("The lyrics response was invalid") from exc

    lyrics = normalize_lyrics(payload.get("plainLyrics"))
    if not lyrics:
        lyrics = strip_lyric_timestamps(payload.get("syncedLyrics"))
    result = {
        "found": bool(lyrics),
        "lyrics": lyrics,
        "source": "LRCLIB",
        "title": clean_text(payload.get("trackName"), 240, title),
        "artist": clean_text(payload.get("artistName"), 160, artist),
    }
    with CACHE_LOCK:
        LYRICS_CACHE[cache_key] = (now, result)
    return result


def lyrics_lookup(title: str, artist: str = "") -> dict[str, Any]:
    metadata = resolve_track_metadata(title, artist)
    candidates: list[tuple[str, str]] = []
    normalized_title = clean_text(metadata.get("title"), 240)
    normalized_artist = clean_text(metadata.get("artist"), 160)
    if normalized_title:
        candidates.append((normalized_title, normalized_artist))
    for candidate in lyrics_query_candidates(title, artist):
        if candidate not in candidates:
            candidates.append(candidate)
    request_key = f"{candidates[0][0].casefold()}|{candidates[0][1].casefold()}"
    last_result: dict[str, Any] = {
        "found": False,
        "lyrics": "",
        "source": "LRCLIB",
        "metadata": metadata,
    }
    for candidate_title, candidate_artist in candidates:
        result = fetch_lyrics_candidate(candidate_title, candidate_artist)
        result["metadata"] = metadata
        last_result = result
        if result.get("found"):
            with CACHE_LOCK:
                LYRICS_CACHE[request_key] = (time.time(), result)
            return result
    return last_result


def radio_browser_request(path: str, parameters: dict[str, Any] | None = None) -> Any:
    query = urllib.parse.urlencode(parameters or {})
    url = f"{RADIO_BROWSER_BASE_URL}/{path.lstrip('/')}"
    if query:
        url = f"{url}?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": RADIO_BROWSER_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(4 * 1024 * 1024 + 1)
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("The radio catalog is unavailable") from exc
    if len(body) > 4 * 1024 * 1024:
        raise RuntimeError("Unexpected response from the radio catalog")
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Invalid response from the radio catalog") from exc


def public_stream_url(value: Any) -> str:
    url = clean_text(value, 2048)
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError as exc:
        raise ValueError("Invalid station URL") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("This station has no compatible HTTP stream")
    if parsed.username or parsed.password:
        raise ValueError("Radio streams with embedded credentials are not allowed")
    try:
        addresses = socket.getaddrinfo(
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise ValueError("Could not resolve the station host") from exc
    if not addresses:
        raise ValueError("Could not resolve the station host")
    for address in addresses:
        host = str(address[4][0]).split("%", 1)[0]
        try:
            address_ip = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError("Invalid radio stream address") from exc
        if not address_ip.is_global:
            raise ValueError("The radio stream points to a private network")
    return url


def normalize_live_radio(entry: dict[str, Any]) -> dict[str, Any] | None:
    station_uuid = clean_text(entry.get("stationuuid"), 36)
    stream_url = clean_text(entry.get("url_resolved") or entry.get("url"), 2048)
    try:
        parsed = urllib.parse.urlparse(stream_url)
    except ValueError:
        return None
    if (
        not RADIO_STATION_ID_RE.fullmatch(station_uuid)
        or parsed.scheme not in {"http", "https"}
        or not parsed.hostname
    ):
        return None
    return {
        "station_uuid": station_uuid.lower(),
        "name": clean_text(entry.get("name"), 240, "Unnamed station"),
        "stream_url": stream_url,
        "homepage": clean_text(entry.get("homepage"), 1024),
        "favicon": clean_text(entry.get("favicon"), 1024),
        "country": clean_text(entry.get("country"), 120),
        "countrycode": clean_text(entry.get("countrycode"), 2).upper(),
        "state": clean_text(entry.get("state"), 120),
        "language": clean_text(entry.get("language"), 120),
        "tags": clean_text(entry.get("tags"), 240),
        "codec": clean_text(entry.get("codec"), 40),
        "bitrate": as_int_or_none(entry.get("bitrate")),
        "votes": as_int_or_none(entry.get("votes")),
        "click_count": as_int_or_none(entry.get("clickcount")),
    }


def search_live_radios(query: str) -> list[dict[str, Any]]:
    query = clean_text(query, 100)
    if len(query) < 2:
        raise ValueError("Enter at least 2 characters")
    cache_key = query.casefold()
    now = time.time()
    with CACHE_LOCK:
        cached = LIVE_RADIO_SEARCH_CACHE.get(cache_key)
        if cached and now - cached[0] < 900:
            return cached[1]
    payload = radio_browser_request(
        "/json/stations/search",
        {
            "name": query,
            "hidebroken": "true",
            "order": "clickcount",
            "reverse": "true",
            "limit": LIVE_RADIO_SEARCH_LIMIT,
        },
    )
    if not isinstance(payload, list):
        raise RuntimeError("Invalid response from the radio catalog")
    results = [station for entry in payload if isinstance(entry, dict) if (station := normalize_live_radio(entry))]
    with CACHE_LOCK:
        LIVE_RADIO_SEARCH_CACHE[cache_key] = (now, results)
    return results


def get_live_radio_station(station_uuid: Any) -> dict[str, Any]:
    station_uuid = clean_text(station_uuid, 36).lower()
    if not RADIO_STATION_ID_RE.fullmatch(station_uuid):
        raise ValueError("Invalid radio station")
    payload = radio_browser_request(f"/json/stations/byuuid/{station_uuid}")
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise ValueError("Radio station not found")
    station = normalize_live_radio(payload[0])
    if station is None:
        raise ValueError("This station has no compatible stream")
    station["stream_url"] = public_stream_url(station["stream_url"])
    return station


def add_live_radio_to_queue(payload: dict[str, Any]) -> dict[str, Any]:
    station = get_live_radio_station(payload.get("station_uuid"))
    station_uuid = station["station_uuid"]
    media_id = f"radio:{station_uuid}"
    added_by = clean_text(payload.get("device_name"), 40, "Browser")
    client_id = clean_text(payload.get("client_id"), 80, "unknown")
    location = " / ".join(
        value for value in (station["state"], station["country"]) if value
    )
    now = time.time()
    with DB_LOCK, db_connect() as db:
        db.execute(
            """
            INSERT INTO tracks(
                video_id, title, uploader, duration, view_count,
                media_type, station_uuid, stream_url, thumbnail,
                country, state, tags, codec, bitrate
            ) VALUES (?, ?, ?, 0, ?, 'radio', ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                title = excluded.title,
                uploader = excluded.uploader,
                view_count = excluded.view_count,
                media_type = 'radio',
                station_uuid = excluded.station_uuid,
                stream_url = excluded.stream_url,
                thumbnail = excluded.thumbnail,
                country = excluded.country,
                state = excluded.state,
                tags = excluded.tags,
                codec = excluded.codec,
                bitrate = excluded.bitrate
            """,
            (
                media_id,
                station["name"],
                location or "Radio online",
                station["click_count"],
                station_uuid,
                station["stream_url"],
                station["favicon"],
                station["country"],
                station["state"],
                station["tags"],
                station["codec"],
                station["bitrate"],
            ),
        )
        cursor = db.execute(
            """
            INSERT INTO queue(
                video_id, title, uploader, duration, view_count,
                added_by, added_client_id, added_at, status
            ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, 'pending')
            """,
            (
                media_id,
                station["name"],
                location or "Radio online",
                station["click_count"],
                added_by,
                client_id,
                now,
            ),
        )
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if playback and playback["status"] == "idle" and playback["current_queue_id"] is None:
            start_next(db, now)
        db.commit()
        return {"queue_id": cursor.lastrowid, "added": True, "station": station}


def playlist_limit(value: Any, default: int = 25) -> int:
    try:
        return max(1, min(int(value), PLAYLIST_IMPORT_LIMIT))
    except (TypeError, ValueError):
        return min(default, PLAYLIST_IMPORT_LIMIT)


def normalize_playlist_url(value: Any) -> tuple[str, str, str]:
    raw_url = clean_text(value, 2048)
    spotify_uri = re.fullmatch(r"spotify:playlist:([A-Za-z0-9]+)", raw_url, re.I)
    if spotify_uri:
        playlist_id = spotify_uri.group(1)
        return (
            "spotify",
            playlist_id,
            f"https://open.spotify.com/embed/playlist/{playlist_id}",
        )

    try:
        parsed = urllib.parse.urlparse(raw_url)
    except ValueError as exc:
        raise ValueError("Invalid playlist URL") from exc
    host = (parsed.hostname or "").casefold().removeprefix("www.")

    if host == "open.spotify.com":
        match = re.fullmatch(r"/(?:embed/)?playlist/([A-Za-z0-9]+)/*", parsed.path)
        if not match:
            raise ValueError("Use a Spotify playlist URL")
        playlist_id = match.group(1)
        return (
            "spotify",
            playlist_id,
            f"https://open.spotify.com/embed/playlist/{playlist_id}",
        )

    if host in {"youtube.com", "m.youtube.com", "music.youtube.com", "youtu.be"}:
        query = urllib.parse.parse_qs(parsed.query)
        playlist_id = clean_text(query.get("list", [""])[0], 100)
        if not PLAYLIST_ID_RE.fullmatch(playlist_id):
            raise ValueError("The YouTube URL does not contain a valid playlist")
        video_id = clean_text(query.get("v", [""])[0], 20)
        if playlist_id.startswith("RD") and VIDEO_ID_RE.fullmatch(video_id):
            normalized = (
                "https://www.youtube.com/watch?"
                + urllib.parse.urlencode({"v": video_id, "list": playlist_id})
            )
        else:
            normalized = "https://www.youtube.com/playlist?" + urllib.parse.urlencode(
                {"list": playlist_id}
            )
        return "youtube", playlist_id, normalized

    raise ValueError("Paste a YouTube or Spotify playlist URL")


SPOTIFY_CLIENT_ID = "f6a40776580943a7bc5173125a1e8832"
SPOTIFY_CLIENT_VERSION = "1.2.90.116.g2f7f1c48"
SPOTIFY_PLAYLIST_QUERY_HASH = "908a5597b4d0af0489a9ad6a2d41bc3b416ff47c0884016d92bbd6822d0eb6d8"


def spotify_pathfinder_playlist(playlist_id: str) -> dict[str, Any]:
    token_payload = {
        "client_data": {
            "client_version": SPOTIFY_CLIENT_VERSION,
            "client_id": SPOTIFY_CLIENT_ID,
            "js_sdk_data": {
                "device_brand": "unknown", "device_model": "unknown", "os": "web",
                "os_version": "", "container_version": "", "device_id": "",
                "device_type": "computer", "platform_identifier": "web_player",
            },
        }
    }
    token_request = urllib.request.Request(
        "https://clienttoken.spotify.com/v1/clienttoken",
        data=json.dumps(token_payload).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(token_request, timeout=18) as response:
        token_data = json.loads(response.read().decode("utf-8", "replace"))
    token = ((token_data.get("granted_token") or {}).get("token") or "").strip()
    if not token:
        raise RuntimeError("Spotify did not provide a client token")

    tracks: list[dict[str, Any]] = []
    title = "Playlist do Spotify"
    cover = ""
    offset = 0
    page_size = 100
    total_count: int | None = None
    max_tracks = 2000
    while offset < max_tracks and (total_count is None or offset < total_count):
        payload = {
            "variables": {"uri": f"spotify:playlist:{playlist_id}", "limit": page_size, "offset": offset},
            "operationName": "queryPlaylist",
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": SPOTIFY_PLAYLIST_QUERY_HASH}},
        }
        request = urllib.request.Request(
            "https://api-partner.spotify.com/pathfinder/v2/query",
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json", "Accept": "application/json",
                "User-Agent": "Mozilla/5.0", "client-token": token,
                "Spotify-App-Version": SPOTIFY_CLIENT_VERSION,
            },
        )
        with urllib.request.urlopen(request, timeout=22) as response:
            result = json.loads(response.read().decode("utf-8", "replace"))
        entity = ((result.get("data") or {}).get("playlistV2") or {})
        if entity.get("__typename") in {"NotFound", "GenericError"}:
            raise ValueError("The Spotify playlist is unavailable through Pathfinder")
        title = clean_text(entity.get("name"), 240, title)
        sources = ((entity.get("coverArt") or {}).get("sources") or [])
        if sources and isinstance(sources[0], dict):
            cover = clean_text(sources[0].get("url"), 1000, cover)
        content = entity.get("content") or {}
        items = content.get("items") if isinstance(content, dict) else None
        if not isinstance(items, list):
            break
        if isinstance(content, dict):
            for key in ("totalCount", "total", "itemCount"):
                candidate_total = content.get(key)
                if isinstance(candidate_total, int) and candidate_total >= 0:
                    total_count = candidate_total
                    break
        page_tracks = []
        for item in items:
            track = item.get("track") if isinstance(item, dict) else None
            if not isinstance(track, dict):
                continue
            track_title = clean_text(track.get("name") or track.get("title"), 240)
            if not track_title:
                continue
            artists = track.get("artists") or track.get("artist") or []
            if isinstance(artists, list):
                artist_name = ", ".join(clean_text((a or {}).get("name"), 100) for a in artists if isinstance(a, dict))
            else:
                artist_name = clean_text(artists.get("name"), 100) if isinstance(artists, dict) else ""
            page_tracks.append({
                "source_id": clean_text(track.get("uri") or track.get("id"), 120),
                "title": track_title, "uploader": artist_name or "Spotify",
                "duration": as_float(track.get("duration_ms") or track.get("duration")) / (1000 if track.get("duration_ms") else 1),
                "available": True,
            })
        if not page_tracks:
            break
        tracks.extend(page_tracks)
        if total_count is not None and len(tracks) >= total_count:
            break
        if len(items) < page_size:
            break
        offset += page_size
    if not tracks:
        raise ValueError("Spotify returned no tracks for this playlist")
    truncated = bool(total_count is not None and len(tracks) < total_count)
    if total_count is not None and total_count > max_tracks:
        truncated = True
    return {"source": "spotify", "title": title, "url": f"https://open.spotify.com/playlist/{playlist_id}", "cover": cover, "tracks": tracks, "count": len(tracks), "truncated": truncated, "total_count": total_count or len(tracks)}


def spotify_playlist_preview(url: str) -> dict[str, Any]:
    playlist_id = re.search(r"/playlist/([A-Za-z0-9]+)", url)
    if playlist_id:
        try:
            return spotify_pathfinder_playlist(playlist_id.group(1))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError, RuntimeError, json.JSONDecodeError):
            pass
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
                "Chrome/124.0 Mobile Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=18) as response:
            page = response.read(3_000_000).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403, 404}:
            raise ValueError("The Spotify playlist is private or unavailable") from exc
        raise RuntimeError("Could not reach Spotify") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("Could not reach Spotify") from exc

    match = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        page,
        re.I | re.S,
    )
    if not match:
        raise ValueError("The Spotify playlist is private, empty, or unavailable")
    try:
        next_data = json.loads(match.group(1))
        entity = next_data["props"]["pageProps"]["state"]["data"]["entity"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Could not identify the tracks in this playlist") from exc

    track_list = entity.get("trackList") if isinstance(entity, dict) else None
    if not isinstance(track_list, list):
        raise ValueError("The Spotify playlist is private, empty, or unavailable")

    tracks: list[dict[str, Any]] = []
    for entry in track_list:
        if not isinstance(entry, dict) or entry.get("entityType") != "track":
            continue
        title = clean_text(entry.get("title"), 240)
        if not title:
            continue
        tracks.append(
            {
                "source_id": clean_text(entry.get("uri"), 120),
                "title": title,
                "uploader": clean_text(entry.get("subtitle"), 160),
                "duration": as_float(entry.get("duration")) / 1000,
                "available": bool(entry.get("isPlayable", True)),
            }
        )

    if not tracks:
        raise ValueError("The Spotify playlist is empty, private, or unavailable")

    cover = ""
    cover_sources = ((entity.get("coverArt") or {}).get("sources") or [])
    if cover_sources and isinstance(cover_sources[0], dict):
        cover = clean_text(cover_sources[0].get("url"), 1000)
    return {
        "source": "spotify",
        "title": clean_text(entity.get("name"), 240, "Playlist do Spotify"),
        "url": url,
        "cover": cover,
        "tracks": tracks,
        "count": len(tracks),
        "truncated": False,
    }


def spotify_profile_playlists(profile_id: str) -> dict[str, Any]:
    profile_id = clean_text(profile_id, 120)
    profile_match = re.fullmatch(r"https?://open\.spotify\.com/user/([A-Za-z0-9._-]{1,120})/?(?:\?.*)?", profile_id, re.I)
    if profile_match:
        profile_id = profile_match.group(1)
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,120}", profile_id):
        raise ValueError("Invalid Spotify profile ID")
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.8"}
    page = ""
    for profile_url in (
        f"https://open.spotify.com/user/{urllib.parse.quote(profile_id)}",
        f"https://open.spotify.com/user/{urllib.parse.quote(profile_id)}/playlists",
    ):
        try:
            request = urllib.request.Request(profile_url, headers=headers)
            with urllib.request.urlopen(request, timeout=18) as response:
                candidate = response.read(5_000_000).decode("utf-8", "replace")
            if len(candidate) > len(page):
                page = candidate
        except (urllib.error.URLError, TimeoutError):
            continue
    playlists: list[dict[str, Any]] = []
    seen: set[str] = set()
    for match in re.finditer(r"(?:https://open\.spotify\.com)?/playlist/([A-Za-z0-9]+)", page):
        playlist_id = match.group(1)
        if playlist_id in seen:
            continue
        seen.add(playlist_id)
        card_end = page.find("</a>", match.end())
        card = page[match.start():card_end + 4 if card_end >= 0 else match.end() + 1800]
        image_match = re.search(r'(?i)<img[^>]+(?:src|srcset)=[\"\']([^\"\']+)', card)
        image = html.unescape(image_match.group(1)).split(" ")[0] if image_match else ""
        name_match = re.search(r'(?i)(?:aria-label|title)=[\"\']([^\"\']{2,160})', card)
        title = html.unescape(name_match.group(1)).strip() if name_match else "Playlist do Spotify"
        if title in {"", "Playlist do Spotify"}:
            text_values = [
                html.unescape(value).strip()
                for value in re.findall(r">([^<>]{2,160})<", card)
                if html.unescape(value).strip().lower() not in {"likes", "followers", "de lucas", "de spotify"}
            ]
            if text_values:
                title = text_values[0]
        playlist_item = {
            "id": playlist_id,
            "title": title,
            "url": f"https://open.spotify.com/playlist/{playlist_id}",
            "image": image,
            "count": None,
            "source": "Spotify",
        }
        try:
            metadata = spotify_playlist_preview(playlist_item["url"])
            playlist_item["title"] = metadata.get("title") or playlist_item["title"]
            playlist_item["image"] = metadata.get("cover") or playlist_item["image"]
            playlist_item["count"] = metadata.get("count")
        except (RuntimeError, ValueError):
            pass
        playlists.append(playlist_item)
    if not playlists:
        raise ValueError("No public playlists found for this profile")
    return {"profile_id": profile_id, "results": playlists, "page": 1, "has_more": False}


def youtube_playlist_preview(url: str) -> dict[str, Any]:
    options = youtube_dl_options(
        extract_flat=True,
        skip_download=True,
        lazy_playlist=False,
        socket_timeout=15,
        retries=1,
    )
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=False)
    entries = (info or {}).get("entries") or []
    tracks: list[dict[str, Any]] = []
    for entry in entries:
        if not entry:
            continue
        video_id = clean_text(entry.get("id"), 20)
        if not VIDEO_ID_RE.fullmatch(video_id):
            continue
        tracks.append(
            {
                "video_id": video_id,
                "title": clean_text(entry.get("title"), 240, "Video do YouTube"),
                "uploader": clean_text(entry.get("channel") or entry.get("uploader"), 160),
                "duration": as_float(entry.get("duration")),
                "view_count": as_int_or_none(entry.get("view_count")),
                "thumbnail": f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg",
                "available": True,
            }
        )
    if not tracks:
        raise ValueError("The YouTube playlist is empty, private, or unavailable")
    return {
        "source": "youtube",
        "title": clean_text((info or {}).get("title"), 240, "Playlist do YouTube"),
        "url": url,
        "cover": tracks[0]["thumbnail"],
        "tracks": tracks,
        "count": len(tracks),
        "truncated": False,
    }


def playlist_preview(value: Any) -> dict[str, Any]:
    source, playlist_id, normalized_url = normalize_playlist_url(value)
    cache_key = f"{source}:{playlist_id}"
    now = time.time()
    with PLAYLIST_LOCK:
        cached = PLAYLIST_CACHE.get(cache_key)
        if cached and now - cached[0] < 900:
            return cached[1]

    if not PLAYLIST_EXTRACT_SEMAPHORE.acquire(blocking=False):
        raise RuntimeError("Another playlist is being processed. Try again shortly")
    try:
        preview = (
            spotify_playlist_preview(normalized_url)
            if source == "spotify"
            else youtube_playlist_preview(normalized_url)
        )
        preview["playlist_id"] = playlist_id
        with PLAYLIST_LOCK:
            PLAYLIST_CACHE[cache_key] = (now, preview)
        return preview
    finally:
        PLAYLIST_EXTRACT_SEMAPHORE.release()


def normalized_words(value: Any) -> set[str]:
    ignored = {"a", "an", "and", "audio", "de", "do", "da", "e", "feat", "ft", "official", "the", "video"}
    return {
        word
        for word in re.findall(r"[\w]+", str(value or "").casefold(), re.UNICODE)
        if len(word) > 1 and word not in ignored
    }


def match_spotify_track(track: dict[str, Any]) -> dict[str, Any] | None:
    title = clean_text(track.get("title"), 240)
    artist = clean_text(track.get("uploader"), 160)
    query = clean_text(f"{artist} {title} audio", 120)
    candidates = search_youtube(query)
    wanted_words = normalized_words(f"{artist} {title}")
    unwanted = {"cover", "karaoke", "live", "remix", "slowed", "speed", "sped"}
    source_unwanted = unwanted & wanted_words
    source_duration = as_float(track.get("duration"))
    scored: list[tuple[float, dict[str, Any]]] = []
    for candidate in candidates:
        candidate_words = normalized_words(
            f"{candidate.get('uploader', '')} {candidate.get('title', '')}"
        )
        overlap = len(wanted_words & candidate_words) / max(1, len(wanted_words))
        score = overlap * 100
        candidate_duration = as_float(candidate.get("duration"))
        if source_duration and candidate_duration:
            difference = abs(source_duration - candidate_duration)
            score += 35 if difference <= 6 else 22 if difference <= 15 else 8 if difference <= 30 else -25
        score -= 18 * len((candidate_words & unwanted) - source_unwanted)
        scored.append((score, candidate))
    if not scored:
        return None
    score, best = max(scored, key=lambda item: item[0])
    if score < 45:
        return None
    return {
        **best,
        "title": title or best["title"],
        "uploader": artist or best["uploader"],
        "duration": source_duration or best["duration"],
    }


def video_is_active(video_id: str) -> bool:
    with DB_LOCK, db_connect() as db:
        return db.execute(
            "SELECT 1 FROM queue WHERE video_id = ? AND status IN ('pending', 'playing') LIMIT 1",
            (video_id,),
        ).fetchone() is not None


def update_playlist_job(job_id: str, **values: Any) -> None:
    with PLAYLIST_LOCK:
        job = PLAYLIST_JOBS.get(job_id)
        if job:
            job.update(values)
            job["updated_at"] = time.time()


def playlist_import_worker(job_id: str, url: str, limit: int, device_name: str, client_id: str) -> None:
    with PLAYLIST_IMPORT_SEMAPHORE:
        update_playlist_job(job_id, status="reading")
        try:
            preview = playlist_preview(url)
            tracks = preview["tracks"][:limit]
            update_playlist_job(
                job_id,
                status="running",
                source=preview["source"],
                playlist_title=preview["title"],
                total=len(tracks),
            )
            seen: set[str] = set()
            added = 0
            skipped = 0
            failed = 0
            for index, source_track in enumerate(tracks, start=1):
                update_playlist_job(job_id, current_title=source_track.get("title", ""))
                try:
                    item = (
                        match_spotify_track(source_track)
                        if preview["source"] == "spotify"
                        else source_track
                    )
                    video_id = clean_text((item or {}).get("video_id"), 20)
                    if (
                        not item
                        or not source_track.get("available", True)
                        or not VIDEO_ID_RE.fullmatch(video_id)
                        or video_id in seen
                        or video_is_active(video_id)
                    ):
                        skipped += 1
                    else:
                        seen.add(video_id)
                        add_to_queue(
                            {
                                **item,
                                "device_name": device_name,
                                "client_id": client_id,
                            }
                        )
                        added += 1
                except Exception as exc:
                    failed += 1
                    print(f"Playlist item error ({source_track.get('title', '')}): {exc}", flush=True)
                update_playlist_job(
                    job_id,
                    processed=index,
                    added=added,
                    skipped=skipped,
                    failed=failed,
                )
            update_playlist_job(job_id, status="completed", current_title="")
        except Exception as exc:
            print(f"Playlist import error: {exc}", flush=True)
            update_playlist_job(job_id, status="error", error=clean_text(exc, 300))


def start_playlist_import(payload: dict[str, Any]) -> dict[str, Any]:
    source, _, normalized_url = normalize_playlist_url(payload.get("url"))
    limit = playlist_limit(payload.get("limit"))
    device_name = clean_text(payload.get("device_name"), 40, "Browser")
    client_id = clean_text(payload.get("client_id"), 80, "unknown")
    job_id = uuid.uuid4().hex
    now = time.time()
    job = {
        "id": job_id,
        "status": "queued",
        "source": source,
        "playlist_title": "",
        "total": 0,
        "processed": 0,
        "added": 0,
        "skipped": 0,
        "failed": 0,
        "current_title": "",
        "error": "",
        "created_at": now,
        "updated_at": now,
    }
    with PLAYLIST_LOCK:
        expired = [key for key, value in PLAYLIST_JOBS.items() if now - value["created_at"] > 3600]
        for key in expired:
            PLAYLIST_JOBS.pop(key, None)
        PLAYLIST_JOBS[job_id] = job
    threading.Thread(
        target=playlist_import_worker,
        args=(job_id, normalized_url, limit, device_name, client_id),
        daemon=True,
        name=f"playlist-{job_id[:8]}",
    ).start()
    return dict(job)


def add_playlist_track(payload: dict[str, Any]) -> dict[str, Any]:
    source = clean_text(payload.get("source"), 20).lower()
    track = payload.get("track")
    if not isinstance(track, dict):
        raise ValueError("Invalid playlist track")
    item = match_spotify_track(track) if source == "spotify" else track
    if not item:
        raise ValueError("Could not find this track")
    result = add_to_queue({
        **item,
        "device_name": payload.get("device_name"),
        "client_id": payload.get("client_id"),
    })
    return {"track": track, **result}


def get_playlist_job(job_id: str) -> dict[str, Any]:
    with PLAYLIST_LOCK:
        job = PLAYLIST_JOBS.get(clean_text(job_id, 40))
        if not job:
            raise ValueError("Import job not found or expired")
        return dict(job)


def radio_seed() -> dict[str, Any] | None:
    if not active_clients():
        return None
    now = time.time()
    with DB_LOCK, db_connect() as db:
        if not bool_setting(db, "auto_radio"):
            return None
        if db.execute("SELECT 1 FROM queue WHERE status = 'pending' LIMIT 1").fetchone():
            return None
        playback = db.execute("SELECT * FROM playback WHERE id = 1").fetchone()
        if not playback:
            return None

        if playback["current_queue_id"] is not None:
            if playback["status"] != "playing":
                return None
            seed = db.execute(
                """
                SELECT q.video_id, q.title, q.uploader, q.duration
                  FROM queue q JOIN tracks t ON t.video_id = q.video_id
                 WHERE q.id = ?
                   AND t.media_type = 'youtube'
                """,
                (playback["current_queue_id"],),
            ).fetchone()
            if not seed:
                return None
            duration = as_float(seed["duration"])
            if duration <= 0 or duration - playback_position(playback, now) > 25:
                return None
        elif playback["status"] == "idle":
            seed = db.execute(
                """
                SELECT q.video_id, q.title, q.uploader, q.duration
                  FROM queue q JOIN tracks t ON t.video_id = q.video_id
                 WHERE q.started_at IS NOT NULL
                   AND t.media_type = 'youtube'
                 ORDER BY q.started_at DESC, q.id DESC
                 LIMIT 1
                """
            ).fetchone()
            if not seed:
                return None
        else:
            return None

        recent = {
            row["video_id"]
            for row in db.execute(
                """
                SELECT video_id FROM queue
                 WHERE started_at IS NOT NULL
                 ORDER BY started_at DESC, id DESC
                 LIMIT 20
                """
            ).fetchall()
        }
        return {
            "video_id": seed["video_id"],
            "title": seed["title"],
            "uploader": seed["uploader"],
            "recent": recent,
        }


def related_mix(seed: dict[str, Any]) -> list[dict[str, Any]]:
    video_id = seed["video_id"]
    options = youtube_dl_options(
        extract_flat=True,
        skip_download=True,
        playlistend=50,
        socket_timeout=12,
        retries=1,
    )
    mix_url = f"https://www.youtube.com/watch?v={video_id}&list=RD{video_id}"
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(mix_url, download=False)

    candidates: list[dict[str, Any]] = []
    seen = set(seed["recent"])
    seen.add(video_id)
    for entry in (info or {}).get("entries") or []:
        if not entry:
            continue
        candidate_id = clean_text(entry.get("id"), 20)
        if not VIDEO_ID_RE.fullmatch(candidate_id) or candidate_id in seen:
            continue
        seen.add(candidate_id)
        candidates.append(
            {
                "video_id": candidate_id,
                "title": clean_text(entry.get("title"), 240, "Video do YouTube"),
                "uploader": clean_text(
                    entry.get("channel") or entry.get("uploader"), 160
                ),
                "duration": as_float(entry.get("duration")),
                "view_count": as_int_or_none(entry.get("view_count")),
                "thumbnail": f"https://i.ytimg.com/vi/{candidate_id}/mqdefault.jpg",
                "device_name": "Automatic radio",
                "client_id": "auto-radio",
            }
        )
    return candidates


def current_music_seed() -> dict[str, Any] | None:
    with DB_LOCK, db_connect() as db:
        playback = db.execute("SELECT current_queue_id FROM playback WHERE id = 1").fetchone()
        if not playback or playback["current_queue_id"] is None:
            return None
        current = db.execute(
            """
            SELECT q.id AS queue_id, q.video_id, q.title, q.uploader, t.media_type
              FROM queue q JOIN tracks t ON t.video_id = q.video_id
             WHERE q.id = ?
            """,
            (playback["current_queue_id"],),
        ).fetchone()
        if (
            not current
            or not VIDEO_ID_RE.fullmatch(clean_text(current["video_id"], 20))
            or current["media_type"] not in {"music", "youtube"}
        ):
            return None
        recent = {
            row["video_id"]
            for row in db.execute(
                """
                SELECT video_id FROM queue
                 WHERE started_at IS NOT NULL
                 ORDER BY started_at DESC, id DESC
                 LIMIT 20
                """
            ).fetchall()
        }
        return {
            "queue_id": int(current["queue_id"]),
            "video_id": current["video_id"],
            "title": current["title"],
            "uploader": current["uploader"],
            "recent": recent,
        }


def cached_related_mix(seed: dict[str, Any]) -> list[dict[str, Any]]:
    video_id = seed["video_id"]
    now = time.time()
    with CACHE_LOCK:
        cached = RECOMMENDATION_CACHE.get(video_id)
        base_results = cached[1] if cached and now - cached[0] < 300 else None

    if base_results is None:
        if not RECOMMENDATION_SEMAPHORE.acquire(timeout=14):
            raise RuntimeError("Recommendations are busy. Try again shortly")
        try:
            with CACHE_LOCK:
                cached = RECOMMENDATION_CACHE.get(video_id)
                base_results = cached[1] if cached and time.time() - cached[0] < 300 else None
            if base_results is None:
                # Cache each track's raw Mix and filter out recent plays on every request.
                base_results = related_mix({**seed, "recent": set()})[:50]
                now = time.time()
                with CACHE_LOCK:
                    expired = [key for key, (created_at, _) in RECOMMENDATION_CACHE.items() if now - created_at >= 300]
                    for key in expired:
                        RECOMMENDATION_CACHE.pop(key, None)
                    if len(RECOMMENDATION_CACHE) >= 96:
                        oldest = min(RECOMMENDATION_CACHE, key=lambda key: RECOMMENDATION_CACHE[key][0])
                        RECOMMENDATION_CACHE.pop(oldest, None)
                    RECOMMENDATION_CACHE[video_id] = (now, base_results)
        finally:
            RECOMMENDATION_SEMAPHORE.release()

    recent = set(seed.get("recent") or set())
    return [item for item in base_results if item["video_id"] not in recent]


def current_music_recommendations(limit: Any = 10) -> dict[str, Any]:
    try:
        limit = max(1, min(int(limit), 10))
    except (TypeError, ValueError):
        limit = 10
    seed = current_music_seed()
    if not seed:
        return {"source_queue_id": None, "source_video_id": None, "results": []}

    video_id = seed["video_id"]
    recommendations = cached_related_mix(seed)

    # The current item can advance while yt-dlp is fetching the Mix.
    latest_seed = current_music_seed()
    if (
        not latest_seed
        or latest_seed["queue_id"] != seed["queue_id"]
        or latest_seed["video_id"] != video_id
    ):
        return {"source_queue_id": seed["queue_id"], "source_video_id": video_id, "results": []}
    results = [
        {
            "video_id": item["video_id"],
            "title": item["title"],
            "uploader": item["uploader"],
            "duration": item["duration"],
            "view_count": item["view_count"],
            "thumbnail": item["thumbnail"],
        }
        for item in recommendations
    ][:limit]
    return {"source_queue_id": seed["queue_id"], "source_video_id": video_id, "results": results}


def lastfm_related_mix(seed: dict[str, Any]) -> list[dict[str, Any]]:
    similar = lastfm_similar(seed.get("title", ""), seed.get("uploader", ""), 25)
    if not similar:
        similar = lastfm_similar(seed.get("title", ""), "", 25)
    candidates: list[dict[str, Any]] = []
    recent = set(seed.get("recent") or set())
    for item in similar:
        query = " ".join(part for part in (item.get("artist"), item.get("title")) if part).strip()
        if len(query) < 2:
            continue
        for result in search_youtube(query)[:3]:
            if result["video_id"] in recent or any(row["video_id"] == result["video_id"] for row in candidates):
                continue
            candidates.append({
                **result,
                "device_name": "Automatic radio",
                "client_id": "auto-radio",
            })
            break
        if len(candidates) >= 5:
            break
    return candidates


def radio_loop() -> None:
    while not STOP_EVENT.wait(3.0):
        try:
            seed = radio_seed()
            if not seed:
                continue
            now = time.time()
            with RADIO_LOCK:
                if now < float(RADIO_STATE["next_attempt_at"] or 0):
                    continue
                RADIO_STATE["searching"] = True
                RADIO_STATE["last_error"] = ""
            candidates = cached_related_mix(seed)
            if not candidates:
                raise RuntimeError("No related tracks available")
            result = add_to_queue(candidates[0], radio_seed_id=seed["video_id"])
            with RADIO_LOCK:
                if result.get("added"):
                    RADIO_STATE["last_added_at"] = time.time()
                RADIO_STATE["next_attempt_at"] = time.time() + 20
        except Exception as exc:  # pragma: no cover - depende do YouTube
            with RADIO_LOCK:
                RADIO_STATE["last_error"] = clean_text(str(exc), 240)
                RADIO_STATE["next_attempt_at"] = time.time() + 60
            print(f"auto radio error: {exc}", flush=True)
        finally:
            with RADIO_LOCK:
                RADIO_STATE["searching"] = False


def update_metadata(video_id: str, info: dict[str, Any]) -> None:
    duration = as_float(info.get("duration"))
    title = clean_text(info.get("title"), 240)
    uploader = clean_text(info.get("channel") or info.get("uploader"), 160)
    view_count = as_int_or_none(info.get("view_count"))
    with DB_LOCK, db_connect() as db:
        db.execute(
            """
            UPDATE tracks
               SET title = CASE WHEN ? <> '' THEN ? ELSE title END,
                   uploader = CASE WHEN ? <> '' THEN ? ELSE uploader END,
                   duration = CASE WHEN ? > 0 THEN ? ELSE duration END,
                   view_count = COALESCE(?, view_count)
             WHERE video_id = ?
            """,
            (title, title, uploader, uploader, duration, duration, view_count, video_id),
        )
        db.execute(
            """
            UPDATE queue
               SET title = CASE WHEN ? <> '' THEN ? ELSE title END,
                   uploader = CASE WHEN ? <> '' THEN ? ELSE uploader END,
                   duration = CASE WHEN ? > 0 THEN ? ELSE duration END,
                   view_count = COALESCE(?, view_count)
             WHERE video_id = ? AND status IN ('pending', 'playing')
            """,
            (title, title, uploader, uploader, duration, duration, view_count, video_id),
        )
        db.commit()


def resolve_podcast_stream(video_id: str, force: bool = False) -> dict[str, Any]:
    if not PODCAST_ID_RE.fullmatch(video_id):
        raise ValueError("Invalid podcast episode")
    with DB_LOCK, db_connect() as db:
        row = db.execute(
            "SELECT stream_url FROM tracks WHERE video_id = ? AND media_type = 'podcast'",
            (video_id,),
        ).fetchone()
    if row is None or not row["stream_url"]:
        raise DownloadError("Podcast episode not found")
    stream_url = public_stream_url(row["stream_url"])
    with CACHE_LOCK:
        cached = STREAM_CACHE.get(video_id)
        if not force and cached and time.time() - cached[0] < 900:
            return cached[1]
    content_type = mimetypes.guess_type(urllib.parse.urlparse(stream_url).path)[0] or "audio/mpeg"
    stream = {
        "url": stream_url,
        "headers": {"User-Agent": PODCAST_USER_AGENT},
        "content_type": content_type,
    }
    with CACHE_LOCK:
        STREAM_CACHE[video_id] = (time.time(), stream)
    return stream


def resolve_stream(video_id: str, force: bool = False) -> dict[str, Any]:
    if PODCAST_ID_RE.fullmatch(video_id):
        return resolve_podcast_stream(video_id, force=force)
    if not VIDEO_ID_RE.fullmatch(video_id):
        raise ValueError("Invalid video ID")
    if not STREAM_RESOLVE_LOCK.acquire(timeout=STREAM_RESOLVE_WAIT_SECONDS):
        raise RuntimeError("Audio resolution is busy. Try again shortly")
    try:
        now = time.time()
        with CACHE_LOCK:
            cached = STREAM_CACHE.get(video_id)
            if not force and cached and now - cached[0] < 300:
                return cached[1]

        options = youtube_dl_options(
            format="bestaudio[ext=m4a]/bestaudio[acodec^=mp4a]/bestaudio/best",
            noplaylist=True,
            skip_download=True,
            socket_timeout=12,
            retries=1,
            fragment_retries=1,
        )
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        if not info or not info.get("url"):
            raise DownloadError("No audio stream available")
        stream = {
            "url": info["url"],
            "headers": info.get("http_headers") or {},
            "content_type": mimetypes.guess_type(f"audio.{info.get('ext', 'm4a')}")[0]
            or "audio/mp4",
        }
        update_metadata(video_id, info)
        with CACHE_LOCK:
            STREAM_CACHE[video_id] = (now, stream)
        return stream
    finally:
        STREAM_RESOLVE_LOCK.release()


def current_music_for_download(video_id: str) -> dict[str, Any]:
    with DB_LOCK, db_connect() as db:
        row = db.execute(
            """
            SELECT q.video_id, q.title, q.uploader, t.media_type
              FROM playback p
              JOIN queue q ON q.id = p.current_queue_id
              JOIN tracks t ON t.video_id = q.video_id
             WHERE p.id = 1 AND q.video_id = ?
            """,
            (video_id,),
        ).fetchone()
    if row is None or row["media_type"] not in {"music", "youtube"}:
        raise ValueError("Download is available for the current track only")
    return dict(row)


def download_filename(title: str, content_type: str) -> str:
    cleaned = re.sub(r"[^\w .()\[\]-]+", "", clean_text(title, 180), flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .") or "self-station"
    extension = mimetypes.guess_extension(content_type or "") or ".m4a"
    if extension == ".mp4":
        extension = ".m4a"
    return f"{cleaned}{extension}"


def mark_stream_error(video_id: str, message: str) -> None:
    now = time.time()
    with DB_LOCK, db_connect() as db:
        playback = db.execute("SELECT current_queue_id FROM playback WHERE id = 1").fetchone()
        if not playback or playback["current_queue_id"] is None:
            return
        current = db.execute(
            "SELECT video_id FROM queue WHERE id = ?", (playback["current_queue_id"],)
        ).fetchone()
        if current and current["video_id"] == video_id:
            db.execute(
                "UPDATE queue SET status = 'error', finished_at = ?, error_message = ? WHERE id = ?",
                (now, clean_text(message, 500), playback["current_queue_id"]),
            )
            start_next(db, now)
            db.commit()


def maintenance_loop() -> None:
    while not STOP_EVENT.wait(1.0):
        try:
            with DB_LOCK, db_connect() as db:
                advance_if_finished(db, time.time())
                db.commit()
        except Exception as exc:  # pragma: no cover - protecao do servico
            print(f"maintenance error: {exc}", flush=True)


def current_live_radio(queue_id: int) -> dict[str, Any]:
    with DB_LOCK, db_connect() as db:
        row = db.execute(
            """
            SELECT q.id, q.status, t.stream_url, t.title
              FROM playback p
              JOIN queue q ON q.id = p.current_queue_id
              JOIN tracks t ON t.video_id = q.video_id
             WHERE p.id = 1
               AND p.current_queue_id = ?
               AND p.status = 'playing'
               AND q.status = 'playing'
               AND t.media_type = 'radio'
            """,
            (queue_id,),
        ).fetchone()
    if not row:
        raise ValueError("This radio station is not playing")
    return {
        "queue_id": int(row["id"]),
        "title": row["title"],
        "stream_url": public_stream_url(row["stream_url"]),
    }


def recognize_live_radio(queue_id: int) -> dict[str, Any]:
    """Capture three radio windows and accept an AcoustID consensus."""
    if not ACOUSTID_CLIENT:
        raise ValueError("Recognition unavailable: configure SELF_STATION_ACOUSTID_CLIENT")
    now = time.time()
    with RADIO_RECOGNITION_LOCK:
        cached = RADIO_RECOGNITION_CACHE.get(queue_id)
    if cached and cached[1].get("found") and now - cached[0] < RADIO_RECOGNITION_COOLDOWN:
        result = dict(cached[1])
        result["cooldown_remaining"] = max(0, int(RADIO_RECOGNITION_COOLDOWN - (now - cached[0])))
        return result

    source = current_live_radio(queue_id)
    ffmpeg = shutil.which("ffmpeg")
    fpcalc = shutil.which("fpcalc")
    if not ffmpeg or not fpcalc:
        raise RuntimeError("Recognition unavailable: ffmpeg and fpcalc are required")

    capture_seconds = 30
    window_seconds = 10
    window_offsets = (0, 10, 20)

    def score_for(match: dict[str, Any]) -> float:
        try:
            return float(match.get("score") or 0)
        except (TypeError, ValueError):
            return 0.0

    def candidate_key(recording: dict[str, Any]) -> tuple[str, str, str]:
        artists = recording.get("artists") or []
        artist = artists[0].get("name") if artists and isinstance(artists[0], dict) else ""
        title = clean_text(recording.get("title"), 240)
        artist = clean_text(artist, 160)
        recording_id = clean_text(recording.get("id"), 80)
        if recording_id:
            return (f"mbid:{recording_id}", artist, title)
        normalized_artist = re.sub(r"\s+", " ", artist).strip().casefold()
        normalized_title = re.sub(r"\s+", " ", title).strip().casefold()
        return (f"track:{normalized_artist}\x1f{normalized_title}", artist, title)

    def lookup_acoustid(fingerprint_value: str, duration: int) -> list[dict[str, Any]]:
        form = urllib.parse.urlencode({
            "client": ACOUSTID_CLIENT,
            "duration": duration,
            "fingerprint": fingerprint_value,
            "meta": "recordings+releasegroups+releases+compress",
        }).encode("utf-8")
        request = urllib.request.Request(ACOUSTID_API_URL, data=form, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": RADIO_BROWSER_USER_AGENT,
        }, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                acoustid = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError("Could not reach AcoustID") from exc
        if not isinstance(acoustid, dict) or acoustid.get("status") != "ok":
            api_error = acoustid.get("error") if isinstance(acoustid, dict) else None
            error_message = api_error.get("message") if isinstance(api_error, dict) else "invalid response"
            raise RuntimeError(f"AcoustID rejected the request: {clean_text(error_message, 240)}")
        return [item for item in acoustid.get("results") or [] if isinstance(item, dict)]

    groups: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="self-station-") as temp_dir:
        sample_path = Path(temp_dir) / "sample.wav"
        command = [
            ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_at_eof", "1",
            "-reconnect_delay_max", "5", "-rw_timeout", "15000000",
            "-user_agent", RADIO_BROWSER_USER_AGENT, "-re", "-i", source["stream_url"],
            "-t", str(capture_seconds), "-vn", "-ac", "1", "-ar", "11025", "-c:a", "pcm_s16le",
            "-f", "wav", str(sample_path),
        ]
        capture = subprocess.run(command, capture_output=True, timeout=45, check=False)
        if capture.returncode != 0 or not sample_path.exists() or sample_path.stat().st_size < 256:
            detail = capture.stderr.decode("utf-8", "replace").strip()
            raise RuntimeError("Could not capture a radio sample" + (f": {clean_text(detail, 240)}" if detail else ""))
        for index, offset in enumerate(window_offsets, start=1):
            window_path = Path(temp_dir) / f"window-{index}.wav"
            extract = subprocess.run([
                ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                "-ss", str(offset), "-i", str(sample_path), "-t", str(window_seconds),
                "-vn", "-ac", "1", "-ar", "11025", "-c:a", "pcm_s16le", "-f", "wav", str(window_path),
            ], capture_output=True, timeout=15, check=False)
            if extract.returncode != 0 or not window_path.exists() or window_path.stat().st_size < 256:
                detail = extract.stderr.decode("utf-8", "replace").strip()
                raise RuntimeError(f"Could not prepare radio sample {index}" + (f": {clean_text(detail, 240)}" if detail else ""))
            fingerprint = subprocess.run([fpcalc, "-json", str(window_path)], capture_output=True, timeout=10, check=False)
            try:
                fingerprint_data = json.loads(fingerprint.stdout.decode("utf-8"))
                duration = max(1, int(float(fingerprint_data["duration"])))
                fingerprint_value = clean_text(fingerprint_data["fingerprint"], 200000)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                detail = fingerprint.stderr.decode("utf-8", "replace").strip()
                raise RuntimeError("Could not fingerprint the audio" + (f": {clean_text(detail, 240)}" if detail else "")) from exc
            if duration < window_seconds:
                raise RuntimeError(f"Radio sample {index} was too short ({duration}s). Try again")

            window_candidates: dict[str, dict[str, Any]] = {}
            for match in lookup_acoustid(fingerprint_value, duration):
                score = score_for(match)
                for recording in match.get("recordings") or []:
                    if not isinstance(recording, dict) or not recording.get("title"):
                        continue
                    key, artist, title = candidate_key(recording)
                    candidate = window_candidates.get(key)
                    if candidate is None or score > candidate["score"]:
                        window_candidates[key] = {"artist": artist, "title": title, "score": score}
            for key, candidate in window_candidates.items():
                group = groups.setdefault(key, {
                    "artist": candidate["artist"], "title": candidate["title"], "scores": [], "windows": set(),
                })
                group["scores"].append(candidate["score"])
                group["windows"].add(index)

    best_group = max(
        groups.values(),
        key=lambda group: (len(group["windows"]), sum(group["scores"]) / len(group["scores"])),
        default=None,
    )
    matched_windows = len(best_group["windows"]) if best_group else 0
    score = round(sum(best_group["scores"]) / len(best_group["scores"]), 3) if best_group else None
    consensus = matched_windows >= 2
    if consensus and best_group:
        result = {
            "found": True, "station": source["title"], "artist": best_group["artist"],
            "title": best_group["title"], "source": "AcoustID", "score": score,
            "sample_duration": capture_seconds, "windows": len(window_offsets),
            "matched_windows": matched_windows, "consensus": True,
            "cooldown": RADIO_RECOGNITION_COOLDOWN,
        }
    else:
        stream_title = LIVE_RADIO_RELAY.current_title(queue_id)
        if stream_title:
            metadata_parts = re.split(r"\s+-\s+", stream_title, maxsplit=1)
            result = {
                "found": True, "station": source["title"],
                "artist": clean_text(metadata_parts[0], 160) if len(metadata_parts) > 1 else "",
                "title": clean_text(metadata_parts[-1], 240), "source": "stream metadata",
                "score": score, "sample_duration": capture_seconds, "windows": len(window_offsets),
                "matched_windows": matched_windows, "consensus": False,
                "cooldown": RADIO_RECOGNITION_COOLDOWN,
            }
        else:
            message = (
                "Matches found, but the captured samples disagree"
                if best_group else "No matches found in the captured samples"
            )
            result = {
                "found": False, "station": source["title"], "message": message,
                "score": score, "sample_duration": capture_seconds, "windows": len(window_offsets),
                "matched_windows": matched_windows, "consensus": False,
                "cooldown": RADIO_RECOGNITION_COOLDOWN,
            }
    if result.get("found"):
        with RADIO_RECOGNITION_LOCK:
            RADIO_RECOGNITION_CACHE[queue_id] = (time.time(), result)
    return result


class LiveRadioRelay:
    """One FFmpeg process receives the station stream and relays MP3 audio to browsers."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.key: int | None = None
        self.process: subprocess.Popen[bytes] | None = None
        self.subscribers: dict[str, queue_module.Queue[bytes | None]] = {}
        self.started_at: float | None = None
        self.last_error = ""
        self.metadata_title = ""
        self.metadata_response: Any | None = None
        self.metadata_stop = threading.Event()
        self.metadata_thread: threading.Thread | None = None

    def _close_subscribers_locked(self) -> None:
        subscribers = list(self.subscribers.values())
        self.subscribers.clear()
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(None)
            except queue_module.Full:
                try:
                    subscriber.get_nowait()
                    subscriber.put_nowait(None)
                except (queue_module.Empty, queue_module.Full):
                    pass

    def _stop_locked(self) -> None:
        process = self.process
        metadata_response = self.metadata_response
        self.process = None
        self.key = None
        self.started_at = None
        self.metadata_stop.set()
        self.metadata_response = None
        self.metadata_thread = None
        self.metadata_title = ""
        self._close_subscribers_locked()
        if metadata_response is not None:
            try:
                metadata_response.close()
            except OSError:
                pass
        if process and process.poll() is None:
            try:
                process.terminate()
            except OSError:
                pass

    def stop(self) -> None:
        with self.lock:
            self._stop_locked()

    def _start_locked(self, queue_id: int, stream_url: str) -> None:
        self._stop_locked()
        command = [
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_at_eof",
            "1",
            "-reconnect_delay_max",
            "5",
            "-rw_timeout",
            "15000000",
            "-user_agent",
            RADIO_BROWSER_USER_AGENT,
            "-i",
            stream_url,
            "-vn",
            "-sn",
            "-dn",
            "-c:a",
            "libmp3lame",
            "-b:a",
            f"{RADIO_RELAY_BITRATE}k",
            "-ar",
            "44100",
            "-ac",
            "2",
            "-f",
            "mp3",
            "pipe:1",
        ]
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )
        except OSError as exc:
            self.last_error = clean_text(str(exc), 240)
            raise RuntimeError("Could not start the radio relay") from exc
        self.process = process
        self.key = queue_id
        self.started_at = time.time()
        self.last_error = ""
        self.metadata_stop = threading.Event()
        self.metadata_thread = threading.Thread(
            target=self._metadata_reader,
            args=(stream_url, queue_id, self.metadata_stop),
            daemon=True,
            name=f"radio-metadata-{queue_id}",
        )
        self.metadata_thread.start()
        threading.Thread(
            target=self._reader,
            args=(process, queue_id),
            daemon=True,
            name=f"radio-relay-{queue_id}",
        ).start()

    @staticmethod
    def _read_exact(stream: Any, size: int, stop: threading.Event) -> bytes | None:
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0 and not stop.is_set():
            chunk = stream.read(remaining)
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks) if remaining == 0 else None

    def _metadata_reader(self, stream_url: str, queue_id: int, stop: threading.Event) -> None:
        request = urllib.request.Request(
            stream_url,
            headers={
                "Icy-MetaData": "1",
                "User-Agent": RADIO_BROWSER_USER_AGENT,
            },
        )
        response: Any | None = None
        try:
            response = urllib.request.urlopen(request, timeout=15)
            with self.lock:
                if self.key != queue_id or stop.is_set():
                    return
                self.metadata_response = response
            try:
                metaint = int(response.headers.get("icy-metaint", "0") or 0)
            except (TypeError, ValueError):
                metaint = 0
            if metaint <= 0:
                return
            while not stop.is_set():
                if self._read_exact(response, metaint, stop) is None:
                    break
                length_byte = self._read_exact(response, 1, stop)
                if not length_byte:
                    break
                metadata_length = length_byte[0] * 16
                if metadata_length == 0:
                    continue
                metadata = self._read_exact(response, metadata_length, stop)
                if metadata is None:
                    break
                text = metadata.rstrip(b"\x00").decode("utf-8", errors="replace")
                match = re.search(r"StreamTitle=['\"](.*?)['\"]\s*;", text, re.IGNORECASE)
                if not match:
                    continue
                title = clean_text(match.group(1), 240)
                if title:
                    with self.lock:
                        if self.key == queue_id and not stop.is_set():
                            self.metadata_title = title
        except (OSError, urllib.error.URLError, ValueError):
            return
        finally:
            if response is not None:
                try:
                    response.close()
                except OSError:
                    pass
            with self.lock:
                if self.metadata_response is response:
                    self.metadata_response = None

    def current_title(self, queue_id: int) -> str:
        with self.lock:
            return self.metadata_title if self.key == queue_id else ""

    def _reader(self, process: subprocess.Popen[bytes], queue_id: int) -> None:
        error = ""
        try:
            if process.stdout is None:
                raise RuntimeError("FFmpeg produced no audio")
            while not STOP_EVENT.is_set():
                chunk = process.stdout.read(16 * 1024)
                if not chunk:
                    break
                stale: list[str] = []
                with self.lock:
                    if self.process is not process or self.key != queue_id:
                        break
                    for token, subscriber in self.subscribers.items():
                        try:
                            subscriber.put_nowait(chunk)
                        except queue_module.Full:
                            stale.append(token)
                    for token in stale:
                        subscriber = self.subscribers.pop(token, None)
                        if subscriber:
                            try:
                                subscriber.get_nowait()
                                subscriber.put_nowait(None)
                            except (queue_module.Empty, queue_module.Full):
                                pass
        except Exception as exc:  # pragma: no cover - depende do stream externo
            error = clean_text(str(exc), 240)
        finally:
            try:
                if process.stdout:
                    process.stdout.close()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    process.kill()
                except OSError:
                    pass
            with self.lock:
                if self.process is process:
                    if not error and not STOP_EVENT.is_set():
                        error = "O stream da radio foi encerrado"
                    self.metadata_stop.set()
                    metadata_response = self.metadata_response
                    self.metadata_response = None
                    self.metadata_thread = None
                    self.metadata_title = ""
                    self.last_error = error
                    self.process = None
                    self.key = None
                    self.started_at = None
                    self._close_subscribers_locked()
                    if metadata_response is not None:
                        try:
                            metadata_response.close()
                        except OSError:
                            pass

    def subscribe(
        self, queue_id: int, stream_url: str
    ) -> tuple[str, queue_module.Queue[bytes | None]]:
        token = uuid.uuid4().hex
        subscriber: queue_module.Queue[bytes | None] = queue_module.Queue(maxsize=64)
        with self.lock:
            if (
                self.key != queue_id
                or self.process is None
                or self.process.poll() is not None
            ):
                self._start_locked(queue_id, stream_url)
            self.subscribers[token] = subscriber
        return token, subscriber

    def unsubscribe(self, token: str) -> None:
        with self.lock:
            self.subscribers.pop(token, None)

    def status(self) -> dict[str, Any]:
        with self.lock:
            running = self.process is not None and self.process.poll() is None
            return {
                "running": running,
                "queue_id": self.key if running else None,
                "listeners": len(self.subscribers),
                "started_at": self.started_at if running else None,
                "last_error": self.last_error,
            }


LIVE_RADIO_RELAY = LiveRadioRelay()


class SelfStationServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(BaseHTTPRequestHandler):
    server_version = "self-station/2.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} [{self.log_date_time_string()}] {fmt % args}", flush=True)

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def send_error_json(self, message: str, status: int = 400) -> None:
        self.send_json({"error": message}, status)

    def read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Invalid Content-Length header") from exc
        if length <= 0 or length > 65536:
            raise ValueError("Invalid request body")
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object")
        return data

    def do_HEAD(self) -> None:  # noqa: N802
        self.route(head_only=True)

    def do_GET(self) -> None:  # noqa: N802
        self.route()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        try:
            payload = self.read_json()
            if parsed.path == "/api/queue":
                result = add_to_queue(payload)
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/radio/queue":
                result = add_live_radio_to_queue(payload)
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/podcast/queue":
                result = add_podcast_episode_to_queue(payload)
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/queue/remove":
                result = remove_from_queue(int(payload.get("queue_id", 0)))
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/queue/clear":
                result = clear_queue()
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/queue/move":
                result = move_queue_item(
                    int(payload.get("queue_id", 0)),
                    clean_text(payload.get("direction"), 10),
                )
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/playlist/import":
                job = start_playlist_import(payload)
                self.send_json({"ok": True, "job": job}, 202)
                return
            if parsed.path == "/api/playlist/track":
                result = add_playlist_track(payload)
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/control":
                control_playback(clean_text(payload.get("action"), 20))
                self.send_json({"ok": True})
                return
            if parsed.path == "/api/favorites/toggle":
                result = toggle_favorite(payload.get("video_id"))
                self.send_json({"ok": True, **result})
                return
            if parsed.path == "/api/settings":
                if not isinstance(payload.get("auto_radio"), bool):
                    raise ValueError("auto_radio must be true or false")
                set_auto_radio(payload["auto_radio"])
                self.send_json({"ok": True, "auto_radio": payload["auto_radio"]})
                return
            if parsed.path == "/api/ended":
                handle_ended(int(payload.get("queue_id", 0)))
                self.send_json({"ok": True})
                return
            self.send_error_json("Endpoint not found", 404)
        except ValueError as exc:
            self.send_error_json(str(exc), 400)
        except RuntimeError as exc:
            self.send_error_json(str(exc), 429)
        except Exception as exc:
            print(f"POST error: {exc}", flush=True)
            self.send_error_json("Internal server error", 500)

    def route(self, head_only: bool = False) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if parsed.path in {"/", "/index.html"}:
                body = INDEX_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                if not head_only:
                    self.wfile.write(body)
                return
            ui_assets = {
                "/in-the-black/app.css": ("app.css", "text/css; charset=utf-8"),
                "/in-the-black/black.css": ("black.css", "text/css; charset=utf-8"),
                "/in-the-black/black.js": ("black.js", "text/javascript; charset=utf-8"),
                "/in-the-black/lucide-icons.json": ("lucide-icons.json", "application/json"),
            }
            if parsed.path in ui_assets:
                filename, content_type = ui_assets[parsed.path]
                body = (BASE_DIR / filename).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                if not head_only:
                    self.wfile.write(body)
                return
            if parsed.path == "/app.css":
                body = APP_CSS_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/css; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                if not head_only:
                    self.wfile.write(body)
                return
            if parsed.path == "/gradient-motion.webm":
                body = VIDEO_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "video/webm")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "public, max-age=31536000, immutable")
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                if not head_only:
                    self.wfile.write(body)
                return
            if parsed.path == "/api/health":
                self.send_json(
                    {
                        "ok": True,
                        "service": "self-station",
                        "time": time.time(),
                        "live_radio_relay": LIVE_RADIO_RELAY.status(),
                    }
                )
                return
            if parsed.path == "/api/state":
                self.send_json(
                    get_state(
                        query.get("client_id", [""])[0],
                        query.get("device_name", [""])[0],
                        self.client_address[0] if self.client_address else "",
                    )
                )
                return
            if parsed.path == "/api/search":
                self.send_json(
                    search_youtube_page(
                        query.get("q", [""])[0],
                        int(query.get("page", ["1"])[0]),
                        int(query.get("limit", [str(SEARCH_LIMIT)])[0]),
                    )
                )
                return
            if parsed.path == "/api/recommendations":
                self.send_json(
                    current_music_recommendations(query.get("limit", ["10"])[0])
                )
                return
            if parsed.path == "/api/recents":
                with DB_LOCK, db_connect() as db:
                    payload = paginated_recent_items(
                        db,
                        int(query.get("page", ["1"])[0]),
                        int(query.get("limit", ["25"])[0]),
                    )
                self.send_json(payload)
                return
            if parsed.path == "/api/favorites":
                with DB_LOCK, db_connect() as db:
                    payload = paginated_favorite_items(
                        db,
                        int(query.get("page", ["1"])[0]),
                        int(query.get("limit", ["25"])[0]),
                    )
                self.send_json(payload)
                return
            if parsed.path == "/api/metadata":
                self.send_json(
                    resolve_track_metadata(
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                    )
                )
                return
            if parsed.path == "/api/lyrics":
                self.send_json(
                    lyrics_lookup(
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                    )
                )
                return
            if parsed.path == "/api/lastfm/context":
                self.send_json(
                    lastfm_context(
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                    )
                )
                return
            if parsed.path == "/api/lastfm/similar":
                self.send_json({
                    "results": lastfm_similar(
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                        25,
                    )
                })
                return
            if parsed.path == "/api/lastfm/discover":
                self.send_json(
                    lastfm_discover(
                        query.get("tag", ["rock"])[0],
                        int(query.get("page", ["1"])[0]),
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                    )
                )
                return
            if parsed.path == "/api/spotify/playlists":
                self.send_json(spotify_profile_playlists(query.get("profile", [""])[0]))
                return
            if parsed.path == "/api/radio/search":
                results = search_live_radios(query.get("q", [""])[0])
                self.send_json({"results": results})
                return
            if parsed.path == "/api/podcast/search":
                results = search_podcasts(query.get("q", [""])[0])
                self.send_json({"results": results})
                return
            if parsed.path == "/api/podcast/episodes":
                result = podcast_episodes(query.get("id", [""])[0])
                self.send_json(result)
                return
            if parsed.path == "/api/playlist/preview":
                preview = playlist_preview(query.get("url", [""])[0])
                self.send_json(preview)
                return
            if parsed.path.startswith("/api/playlist/import/"):
                job_id = parsed.path.removeprefix("/api/playlist/import/")
                self.send_json({"job": get_playlist_job(job_id)})
                return
            if parsed.path.startswith("/download/"):
                video_id = urllib.parse.unquote(parsed.path.removeprefix("/download/"))
                self.proxy_stream(video_id, head_only, download=True)
                return
            if parsed.path.startswith("/stream/"):
                video_id = urllib.parse.unquote(parsed.path.removeprefix("/stream/"))
                self.proxy_stream(video_id, head_only)
                return
            if parsed.path.startswith("/relay/radio/"):
                queue_id = int(parsed.path.removeprefix("/relay/radio/"))
                self.relay_live_radio(queue_id, head_only)
                return
            self.send_error_json("Endpoint not found", 404)
        except ValueError as exc:
            self.send_error_json(str(exc), 400)
        except RuntimeError as exc:
            self.send_error_json(str(exc), 429)
        except DownloadError as exc:
            video_id = ""
            if parsed.path.startswith("/stream/"):
                video_id = urllib.parse.unquote(parsed.path.removeprefix("/stream/"))
            elif parsed.path.startswith("/download/"):
                video_id = urllib.parse.unquote(parsed.path.removeprefix("/download/"))
            if video_id:
                mark_stream_error(video_id, str(exc))
            message = "Could not retrieve audio" if video_id else "Could not search YouTube"
            self.send_error_json(message, 502)
        except Exception as exc:
            print(f"GET error: {exc}", flush=True)
            self.send_error_json("Internal server error", 500)

    def relay_live_radio(self, queue_id: int, head_only: bool) -> None:
        source = current_live_radio(queue_id)
        token = ""
        subscriber: queue_module.Queue[bytes | None] | None = None
        if not head_only:
            token, subscriber = LIVE_RADIO_RELAY.subscribe(
                queue_id, source["stream_url"]
            )
        self.send_response(200)
        self.send_header("Content-Type", "audio/mpeg")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if head_only:
            return
        if subscriber is None:
            return
        try:
            while not STOP_EVENT.is_set():
                try:
                    chunk = subscriber.get(timeout=20)
                except queue_module.Empty:
                    if not LIVE_RADIO_RELAY.status()["running"]:
                        break
                    continue
                if chunk is None:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            LIVE_RADIO_RELAY.unsubscribe(token)

    def proxy_stream(self, video_id: str, head_only: bool, download: bool = False) -> None:
        current = current_music_for_download(video_id) if download else None
        stream = resolve_stream(video_id)

        def upstream_headers(current_stream: dict[str, Any]) -> dict[str, str]:
            headers = {
                str(key): str(value)
                for key, value in current_stream["headers"].items()
                if key.lower() in {"user-agent", "referer", "origin", "accept-language"}
            }
            if self.headers.get("Range"):
                headers["Range"] = self.headers["Range"]
            return headers

        headers = upstream_headers(stream)
        request = urllib.request.Request(
            stream["url"], headers=headers, method="HEAD" if head_only else "GET"
        )
        try:
            upstream = urllib.request.urlopen(request, timeout=35)
        except urllib.error.HTTPError as exc:
            if exc.code in {403, 410}:
                stream = resolve_stream(video_id, force=True)
                headers = upstream_headers(stream)
                request = urllib.request.Request(
                    stream["url"], headers=headers, method="HEAD" if head_only else "GET"
                )
                upstream = urllib.request.urlopen(request, timeout=35)
            else:
                raise

        with upstream:
            status = getattr(upstream, "status", HTTPStatus.OK)
            self.send_response(status)
            forwarded = {
                "content-type",
                "content-length",
                "content-range",
                "accept-ranges",
                "etag",
                "last-modified",
            }
            has_content_type = False
            for key, value in upstream.headers.items():
                if key.lower() in forwarded:
                    self.send_header(key, value)
                    has_content_type = has_content_type or key.lower() == "content-type"
            if not has_content_type:
                self.send_header("Content-Type", stream["content_type"])
            if download and current:
                filename = download_filename(current["title"], stream["content_type"])
                fallback = filename.encode("ascii", "ignore").decode("ascii") or "self-station.m4a"
                encoded = urllib.parse.quote(filename, safe="-._()[]")
                self.send_header(
                    "Content-Disposition",
                    f'attachment; filename="{fallback}"; filename*=UTF-8\'\'{encoded}',
                )
                self.send_header("Cache-Control", "private, no-store")
            else:
                self.send_header("Cache-Control", "private, max-age=300")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            if head_only:
                return
            try:
                while chunk := upstream.read(128 * 1024):
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass


def main() -> None:
    init_db()
    maintenance = threading.Thread(target=maintenance_loop, name="maintenance", daemon=True)
    maintenance.start()
    radio = threading.Thread(target=radio_loop, name="auto-radio", daemon=True)
    radio.start()
    server = SelfStationServer((HOST, PORT), Handler)
    print(f"Self Station listening on http://{HOST}:{PORT}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        STOP_EVENT.set()
        LIVE_RADIO_RELAY.stop()
        server.server_close()


if __name__ == "__main__":
    main()
