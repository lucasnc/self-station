#!/usr/bin/env python3
"""Self Station: collaborative self-hosted music queue and radio relay."""

from __future__ import annotations

import json
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
import uuid
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError


BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
APP_CSS_FILE = BASE_DIR / "app.css"
DB_FILE = Path(os.environ.get("SELF_STATION_DB", BASE_DIR / "self_station.db"))
HOST = os.environ.get("SELF_STATION_HOST", "0.0.0.0")
PORT = int(os.environ.get("SELF_STATION_PORT", "8090"))
SEARCH_LIMIT = max(1, min(int(os.environ.get("SELF_STATION_SEARCH_LIMIT", "12")), 25))
LIVE_RADIO_SEARCH_LIMIT = max(
    1, min(int(os.environ.get("SELF_STATION_LIVE_SEARCH_LIMIT", "24")), 50)
)
RADIO_BROWSER_BASE_URL = os.environ.get(
    "SELF_STATION_BROWSER_URL", "https://all.api.radio-browser.info"
).rstrip("/")
RADIO_BROWSER_USER_AGENT = "self-station/2.0"
LYRICS_BASE_URL = os.environ.get("SELF_STATION_LYRICS_URL", "https://lrclib.net").rstrip("/")
LYRICS_USER_AGENT = "self-station/2.0"
RADIO_RELAY_BITRATE = max(
    64, min(int(os.environ.get("SELF_STATION_RELAY_BITRATE", "128")), 320)
)
PLAYLIST_IMPORT_LIMIT = max(
    1, min(int(os.environ.get("SELF_STATION_PLAYLIST_IMPORT_LIMIT", "100")), 100)
)
SYNC_GRACE_SECONDS = 1.5
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
PLAYLIST_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,100}$")
RADIO_STATION_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
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
    """Prefer IPv4 when the local network advertises unusable IPv6 routes."""
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
PLAYLIST_LOCK = threading.RLock()
SEARCH_SEMAPHORE = threading.BoundedSemaphore(2)
PLAYLIST_EXTRACT_SEMAPHORE = threading.BoundedSemaphore(1)
PLAYLIST_IMPORT_SEMAPHORE = threading.BoundedSemaphore(1)
STOP_EVENT = threading.Event()

SEARCH_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
LYRICS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
LIVE_RADIO_SEARCH_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
STREAM_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
CLIENTS: dict[str, dict[str, Any]] = {}
PLAYLIST_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
PLAYLIST_JOBS: dict[str, dict[str, Any]] = {}
RADIO_STATE: dict[str, Any] = {
    "searching": False,
    "last_error": "",
    "last_added_at": None,
    "next_attempt_at": 0.0,
}


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
        "thumbnail": row["thumbnail"] if "thumbnail" in keys else "",
        "country": row["country"] if "country" in keys else "",
        "state": row["state"] if "state" in keys else "",
        "tags": row["tags"] if "tags" in keys else "",
        "codec": row["codec"] if "codec" in keys else "",
        "bitrate": row["bitrate"] if "bitrate" in keys else None,
    }


def register_client(client_id: str, device_name: str) -> None:
    if not client_id:
        return
    now = time.time()
    with CLIENT_LOCK:
        CLIENTS[client_id[:80]] = {
            "id": client_id[:80],
            "name": clean_text(device_name, 40, "Navegador"),
            "last_seen": now,
        }
        stale = [key for key, value in CLIENTS.items() if now - value["last_seen"] > 25]
        for key in stale:
            CLIENTS.pop(key, None)


def active_clients() -> list[dict[str, Any]]:
    now = time.time()
    with CLIENT_LOCK:
        return [
            {"id": value["id"], "name": value["name"]}
            for value in CLIENTS.values()
            if now - value["last_seen"] <= 25
        ]


def get_state(client_id: str = "", device_name: str = "") -> dict[str, Any]:
    register_client(client_id, device_name)
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
        recent_rows = db.execute(
            """
            SELECT q.id, q.video_id, q.title, q.uploader, q.duration,
                   q.view_count, q.added_by, q.added_at, q.started_at, q.status,
                   t.local_plays, t.media_type, t.station_uuid,
                   t.thumbnail, t.country, t.state, t.tags, t.codec, t.bitrate
              FROM queue q
              JOIN tracks t ON t.video_id = q.video_id
             WHERE q.started_at IS NOT NULL
             ORDER BY q.started_at DESC, q.id DESC
             LIMIT 5
            """
        ).fetchall()
        current = row_to_item(current_row)
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
        return {
            "server_time": now,
            "status": playback["status"] if playback else "idle",
            "position": position,
            "revision": playback["revision"] if playback else 0,
            "current": current,
            "queue": [row_to_item(row) for row in queue_rows],
            "auto_radio": auto_radio,
            "radio": radio_status,
            "recent_tracks": [
                {**(row_to_item(row) or {}), "started_at": row["started_at"]}
                for row in recent_rows
            ],
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
    added_by = clean_text(payload.get("device_name"), 40, "Navegador")
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
               SET status = 'removed', finished_at = ?, error_message = 'Removed from queue'
             WHERE id = ? AND status = 'pending'
            """,
            (now, queue_id),
        )
        db.commit()
        if cursor.rowcount != 1:
            raise ValueError("The track has already left the queue")
        return {"removed": True, "queue_id": queue_id}


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
                raise ValueError("Invalid control")
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


def search_youtube(query: str) -> list[dict[str, Any]]:
    query = clean_text(query, 120)
    if len(query) < 2:
        raise ValueError("Enter at least two characters")
    cache_key = query.casefold()
    now = time.time()
    with CACHE_LOCK:
        cached = SEARCH_CACHE.get(cache_key)
        if cached and now - cached[0] < 600:
            return cached[1]

    if not SEARCH_SEMAPHORE.acquire(blocking=False):
        raise RuntimeError("The server is already processing other searches")
    try:
        options = youtube_dl_options(
            extract_flat=True,
            skip_download=True,
            playlistend=SEARCH_LIMIT,
            socket_timeout=12,
            retries=1,
        )
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"ytsearch{SEARCH_LIMIT}:{query}", download=False)
        results: list[dict[str, Any]] = []
        for entry in (info or {}).get("entries") or []:
            if not entry:
                continue
            video_id = clean_text(entry.get("id"), 20)
            if not VIDEO_ID_RE.fullmatch(video_id):
                continue
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

    cleaned_title = re.sub(
        r"\s*(?:\[[^\]]*(?:official|video|audio|lyrics?|hd|4k|visualizer|remaster|live)[^\]]*\]|\([^)]*(?:official|video|audio|lyrics?|hd|4k|visualizer|remaster|live|from\b)[^)]*\))\s*$",
        "",
        raw_title,
        flags=re.IGNORECASE,
    ).strip(" -–—")
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
        add_candidate(embedded.group("title"), raw_artist)
        cleaned_title = embedded.group("title").strip()
    add_candidate(cleaned_title, raw_artist)
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
    candidates = lyrics_query_candidates(title, artist)
    request_key = f"{candidates[0][0].casefold()}|{candidates[0][1].casefold()}"
    last_result: dict[str, Any] = {"found": False, "lyrics": "", "source": "LRCLIB"}
    for candidate_title, candidate_artist in candidates:
        result = fetch_lyrics_candidate(candidate_title, candidate_artist)
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
        raise ValueError("Invalid radio URL") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("The radio station does not have a compatible HTTP stream")
    if parsed.username or parsed.password:
        raise ValueError("Radio streams with credentials are not allowed")
    try:
        addresses = socket.getaddrinfo(
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except OSError as exc:
        raise ValueError("Could not resolve the radio server") from exc
    if not addresses:
        raise ValueError("Could not resolve the radio server")
    for address in addresses:
        host = str(address[4][0]).split("%", 1)[0]
        try:
            if not ipaddress.ip_address(host).is_global:
                raise ValueError("The radio stream points to a private network")
        except ValueError as exc:
            if "private network" in str(exc):
                raise
            raise ValueError("Invalid radio stream address") from exc
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
        "name": clean_text(entry.get("name"), 240, "Unnamed radio"),
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
        raise ValueError("Enter at least two characters")
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
        raise ValueError("The radio station does not have a compatible stream")
    station["stream_url"] = public_stream_url(station["stream_url"])
    return station


def add_live_radio_to_queue(payload: dict[str, Any]) -> dict[str, Any]:
    station = get_live_radio_station(payload.get("station_uuid"))
    station_uuid = station["station_uuid"]
    media_id = f"radio:{station_uuid}"
    added_by = clean_text(payload.get("device_name"), 40, "Navegador")
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


def spotify_playlist_preview(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 "
                "Chrome/124.0 Mobile Safari/537.36"
            ),
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=18) as response:
            page = response.read(3_000_000).decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        if exc.code in {401, 403, 404}:
            raise ValueError("The Spotify playlist is private or unavailable") from exc
        raise RuntimeError("Could not query Spotify") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("Could not query Spotify") from exc

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
        raise ValueError("Could not identify tracks in this playlist") from exc

    track_list = entity.get("trackList") if isinstance(entity, dict) else None
    if not isinstance(track_list, list):
        raise ValueError("The Spotify playlist is private, empty, or unavailable")

    tracks: list[dict[str, Any]] = []
    for entry in track_list[:PLAYLIST_IMPORT_LIMIT]:
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
        "title": clean_text(entity.get("name"), 240, "Spotify playlist"),
        "url": url,
        "cover": cover,
        "tracks": tracks,
        "count": len(tracks),
        "truncated": len(track_list) >= PLAYLIST_IMPORT_LIMIT,
    }


def youtube_playlist_preview(url: str) -> dict[str, Any]:
    options = youtube_dl_options(
        extract_flat=True,
        skip_download=True,
        lazy_playlist=False,
        playlistend=PLAYLIST_IMPORT_LIMIT,
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
        "title": clean_text((info or {}).get("title"), 240, "YouTube playlist"),
        "url": url,
        "cover": tracks[0]["thumbnail"],
        "tracks": tracks,
        "count": len(tracks),
        "truncated": len(entries) >= PLAYLIST_IMPORT_LIMIT,
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
        raise RuntimeError("Outra playlist esta sendo identificada. Tente novamente em instantes")
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
    device_name = clean_text(payload.get("device_name"), 40, "Navegador")
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


def get_playlist_job(job_id: str) -> dict[str, Any]:
    with PLAYLIST_LOCK:
        job = PLAYLIST_JOBS.get(clean_text(job_id, 40))
        if not job:
            raise ValueError("Import not found or expired")
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
                "device_name": "Automatic radio",
                "client_id": "auto-radio",
            }
        )
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
            candidates = related_mix(seed)
            if not candidates:
                raise RuntimeError("No related track is available")
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


def resolve_stream(video_id: str, force: bool = False) -> dict[str, Any]:
    if not VIDEO_ID_RE.fullmatch(video_id):
        raise ValueError("Invalid video ID")
    if not STREAM_RESOLVE_LOCK.acquire(timeout=STREAM_RESOLVE_WAIT_SECONDS):
        raise RuntimeError("Audio resolution is busy; try again")
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


class LiveRadioRelay:
    """Um unico FFmpeg recebe a radio e distribui MP3 para os navegadores."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.key: int | None = None
        self.process: subprocess.Popen[bytes] | None = None
        self.subscribers: dict[str, queue_module.Queue[bytes | None]] = {}
        self.started_at: float | None = None
        self.last_error = ""

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
        self.process = None
        self.key = None
        self.started_at = None
        self._close_subscribers_locked()
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
        threading.Thread(
            target=self._reader,
            args=(process, queue_id),
            daemon=True,
            name=f"radio-relay-{queue_id}",
        ).start()

    def _reader(self, process: subprocess.Popen[bytes], queue_id: int) -> None:
        error = ""
        try:
            if process.stdout is None:
                raise RuntimeError("FFmpeg sem saida de audio")
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
                        error = "The radio stream ended"
                    self.last_error = error
                    self.process = None
                    self.key = None
                    self.started_at = None
                    self._close_subscribers_locked()

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
            raise ValueError("Invalid Content-Length") from exc
        if length <= 0 or length > 65536:
            raise ValueError("Invalid request body")
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("Objeto JSON esperado")
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
            if parsed.path == "/api/queue/remove":
                result = remove_from_queue(int(payload.get("queue_id", 0)))
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
            if parsed.path == "/api/control":
                control_playback(clean_text(payload.get("action"), 20))
                self.send_json({"ok": True})
                return
            if parsed.path == "/api/settings":
                if not isinstance(payload.get("auto_radio"), bool):
                    raise ValueError("auto_radio deve ser verdadeiro ou falso")
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
                    )
                )
                return
            if parsed.path == "/api/search":
                results = search_youtube(query.get("q", [""])[0])
                self.send_json({"results": results})
                return
            if parsed.path == "/api/lyrics":
                self.send_json(
                    lyrics_lookup(
                        query.get("title", [""])[0],
                        query.get("artist", [""])[0],
                    )
                )
                return
            if parsed.path == "/api/radio/search":
                results = search_live_radios(query.get("q", [""])[0])
                self.send_json({"results": results})
                return
            if parsed.path == "/api/playlist/preview":
                preview = playlist_preview(query.get("url", [""])[0])
                self.send_json(preview)
                return
            if parsed.path.startswith("/api/playlist/import/"):
                job_id = parsed.path.removeprefix("/api/playlist/import/")
                self.send_json({"job": get_playlist_job(job_id)})
                return
            if parsed.path.startswith("/stream/"):
                video_id = parsed.path.removeprefix("/stream/")
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
            video_id = parsed.path.removeprefix("/stream/") if parsed.path.startswith("/stream/") else ""
            if video_id:
                mark_stream_error(video_id, str(exc))
            message = "Could not obtain audio" if video_id else "Could not search YouTube"
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

    def proxy_stream(self, video_id: str, head_only: bool) -> None:
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
