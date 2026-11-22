# Deployment

## Docker Compose

```bash
git clone https://github.com/lucasnc/self-station.git
cd self-station
docker compose up -d --build
```

The default service listens on port `8090` and persists SQLite data in `/srv/data/self-station`.

Open `http://localhost:8090/` for **In the Black**. Reverse proxies should forward `/in-the-black/`, `/api/`, `/stream/`, and `/relay/` to the same service.

To check deployment, request `/api/health` and confirm that the homepage and its `/in-the-black/` assets return HTTP 200.

Useful commands:

```bash
docker compose ps
docker compose logs -f self-station
docker compose restart self-station
docker compose up -d --build
```

## Reverse Proxy

For public or semi-public access, place Self Station behind a reverse proxy that provides:

- TLS;
- authentication;
- request body size limits;
- access logs.

The app itself is designed for trusted networks and does not provide accounts.

## Data Backup

Back up the mounted data directory:

```bash
sudo tar -czf self-station-data.tgz /srv/data/self-station
```

Stop the container first if you need a strictly consistent SQLite snapshot.
