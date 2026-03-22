# DataMind — Live Streaming Quick Start

Get Kafka streaming + live dashboard running in under 5 minutes.

---

## Prerequisites

```bash
pip install -r requirements.txt
docker --version
```

---

## Step 1 — Start Kafka

```bash
docker compose up -d
docker compose ps
```

Kafka UI: **http://localhost:8080**

---

## Step 2 — Start FastAPI

```bash
uvicorn src.api.main:app --reload --port 8000
```

Live endpoints:
- `GET  /live/status`
- `GET  /live/kpis`
- `GET  /live/transactions`
- `GET  /live/forecast-vs-actual`
- `GET  /live/revenue`
- `GET  /live/top-products`
- `POST /live/control/speed`
- `POST /live/reset`

---

## Step 3 — Start Consumer (new terminal)

```bash
python -m src.streaming.consumer
```

---

## Step 4 — Start Producer (new terminal)

```bash
python -m src.streaming.producer --speed normal
# or
python -m src.streaming.producer --speed fast
# or
python -m src.streaming.producer --speed burst
```

---

## Step 5 — Start React Frontend

```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173** and switch to the **Live Feed** tab.

---

## Change Speed at Runtime

```bash
curl -X POST http://localhost:8000/live/control/speed \
  -H "Content-Type: application/json" \
  -d '{"speed_mode":"fast"}'
```

---

## Reset Live Data

```bash
curl -X POST http://localhost:8000/live/reset \
  -H "Content-Type: application/json" \
  -d '{"confirm":true}'
```

---

## Stop

```bash
# stop producer + consumer terminals (Ctrl+C)
docker compose down
```
