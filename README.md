# AIS Collision Detection

Identifies the two vessels that experienced the closest physical proximity (collision event) in the Baltic Sea during **December 2021**, using Danish AIS data processed with **Apache Spark / PySpark** inside a **Docker** container.

---

## Requirements

| Tool | Minimum version |
|------|----------------|
| Docker | 20.10 |
| Docker Compose | 2.x |
| RAM (host) | 10 GB recommended |

---

## Data preparation

1. Download the daily AIS CSV files for **December 2021** from the Danish Maritime Authority:
   ```
   https://web.ais.dk/aisdata/
   ```
   Files are named `aisdk-2021-12-01.csv` through `aisdk-2021-12-31.csv`.

2. Place **all 31 files** inside the `data/` directory:
   ```
   BDA/
   └── data/
       ├── aisdk-2021-12-01.csv
       ├── aisdk-2021-12-02.csv
       …
       └── aisdk-2021-12-31.csv
   ```

---

## Build and run

### Option A — Pull from Docker Hub (fastest)

A pre-built image is available on Docker Hub:

```bash
docker pull jonuxs/ais-collision-detection:latest
```

Run it:

```bash
docker run --rm \
  --memory=10g \
  -e JAVA_TOOL_OPTIONS="-Xmx6g" \
  -e DATA_PATH=/data \
  -e OUTPUT_PATH=/output \
  -v "$(pwd)/data":/data:ro \
  -v "$(pwd)/output":/output \
  jonuxs/ais-collision-detection:latest
```

---

### Option B — Docker Compose (recommended for local builds)

```bash
# 1. Clone / navigate to project root
cd BDA

# 2. Build the image and start the container
docker compose up --build

# Results appear in ./output/ when complete

# To stop and remove the container afterwards:
docker compose down
```

> **Note:** Always run `docker compose down` before re-running to ensure the
> container is fully recreated from the latest image:
> ```bash
> docker compose down && docker compose up --build
> ```


---

## Outputs

After the container finishes, three files appear in `./output/`:

| File | Description |
|------|-------------|
| `collision_result.json` | MMSI numbers, vessel names, timestamp, coordinates, closest-approach distance |
| `trajectory_plot.png` | Static two-panel figure: geographic tracks + SOG time series (±10 min) |
| `trajectory_map.html` | Interactive Folium map — open in any browser |

---

## Results

| Field | Value |
|-------|-------|
| Vessel 1 | **KARIN HOEJ** (MMSI 219021240) |
| Vessel 2 | **MV SCOT CARRIER** (MMSI 232018267) |
| Timestamp | 2021-12-13 02:27:29 UTC |
| Latitude | 55.223079° N |
| Longitude | 14.243707° E |
| Closest approach | ~4.1 m |

---

## Configuration

Environment variables accepted by the container:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PATH` | `/data` | Directory containing AIS CSV files |
| `OUTPUT_PATH` | `/output` | Directory where results are written |
| `JAVA_TOOL_OPTIONS` | `"-Xmx6g"` | JVM heap limit (set before JVM launch) |
| `TARGET_YEAR` | `2021` | Filter to this year (0 = any) |
| `TARGET_MONTH` | `12` | Filter to this month |
| `LOAD_PARTITIONS` | `16` | Spark partition count after CSV load |

---

## Project structure

```
BDA/
├── src/
│   ├── main.py                # Pipeline entry point (3-phase architecture)
│   ├── preprocessing.py       # CSV load, geographic + quality filtering
│   ├── collision_detection.py # Bucketed spatial-temporal self-join
│   └── visualization.py       # Matplotlib + Folium trajectory plots
├── data/                      # Place AIS CSV files here (gitignored)
├── output/                    # Results written here (gitignored)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── README.md
└── REPORT.md
```

---
