# AIS Collision Detection

Identifies the two vessels that experienced the closest physical proximity (collision event) in the Baltic Sea during **December 2021**, using Danish AIS data processed with **Apache Spark / PySpark** inside a **Docker** container.

---

## Requirements

| Tool | Minimum version |
|------|----------------|
| Docker | 20.10 |
| Docker Compose | 2.x |
| RAM (host) | 8 GB recommended |

---

## Data preparation

1. Download the daily AIS CSV files for **December 2021** from the Danish Maritime Authority:
   ```
   https://web.ais.dk/aisdata/
   ```
   Files are named `aisdk-2021-12-01.csv` through `aisdk-2021-12-31.csv`.

2. Place **all 31 files** inside the `data/` directory:
   ```
   ais-collision-detection/
   └── data/
       ├── aisdk-2021-12-01.csv
       ├── aisdk-2021-12-02.csv
       …
       └── aisdk-2021-12-31.csv
   ```

---

## Build and run

### Option A — Docker Compose (recommended)

```bash
# Clone / navigate to project root
cd ais-collision-detection

# Build image and start container
docker compose up --build

# Results appear in ./output/ when done
```

### Option B — Docker CLI

```bash
# Build
docker build -t ais-collision-detection:latest .

# Run  (replace $(pwd) with absolute paths on Windows)
docker run --rm \
  -v "$(pwd)/data":/data:ro \
  -v "$(pwd)/output":/output \
  -e JAVA_OPTS="-Xmx6g -Xms2g" \
  ais-collision-detection:latest
```

### Option C — Using a pre-built image from Docker Hub

```bash
docker pull <your-dockerhub-username>/ais-collision-detection:latest

docker run --rm \
  -v "$(pwd)/data":/data:ro \
  -v "$(pwd)/output":/output \
  <your-dockerhub-username>/ais-collision-detection:latest
```

---

## Outputs

After the container finishes, three files appear in `./output/`:

| File | Description |
|------|-------------|
| `collision_result.json` | Machine-readable summary: MMSI numbers, vessel names, timestamp, coordinates, distance |
| `trajectory_plot.png` | Static two-panel figure: geographic tracks + SOG time series (±10 min) |
| `trajectory_map.html` | Interactive Folium map — open in any browser |

---

## Configuration

Override defaults via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_PATH` | `/data` | Directory of AIS CSV files |
| `OUTPUT_PATH` | `/output` | Output directory |
| `JAVA_OPTS` | `-Xmx4g -Xms1g` | JVM heap settings |

---

## Project structure

```
ais-collision-detection/
├── src/
│   ├── main.py                # Pipeline entry point
│   ├── preprocessing.py       # Data loading, cleaning, filtering
│   ├── collision_detection.py # Grid-bucketed spatial self-join
│   └── visualization.py      # Matplotlib + Folium plots
├── data/                      # Place AIS CSV files here (gitignored)
├── output/                    # Results written here (gitignored)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── README.md
└── REPORT.md
```

---

## Push to Docker Hub

```bash
docker build -t <username>/ais-collision-detection:latest .
docker push <username>/ais-collision-detection:latest
```

---

## Running tests locally (without Docker)

```bash
pip install -r requirements.txt
cd src
DATA_PATH=../data OUTPUT_PATH=../output python main.py
```
