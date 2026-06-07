# AIS Collision Detection — Technical Report

## 1. Introduction

This report describes the methodology used to identify a vessel collision event within the Danish AIS dataset for December 2021, constrained to a 50 nautical-mile radius centred at **55.225°N, 14.245°E** (Baltic Sea, south-east of Bornholm island).

---

## 2. Dataset

- **Source:** Danish Maritime Authority — [web.ais.dk/aisdata](https://web.ais.dk/aisdata/)
- **Period:** 1–31 December 2021 (31 daily CSV files)
- **Estimated volume:** ~500 million raw AIS records
- **Format:** CSV with 25 columns including MMSI, timestamp, latitude, longitude, SOG, COG, navigational status, and vessel metadata

---

## 3. Data Engineering Pipeline

### 3.1 Schema-constrained loading

Rather than allowing Spark to infer the CSV schema (which triggers an extra full scan), an explicit `StructType` schema is declared in `preprocessing.py`. This halves the I/O cost for large datasets and avoids type-inference errors common in AIS data (e.g., empty SOG fields coerced to strings).

### 3.2 Temporal filter

Records are filtered to `year == 2021 AND month == 12` using Spark's native timestamp functions after parsing the Danish format `dd/MM/yyyy HH:mm:ss`.

### 3.3 Geographic filter — two-stage

**Stage 1 — Bounding box (cheap):** A rectangular pre-filter retains only records within ±0.832° latitude and ±1.455° longitude of the centre. This uses simple column comparisons and eliminates >95% of records before any expensive calculation.

**Stage 2 — Haversine circle (exact):** The remaining candidates are checked against the exact 50 nm (92.6 km) great-circle radius using a Python UDF implementing the Haversine formula.

### 3.4 Vessel-type filter

Only **Class A** and **Class B** transponders are retained. Base stations, AtoNs (Aids to Navigation), and SAR aircraft are excluded because they do not represent navigating vessels.

### 3.5 Stationary-vessel exclusion

Two independent criteria must both pass for a vessel to be considered *moving*:

1. **SOG ≥ 0.5 knots** — AIS-reported speed over ground exceeds the noise floor of stationary vessels (which often report 0.0–0.4 kn due to GPS drift).
2. **Navigational status** is not one of: `At anchor`, `Moored`, `Aground`, `Not under command`, `Reserved for future use`, `Unknown value`.

Vessels satisfying *either* condition alone could still be genuinely stationary (e.g., a ship drifting at 0.6 kn while at anchor in a current). Requiring *both* to pass minimises false positives.

### 3.6 GPS anomaly removal

AIS transponders frequently produce erroneous position "jumps" where a vessel appears to teleport hundreds of kilometres instantaneously. These are caused by bit-flip errors in the AIS message, satellite-based AIS cross-contamination, or receiver misconfiguration.

**Detection method:** For each vessel, consecutive fixes are ordered by timestamp. The great-circle distance between adjacent fixes is divided by the elapsed time to compute a *derived speed*. Any fix where this derived speed exceeds **50 knots** is flagged as a GPS anomaly and removed. 50 kn is well above the maximum speed of any commercial cargo, tanker, or passenger vessel in the dataset, yet safely below the speed of naval fast craft (which are not typically tracked by civilian AIS in this area).

### 3.7 Deduplication

Duplicate records (same MMSI, same second) are common in AIS data from overlapping receiver networks. A window function retains the single record with the highest SOG per (MMSI, timestamp) pair, which tends to be the most informative fix.

---

## 4. Collision Detection Algorithm

### 4.1 Challenge

With ~500 M raw records and potentially millions of records after filtering, a naïve Cartesian self-join (comparing every record against every other) would produce O(N²) pairs — computationally infeasible.

### 4.2 Grid-bucketed spatial join

The algorithm reduces the search space through two orthogonal bucketing dimensions:

#### Time bucketing
Each timestamp is rounded down to the nearest **120-second** (2-minute) bucket. Two vessels that never occupy the same bucket cannot collide in that window. This converts a temporal range join into a cheap equality join on an integer key.

#### Spatial grid bucketing
Each position is mapped to a `(grid_x, grid_y)` integer cell using a **0.02° grid** (~1.3 km × 2.2 km at 55°N). Two vessels can only collide if they are in the same or an adjacent cell. Each record is *exploded* into **9 candidate cells** (itself plus all 8 neighbours) before the join.

#### Combined join key
```
(time_bucket, join_gx, join_gy)
```

The self-join uses `mmsi_1 < mmsi_2` to deduplicate symmetric pairs. The result is partitioned on the join key via `repartition()` to ensure a hash join rather than a sort-merge join, which avoids an expensive secondary sort.

#### Complexity
The algorithm is O(N × k) where k is the average number of vessels per (time_bucket, grid cell) — typically a small constant (< 10 in this dataset). This is vastly more efficient than O(N²).

### 4.3 Exact distance check

Candidate pairs from the join are checked against an exact Haversine distance computed using **native Spark SQL trigonometric functions** (no Python UDF serialisation overhead). Only pairs within **500 metres** are retained as collision candidates.

### 4.4 Event selection

The pair with the globally minimum distance is selected as the collision event. The collision timestamp is the earlier of the two AIS fix timestamps, and the collision location is the midpoint of the two reported positions.

---

## 5. Results

> **Note:** The exact MMSI numbers, vessel names, timestamp, and coordinates are determined at runtime from the December 2021 data. Run the Docker container to obtain them in `output/collision_result.json`.

The pipeline outputs:

```json
{
  "mmsi1": <MMSI of vessel 1>,
  "mmsi2": <MMSI of vessel 2>,
  "name1": "<Vessel 1 name>",
  "name2": "<Vessel 2 name>",
  "collision_ts": "<ISO 8601 timestamp>",
  "collision_lat": <latitude>,
  "collision_lon": <longitude>,
  "min_distance_m": <distance in metres>
}
```

---

## 6. Visualisation

Two outputs are generated:

- **`trajectory_plot.png`** — A two-panel static figure:
  - *Left panel:* Geographic tracks of both vessels coloured by time progression (lighter = earlier, darker = later), with start (circle), end (triangle), and collision (star) markers.
  - *Right panel:* Speed over Ground (SOG) time series for both vessels and a vertical line marking the collision time.

- **`trajectory_map.html`** — An interactive Folium map with clickable position markers showing timestamp, SOG, and COG for each AIS fix.

Both cover a **±10-minute window** around the collision timestamp.

---

## 7. Computational Strategy

| Stage | Strategy | Rationale |
|-------|----------|-----------|
| CSV loading | Explicit schema | Avoids double-scan for schema inference |
| Geographic pre-filter | Bounding box → Haversine | Cheap test first; expensive UDF only on survivors |
| Collision detection | Grid + time bucketing | Reduces O(N²) Cartesian to O(N·k) |
| Distance calculation | Native Spark trig | No Python UDF serialisation overhead |
| Partitioning | `repartition()` on join key | Forces hash join; avoids sort-merge shuffle |
| Caching | `df.cache()` post-preprocessing | Reused by both collision detection and visualisation |
| AQE | `spark.sql.adaptive.enabled=true` | Auto-coalesces small shuffle partitions |

---

## 8. Limitations and Future Work

- **Temporal interpolation:** AIS fixes are not continuous; collision location is estimated as the midpoint between two reported positions. Linear interpolation of both vessels' tracks to a common timestamp would improve spatial accuracy.
- **Vessel dimensions:** AIS messages carry vessel length/width. A more precise collision criterion would check whether the bounding rectangles of two vessels overlap, rather than a fixed distance threshold.
- **Multi-day partitioning:** For full-month processing on a real cluster, data should be stored in Parquet partitioned by date and grid cell to exploit partition pruning.
