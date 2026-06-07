"""
preprocessing.py
================
Loads raw Danish AIS CSV files and returns a clean, filtered Spark DataFrame
ready for collision detection.

Key performance decisions
--------------------------
1. Coalesce to LOAD_PARTITIONS immediately after CSV read.
   Spark creates one partition per HDFS block (~128 MB), so 31 files × ~400 MB
   = ~100+ partitions by default.  Every Python UDF spawns one worker process
   per partition; 709 workers × resident memory = OOM.  Coalescing first caps
   the worker count.

2. Haversine uses native Spark SQL math (sin, cos, asin, sqrt, radians).
   No Python UDF is called for the 50-nm circle check, so no Python worker
   processes are spawned at that stage.

3. No df.cache() — the filtered study-area dataset is small enough to
   recompute cheaply if needed.  Caching a huge intermediate set caused OOM.

Pipeline
--------
1. CSV glob load (*.csv only — skips zip files)
2. Coalesce to LOAD_PARTITIONS
3. Header normalisation
4. Type casting
5. Vessel-type filter (Class A / Class B)
6. Temporal filter
7. Null / invalid coordinate removal
8. Cheap bounding-box pre-filter
9. Exact 50-nm Haversine filter (native Spark SQL math — no Python UDF)
10. Stationary-vessel exclusion (SOG + nav status)
11. SOG-based GPS anomaly removal
12. Deduplication via dropDuplicates
"""

import math
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Study-area constants
# ---------------------------------------------------------------------------
CENTER_LAT = 55.225000
CENTER_LON = 14.245000
RADIUS_NM  = 50.0
RADIUS_KM  = RADIUS_NM * 1.852          # 92.6 km
EARTH_R_KM = 6371.0

KM_PER_DEG_LAT = 111.32
LAT_SLACK = RADIUS_KM / KM_PER_DEG_LAT
LON_SLACK = RADIUS_KM / (KM_PER_DEG_LAT * math.cos(math.radians(CENTER_LAT)))

# ---------------------------------------------------------------------------
# Configurable
# ---------------------------------------------------------------------------
TARGET_YEAR   = int(os.environ.get("TARGET_YEAR",       "0"))   # 0 = any year
TARGET_MONTH  = int(os.environ.get("TARGET_MONTH",      "12"))
LOAD_PARTITIONS = int(os.environ.get("LOAD_PARTITIONS", "16"))   # partition cap

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
MIN_SOG_KNOTS = 0.5
MAX_SOG_KNOTS = 102.0   # physically impossible → bad fix

STATIONARY_NAV_STATUSES = {
    "At anchor",
    "Moored",
    "Aground",
    "Not under command",
    "Reserved for future use",
    "Unknown value",
    # "Restricted maneuverability" covers survey vessels, dredgers, and cable
    # layers that are constrained by their work and not freely navigating.
    # A supply boat docking alongside such a vessel (common offshore operation)
    # would otherwise appear as a "collision" candidate.  The assignment
    # explicitly requires excluding vessels "safely docked adjacent to one
    # another", and this status captures that operational category.
    "Restricted maneuverability",
}
MOVING_MOBILE_TYPES = {"Class A", "Class B"}


# ---------------------------------------------------------------------------
# Native-Spark Haversine column expression  (NO Python UDF)
# ---------------------------------------------------------------------------
def _haversine_km_col(lat_col, lon_col):
    """
    Return a Spark Column with the Haversine distance in km from
    (CENTER_LAT, CENTER_LON) to each row's (lat_col, lon_col).

    Uses only built-in Spark SQL math functions — no Python UDF, no
    worker-process spawning, runs entirely inside the JVM.
    """
    R    = EARTH_R_KM
    phi1 = math.radians(CENTER_LAT)
    lam1 = math.radians(CENTER_LON)

    phi2 = F.radians(lat_col)
    lam2 = F.radians(lon_col)

    dphi = phi2 - F.lit(phi1)
    dlam = lam2 - F.lit(lam1)

    a = (
        F.pow(F.sin(dphi / 2), 2)
        + F.lit(math.cos(phi1)) * F.cos(phi2) * F.pow(F.sin(dlam / 2), 2)
    )
    return F.lit(2 * R) * F.asin(F.sqrt(a))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def load_and_preprocess(spark: SparkSession, data_path: str):
    """
    Return a filtered Spark DataFrame with columns:
        mmsi, ts, lat, lon, sog, cog, heading, nav_status, name, ship_type
    """

    # ------------------------------------------------------------------
    # 1. Load only *.csv files
    # ------------------------------------------------------------------
    # Accept either a directory (glob for *.csv) or a direct file path
    csv_glob = data_path if data_path.endswith(".csv") else data_path.rstrip("/") + "/*.csv"

    raw = (
        spark.read
        .option("header",    "true")
        .option("mode",      "PERMISSIVE")
        .csv(csv_glob)
    )

    if not raw.columns:
        raise RuntimeError(f"No CSV files found at: {csv_glob}")

    # Do NOT coalesce here. Spark creates ~500-700 small partitions (one per
    # 128 MB CSV block). Each task reads ~128 MB, applies filters, and outputs
    # a tiny fraction. Peak memory per task is small. We coalesce AFTER the
    # geographic filter when the surviving data is already tiny.

    # ------------------------------------------------------------------
    # 2. Normalise column names  ("# Timestamp" -> "Timestamp", spaces -> _)
    # ------------------------------------------------------------------
    for col in raw.columns:
        clean = col.lstrip("#").strip().replace(" ", "_")
        if clean != col:
            raw = raw.withColumnRenamed(col, clean)

    print(f"[preprocessing] Loaded {len(raw.columns)} columns. Processing ...")

    # ------------------------------------------------------------------
    # 4. Cast numeric types
    # ------------------------------------------------------------------
    for c in ("Latitude", "Longitude", "SOG", "COG", "Heading"):
        if c in raw.columns:
            raw = raw.withColumn(c, F.col(c).cast(DoubleType()))

    raw = raw.withColumn(
        "ts",
        F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm:ss")
    )

    # ------------------------------------------------------------------
    # 5. Vessel-type filter
    # ------------------------------------------------------------------
    if "Type_of_mobile" in raw.columns:
        raw = raw.filter(F.col("Type_of_mobile").isin(list(MOVING_MOBILE_TYPES)))

    # ------------------------------------------------------------------
    # 6. Temporal filter
    # ------------------------------------------------------------------
    raw = raw.filter(F.month("ts") == TARGET_MONTH)
    if TARGET_YEAR > 0:
        raw = raw.filter(F.year("ts") == TARGET_YEAR)

    # ------------------------------------------------------------------
    # 7. Drop null / invalid coordinates
    # ------------------------------------------------------------------
    raw = raw.filter(
        F.col("Latitude").isNotNull()  & F.col("Longitude").isNotNull() &
        (F.col("Latitude")  >= -90)    & (F.col("Latitude")  <= 90)    &
        (F.col("Longitude") >= -180)   & (F.col("Longitude") <= 180)   &
        ~((F.col("Latitude") == 0.0)   & (F.col("Longitude") == 0.0))
    )

    # ------------------------------------------------------------------
    # 7b. Exclude SAR aircraft MMSIs (ITU standard: 111xxxxxx = SAR aircraft).
    #     These appear as "Class A" in AIS feeds but are not vessels.
    #     Their positions are frequently relayed through nearby ship transponders,
    #     producing artefact near-zero-distance "collisions" with the relay ship.
    # ------------------------------------------------------------------
    raw = raw.filter(
        ~(F.col("MMSI").cast("string").startswith("111") &
          (F.length(F.col("MMSI").cast("string")) == 9))
    )

    # ------------------------------------------------------------------
    # 8. Bounding-box pre-filter  (pure column comparisons, very fast)
    # ------------------------------------------------------------------
    raw = raw.filter(
        (F.col("Latitude")  >= CENTER_LAT - LAT_SLACK) &
        (F.col("Latitude")  <= CENTER_LAT + LAT_SLACK) &
        (F.col("Longitude") >= CENTER_LON - LON_SLACK) &
        (F.col("Longitude") <= CENTER_LON + LON_SLACK)
    )

    # ------------------------------------------------------------------
    # 9. Exact 50-nm Haversine filter  (native Spark SQL — no Python UDF)
    # ------------------------------------------------------------------
    raw = raw.withColumn(
        "_dist_km",
        _haversine_km_col(F.col("Latitude"), F.col("Longitude"))
    ).filter(F.col("_dist_km") <= RADIUS_KM).drop("_dist_km")

    # ------------------------------------------------------------------
    # 10. Stationary-vessel exclusion
    # ------------------------------------------------------------------
    sog_ok = F.col("SOG") >= MIN_SOG_KNOTS
    if "Navigational_status" in raw.columns:
        raw = raw.filter(
            sog_ok & ~F.col("Navigational_status").isin(list(STATIONARY_NAV_STATUSES))
        )
    else:
        raw = raw.filter(sog_ok)

    # ------------------------------------------------------------------
    # 11. SOG-based GPS anomaly removal  (no Window / sort needed)
    # ------------------------------------------------------------------
    raw = raw.filter(F.col("SOG").isNotNull() & (F.col("SOG") <= MAX_SOG_KNOTS))

    # ------------------------------------------------------------------
    # 12. Select → canonical schema
    # ------------------------------------------------------------------
    def _col_or_null(name, dtype="string"):
        return F.col(name) if name in raw.columns else F.lit(None).cast(dtype)

    df = raw.select(
        F.col("MMSI").cast("long").alias("mmsi"),
        F.col("ts"),
        F.col("Latitude").alias("lat"),
        F.col("Longitude").alias("lon"),
        F.col("SOG").alias("sog"),
        F.col("COG").alias("cog"),
        _col_or_null("Heading",             "double").alias("heading"),
        _col_or_null("Navigational_status", "string").alias("nav_status"),
        _col_or_null("Name",                "string").alias("name"),
        _col_or_null("Ship_type",           "string").alias("ship_type"),
    )

    df = df.filter(F.col("mmsi").isNotNull() & F.col("ts").isNotNull())

    # ------------------------------------------------------------------
    # 13. Coalesce NOW — after geographic + all other filters the surviving
    #     data is tiny. Merging into LOAD_PARTITIONS partitions speeds up
    #     the subsequent dropDuplicates shuffle and the Parquet write.
    # ------------------------------------------------------------------
    df = df.coalesce(LOAD_PARTITIONS)
    print(f"[preprocessing] Coalesced filtered data to {LOAD_PARTITIONS} partitions")

    # ------------------------------------------------------------------
    # 14. Deduplication  (hash-based, no sort / Window)
    # ------------------------------------------------------------------
    df = df.dropDuplicates(["mmsi", "ts"])

    return df
