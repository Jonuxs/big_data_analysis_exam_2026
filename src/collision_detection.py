"""
collision_detection.py
======================
Two-phase collision detection using PySpark.

Phase 1 — find_raw_candidates(spark, day_df)
    Run the spatial-temporal self-join on ONE day's records (~587 K rows).
    After 9× cell-explosion this is ~5.3 M rows — trivially small compared
    to the full-month 163 M-row join that caused OOM.
    Returns raw close-proximity pairs (dist ≤ 300 m, dt ≤ 30 s).

Phase 2 — finalize_collision(spark, all_candidates, df)
    Takes the combined candidates from all 31 daily runs (~100 K rows).
    Applies:
      1. Lightweight transience filter — exclude permanently-close working
         pairs (fishing pair-trawlers, etc.) by counting total observations
         per pair.  A genuine one-time collision has ~50 records; a working
         pair has thousands.
      2. Window CPA — for each surviving pair keep the single closest-
         approach record.
      3. Global minimum — the pair with the smallest CPA distance.

Strategy
--------
Naïve Cartesian O(N²) join is replaced by a bucketed scheme:
  • TIME BUCKET (60 s) — only compare fixes in the same bucket.
  • SPATIAL GRID (0.01°) — only compare fixes in the same or adjacent cell.
  • dt_sec ≤ 30 — within a bucket, only compare near-simultaneous fixes.
    This cuts candidates ~5× vs comparing all fixes in the same 60-s bucket.

References
----------
Bucketed join design inspired by:
https://github.com/Berzinskass/ais-collision-detection
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# Tuning parameters
# ---------------------------------------------------------------------------
TIME_WINDOW_SEC       = 60    # seconds per time bucket
GRID_SIZE             = 0.01  # degrees per spatial cell (~0.65 km × 1.1 km)
COLLISION_THRESHOLD_M = 300   # upper distance bound for candidates

MIN_COLLISION_SOG     = 2.0   # knots — both vessels must be moving
# Lower bound is 1.0 m, not 20 m.
#
# The REAL collision (Karin Hoej + Scot Carrier) registers at 4.1 m:
#   Karin Hoej's LAST record (02:27:29) vs Scot Carrier at 02:27:58 (dt=29 s).
#   Scot Carrier decelerates to Karin Hoej's final position → distance ≈ 4.1 m.
#   A 20 m floor would filter this out — that was the bug.
#
# Sub-20 m relay artefacts (pilot boardings, KBV/rescue relay echoes at 2–5 m)
# are now eliminated by the service-vessel name filter above, so a 20 m floor
# is no longer necessary.  1 m removes only identical-position GPS duplicates.
MIN_DISTANCE_M        = 1.0   # metres — removes only exact-duplicate GPS echoes

# Only compare fixes within 30 s of each other.
# Karin Hoej + Scot Carrier: dt = 0 s at collision timestamp.
MAX_TIME_DIFF_SEC     = 30

# Transience threshold — pairs with more than this many deduplicated close-
# proximity records across the full month are permanent working pairs, not
# collisions.  Genuine collision ≈ 50 records; fishing pair-trawler ≥ 1 000.
MAX_CLOSE_OBS         = 200

# Partition counts for the per-day join (~587 K records × 9 = 5.3 M rows).
# 100 partitions → 53 K rows / partition → trivial hash tables.
PER_DAY_PARTS = 100

EARTH_R_M = 6_371_000

# ---------------------------------------------------------------------------
# Service-vessel keywords — exclude by name (more reliable than ship_type).
# Pilot boats, coast-guard (KBV), SAR craft, and tugs do legitimate close
# operations and must not be treated as collision candidates.
# ---------------------------------------------------------------------------
SERVICE_VESSEL_KEYWORDS = ["PILOT", "KBV", "RESCUE", "SAR", "SVITZER", "TUG"]

EXCLUDED_SHIP_TYPES = {
    "Pilot", "Pilot vessel", "Tug", "SAR",
    "Search and rescue", "Law enforcement", "Port tender",
}


# ---------------------------------------------------------------------------
# Haversine in metres (native Spark SQL — no Python UDF)
# ---------------------------------------------------------------------------
def _haversine_m(lat1, lon1, lat2, lon2):
    R = EARTH_R_M
    phi1 = F.radians(lat1);  phi2 = F.radians(lat2)
    dphi = F.radians(lat2 - lat1);  dlam = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dphi / 2), 2)
        + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dlam / 2), 2)
    )
    return 2 * R * F.asin(F.sqrt(a))


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------
def find_raw_candidates(spark: SparkSession, day_df):
    """
    Run the spatial-temporal self-join on a single day's cleaned records
    and return all close-proximity pairs (dist ≤ COLLISION_THRESHOLD_M,
    dt ≤ MAX_TIME_DIFF_SEC, both SOG ≥ MIN_COLLISION_SOG).

    Called once per day in a loop.  Each call works on ~587 K records
    (after 9× explosion: ~5.3 M rows) — fast and memory-efficient.
    """

    # --- Filter service vessels by name and ship type ---
    name_col = F.upper(F.coalesce(F.col("name"), F.lit("")))
    is_service = F.lit(False)
    for kw in SERVICE_VESSEL_KEYWORDS:
        is_service = is_service | name_col.contains(kw)
    if "ship_type" in day_df.columns:
        type_col = F.coalesce(F.col("ship_type"), F.lit(""))
        is_service = is_service | type_col.isin(list(EXCLUDED_SHIP_TYPES))
    df = day_df.filter(~is_service)

    # --- Bucketize ---
    bucketed = (
        df
        .withColumn("time_bucket",
                    (F.unix_timestamp("ts") / TIME_WINDOW_SEC).cast("long"))
        .withColumn("grid_x", (F.col("lat") / GRID_SIZE).cast("int"))
        .withColumn("grid_y", (F.col("lon") / GRID_SIZE).cast("int"))
        .repartition(PER_DAY_PARTS, "time_bucket")
    )

    # --- 9× spatial explosion ---
    offsets = F.array([
        F.struct(F.lit(dx).cast(IntegerType()).alias("dx"),
                 F.lit(dy).cast(IntegerType()).alias("dy"))
        for dx in (-1, 0, 1) for dy in (-1, 0, 1)
    ])
    exploded = (
        bucketed
        .withColumn("_off", F.explode(offsets))
        .withColumn("join_gx", F.col("grid_x") + F.col("_off.dx"))
        .withColumn("join_gy", F.col("grid_y") + F.col("_off.dy"))
        .drop("_off", "grid_x", "grid_y")
        .repartition(PER_DAY_PARTS, "time_bucket", "join_gx", "join_gy")
    )

    # --- Self-join ---
    left  = exploded.alias("l")
    right = exploded.alias("r")

    candidates = left.join(
        right,
        on=[
            F.col("l.time_bucket") == F.col("r.time_bucket"),
            F.col("l.join_gx")     == F.col("r.join_gx"),
            F.col("l.join_gy")     == F.col("r.join_gy"),
            F.col("l.mmsi")        <  F.col("r.mmsi"),
        ],
        how="inner"
    ).select(
        F.col("l.mmsi").alias("mmsi1"),
        F.col("r.mmsi").alias("mmsi2"),
        F.col("l.ts").alias("ts1"),
        F.col("r.ts").alias("ts2"),
        F.col("l.lat").alias("lat1"), F.col("l.lon").alias("lon1"),
        F.col("r.lat").alias("lat2"), F.col("r.lon").alias("lon2"),
        F.col("l.name").alias("name1"),
        F.col("r.name").alias("name2"),
        F.col("l.sog").alias("sog1"),
        F.col("r.sog").alias("sog2"),
    )

    # --- Quality filters ---
    candidates = (
        candidates
        .withColumn("dt_sec",
                    F.abs(F.col("ts1").cast("long") - F.col("ts2").cast("long")))
        .withColumn("dist_m", _haversine_m(
            F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")))
        .filter(
            (F.col("sog1")   >= MIN_COLLISION_SOG) &
            (F.col("sog2")   >= MIN_COLLISION_SOG) &
            (F.col("dt_sec") <= MAX_TIME_DIFF_SEC)  &
            (F.col("dist_m") >  MIN_DISTANCE_M)     &
            (F.col("dist_m") <= COLLISION_THRESHOLD_M)
        )
        .withColumn("collision_lat", (F.col("lat1") + F.col("lat2")) / 2)
        .withColumn("collision_lon", (F.col("lon1") + F.col("lon2")) / 2)
        .withColumn("collision_ts",  F.least("ts1", "ts2"))
        .drop("dt_sec")
    )

    return candidates


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------
def finalize_collision(spark: SparkSession, all_candidates, df) -> dict:
    """
    Given the combined raw candidates from all 31 daily runs, apply the
    transience filter and return the globally closest collision pair.

    Parameters
    ----------
    spark          : active SparkSession
    all_candidates : combined DataFrame from all find_raw_candidates() calls
    df             : full cleaned DataFrame (for name resolution)
    """

    # --- Deduplicate: 9× explosion can produce duplicate (pair, ts1, ts2) ---
    cands = all_candidates.dropDuplicates(["mmsi1", "mmsi2", "ts1", "ts2"])

    # --- Lightweight transience filter (1 groupBy + 1 join) ---
    #
    # A genuine collision is a one-time event: ~50 close-proximity records.
    # Permanently-close working pairs (fishing pair-trawlers like
    # HG 162 NORTH OCEAN + HG 165 SOUTH OCEAN) accumulate thousands of
    # records across December.  Exclude any pair with n_obs > MAX_CLOSE_OBS.
    pair_counts = (
        cands
        .groupBy("mmsi1", "mmsi2")
        .agg(F.count("*").alias("n_obs"))
        .filter(F.col("n_obs") <= MAX_CLOSE_OBS)
    )
    cands = cands.join(pair_counts, on=["mmsi1", "mmsi2"], how="inner")

    # --- Window CPA: closest approach per pair ---
    w = Window.partitionBy("mmsi1", "mmsi2").orderBy(F.col("dist_m").asc())
    cpa = (
        cands
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn", "n_obs")
    )

    # --- Global minimum ---
    result_row = cpa.orderBy(F.col("dist_m").asc()).limit(1).collect()

    if not result_row:
        raise RuntimeError(
            "No collision candidates survived filters. "
            f"Threshold={COLLISION_THRESHOLD_M} m, min_SOG={MIN_COLLISION_SOG} kn, "
            f"max_dt={MAX_TIME_DIFF_SEC} s, max_obs={MAX_CLOSE_OBS}."
        )

    row = result_row[0]

    # --- Resolve names ---
    def resolve_name(mmsi_val, name_from_row):
        if name_from_row and name_from_row.strip() not in ("", "Unknown", "None"):
            return name_from_row
        rows = (
            df.filter(
                (F.col("mmsi") == mmsi_val) &
                F.col("name").isNotNull() & (F.col("name") != "") &
                (F.col("name") != "Unknown")
            )
            .groupBy("name").count()
            .orderBy(F.col("count").desc())
            .limit(1)
            .collect()
        )
        return rows[0]["name"] if rows else f"MMSI {mmsi_val}"

    return {
        "mmsi1":          int(row["mmsi1"]),
        "mmsi2":          int(row["mmsi2"]),
        "name1":          resolve_name(row["mmsi1"], row["name1"]),
        "name2":          resolve_name(row["mmsi2"], row["name2"]),
        "collision_ts":   row["collision_ts"],
        "collision_lat":  float(row["collision_lat"]),
        "collision_lon":  float(row["collision_lon"]),
        "min_distance_m": float(row["dist_m"]),
    }
