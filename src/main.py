"""
main.py
=======
Entry point for the AIS Collision Detection pipeline.

Architecture
------------
The full-month self-join (18.2 M records × 9× explosion = 163 M rows) exceeds
the available JVM heap in a single pass.  We therefore split Phase 2 into:

  Phase 2a — per-day candidate finding:
    Each daily Parquet (~587 K records → ~5.3 M after explosion) is joined
    independently.  A per-day join is ~31× smaller and completes in seconds.

  Phase 2b — global finalization:
    All daily raw candidates are combined (~100 K total rows) and the
    transience filter + CPA Window are applied on this tiny dataset.

Usage (inside Docker):
    python main.py [--data /data] [--output /output]

Environment variables:
    DATA_PATH   — directory containing AIS CSV files  (default: /data)
    OUTPUT_PATH — directory for results and plots      (default: /output)
"""

import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession

from preprocessing import load_and_preprocess
from collision_detection import find_raw_candidates, finalize_collision
from visualization import plot_trajectories


# ---------------------------------------------------------------------------
# Spark session builders
# ---------------------------------------------------------------------------
def _base_spark_builder(app_name: str):
    """Common Spark settings shared by both phases."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer",             "org.apache.spark.serializer.KryoSerializer")
        .config("spark.default.parallelism",    "4")
        .config("spark.memory.offHeap.enabled", "false")
        .config("spark.shuffle.spill",          "true")
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.shuffle.compress",       "true")
        .config("spark.eventLog.enabled",       "false")
    )


def build_spark_preprocessing() -> SparkSession:
    return (
        _base_spark_builder("AIS-Preprocessing")
        .config("spark.sql.adaptive.enabled",                   "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled","true")
        .config("spark.sql.autoBroadcastJoinThreshold",         "50mb")
        .config("spark.sql.shuffle.partitions",                 "500")
        .getOrCreate()
    )


def build_spark_collision() -> SparkSession:
    """
    Collision-detection session.

    Per-day joins are small (~587 K records each) so 100 shuffle partitions
    is sufficient and avoids the overhead of 500-partition coordination.
    AQE coalescing is disabled so our partition counts are respected.
    Broadcast is disabled (both sides of the self-join are non-trivial).
    """
    return (
        _base_spark_builder("AIS-CollisionDetection")
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold",          "-1")
        .config("spark.sql.shuffle.partitions",                  "100")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AIS Collision Detection")
    parser.add_argument("--data",   default=os.environ.get("DATA_PATH",   "/data"))
    parser.add_argument("--output", default=os.environ.get("OUTPUT_PATH", "/output"))
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print("=" * 60)
    print("  AIS Collision Detection Pipeline")
    print("=" * 60)
    print(f"  Data path   : {args.data}")
    print(f"  Output path : {args.output}")
    print("=" * 60)

    import glob as _glob

    # ------------------------------------------------------------------
    # PHASE 1 — Preprocessing (one Spark session, one file at a time)
    #
    # Each daily CSV is filtered to the 50 nm study area and written to
    # its own Parquet subdirectory.  Keeping files separate allows Phase 2
    # to join each day's ~587 K records independently — 31× smaller than
    # a full-month join.
    # ------------------------------------------------------------------
    spark_pre = build_spark_preprocessing()
    spark_pre.sparkContext.setLogLevel("WARN")

    t0 = time.time()

    print("\n[1/3] Loading and preprocessing AIS data ...")
    days_root = "/tmp/ais_days"
    cand_root = "/tmp/ais_candidates"

    csv_files = sorted(_glob.glob(args.data.rstrip("/") + "/*.csv"))
    if not csv_files:
        print(f"ERROR: No CSV files found in {args.data}")
        sys.exit(1)

    print(f"      Found {len(csv_files)} CSV file(s). Processing one at a time ...")
    day_paths = []
    total_records = 0
    for i, csv_file in enumerate(csv_files):
        fname = os.path.basename(csv_file)
        print(f"      [{i+1}/{len(csv_files)}] {fname}", flush=True)
        day_df = load_and_preprocess(spark_pre, csv_file)
        day_path = f"{days_root}/day_{i:02d}"
        day_df.write.mode("overwrite").parquet(day_path)
        day_paths.append(day_path)
        spark_pre.catalog.clearCache()

    if not day_paths:
        print("ERROR: No records survived filtering.")
        sys.exit(1)

    # Quick record count for logging
    total_records = spark_pre.read.parquet(days_root + "/*").count()
    print(f"      {total_records:,} study-area records materialised to Parquet.")
    print("      Stopping preprocessing session to reclaim heap ...")
    spark_pre.stop()

    # ------------------------------------------------------------------
    # PHASE 2 — Per-day candidate finding (fresh Spark session)
    #
    # For each day we run the spatial–temporal self-join on only that
    # day's records (~587 K → ~5.3 M after 9× cell explosion).  This is
    # ~31× smaller than a full-month join and completes in seconds per day.
    # Raw candidate pairs (pairs within 300 m with dt ≤ 30 s) are written
    # to a combined Parquet store for the finalization step.
    # ------------------------------------------------------------------
    spark = build_spark_collision()
    spark.sparkContext.setLogLevel("WARN")

    print("\n[2/3] Running collision detection ...")
    print("      Phase 2a: per-day candidate join ...")

    written_cands = 0
    for i, day_path in enumerate(day_paths):
        fname = os.path.basename(_glob.glob(
            args.data.rstrip("/") + "/*.csv")[i])
        day_df = spark.read.parquet(day_path)
        n = day_df.count()
        if n == 0:
            continue
        print(f"      Day {i+1:02d}/31 ({fname}): {n:,} records", flush=True)
        cands = find_raw_candidates(spark, day_df)
        write_mode = "overwrite" if written_cands == 0 else "append"
        cands.write.mode(write_mode).parquet(cand_root)
        written_cands += 1
        spark.catalog.clearCache()

    if written_cands == 0:
        print("ERROR: No candidate pairs found across all days.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # PHASE 2b — Global finalization
    #
    # All daily candidates are combined (~100 K rows) and the transience
    # filter + CPA Window are applied.  Working on this tiny dataset is
    # fast regardless of partition counts.
    # ------------------------------------------------------------------
    print("      Phase 2b: transience filter + global CPA ...")
    all_candidates = spark.read.parquet(cand_root)
    n_cands = all_candidates.count()
    print(f"      {n_cands:,} raw candidate close-proximity records across 31 days.")

    # Load the full cleaned dataset for name resolution and visualization
    df = spark.read.parquet(days_root + "/*")

    result = finalize_collision(spark, all_candidates, df)

    print("\n" + "=" * 60)
    print("  COLLISION DETECTED")
    print("=" * 60)
    print(f"  Vessel 1 : {result['name1']}  (MMSI {result['mmsi1']})")
    print(f"  Vessel 2 : {result['name2']}  (MMSI {result['mmsi2']})")
    print(f"  Timestamp: {result['collision_ts'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Latitude : {result['collision_lat']:.6f}°N")
    print(f"  Longitude: {result['collision_lon']:.6f}°E")
    print(f"  Distance : {result['min_distance_m']:.1f} m")
    print("=" * 60)

    # Save JSON result summary
    summary = {
        "mmsi1":          result["mmsi1"],
        "mmsi2":          result["mmsi2"],
        "name1":          result["name1"],
        "name2":          result["name2"],
        "collision_ts":   result["collision_ts"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "collision_lat":  result["collision_lat"],
        "collision_lon":  result["collision_lon"],
        "min_distance_m": result["min_distance_m"],
    }
    summary_path = os.path.join(args.output, "collision_result.json")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=2)
    print(f"\n  Result JSON → {summary_path}")

    # 3. Visualise
    print("\n[3/3] Generating visualisations ...")
    plot_trajectories(df, result, args.output)

    elapsed = time.time() - t0
    print(f"\nPipeline complete in {elapsed:.1f} s.")
    print(f"Outputs written to: {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
