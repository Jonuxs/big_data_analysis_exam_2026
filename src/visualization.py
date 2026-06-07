"""
visualization.py
================
Generates two complementary trajectory visualisations for the collision event:

  1. trajectory_map.html  — interactive Folium map (opens in browser)
  2. trajectory_plot.png  — static Matplotlib figure (saved to /output)

Both show each vessel's AIS positions in a ±10-minute window around the
detected collision timestamp, with the collision point highlighted.
"""

import os
from datetime import datetime, timedelta

import folium
import matplotlib
matplotlib.use("Agg")          # non-interactive backend for Docker
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
import pandas as pd
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WINDOW_MINUTES = 10            # ±10 min around collision
COLOURS = {
    "vessel1": "#E63946",      # red
    "vessel2": "#457B9D",      # blue
    "collision": "#F4A261",    # amber
}


# ---------------------------------------------------------------------------
# Helper — extract trajectory data as Pandas
# ---------------------------------------------------------------------------
def _get_trajectory(df, mmsi: int, ts_center, window_min: int) -> pd.DataFrame:
    """
    Return a Pandas DataFrame of (ts, lat, lon, sog) for *mmsi* in the
    window [ts_center - window_min, ts_center + window_min].
    """
    ts_lo = ts_center - timedelta(minutes=window_min)
    ts_hi = ts_center + timedelta(minutes=window_min)

    rows = (
        df.filter(
            (F.col("mmsi") == mmsi) &
            (F.col("ts") >= ts_lo) &
            (F.col("ts") <= ts_hi)
        )
        .select("ts", "lat", "lon", "sog", "cog")
        .orderBy("ts")
        .toPandas()
    )
    return rows


# ---------------------------------------------------------------------------
# 1 — Static Matplotlib figure
# ---------------------------------------------------------------------------
def _plot_static(traj1: pd.DataFrame, traj2: pd.DataFrame,
                 result: dict, output_path: str):
    """Save a two-panel Matplotlib figure."""

    fig = plt.figure(figsize=(14, 7), dpi=150)
    fig.suptitle(
        f"Vessel Collision Analysis\n"
        f"{result['name1']} (MMSI {result['mmsi1']})  ×  "
        f"{result['name2']} (MMSI {result['mmsi2']})\n"
        f"Collision: {result['collision_ts'].strftime('%Y-%m-%d %H:%M:%S UTC')}  |  "
        f"Distance: {result['min_distance_m']:.1f} m",
        fontsize=11, fontweight="bold", y=0.98
    )

    # ── Panel 1: geographic track ──────────────────────────────────────
    ax_map = fig.add_subplot(1, 2, 1)
    ax_map.set_facecolor("#d0e8f5")

    # Grid lines
    ax_map.grid(True, linestyle="--", linewidth=0.4, alpha=0.6)

    # Time colour gradient: older = lighter, newer = darker
    def _scatter_coloured(ax, traj, colour, label):
        if traj.empty:
            return
        n = len(traj)
        alphas = [0.3 + 0.7 * i / max(n - 1, 1) for i in range(n)]
        for i in range(n - 1):
            ax.plot(
                traj["lon"].iloc[i:i+2], traj["lat"].iloc[i:i+2],
                color=colour, linewidth=1.5, alpha=alphas[i]
            )
        # Scatter dots
        ax.scatter(traj["lon"], traj["lat"],
                   c=[colour] * n, s=20, zorder=4, alpha=alphas, label=label)
        # Start / end markers
        ax.scatter(traj["lon"].iloc[0],  traj["lat"].iloc[0],
                   marker="o", s=80, color=colour, edgecolors="white",
                   linewidths=1.5, zorder=5)
        ax.scatter(traj["lon"].iloc[-1], traj["lat"].iloc[-1],
                   marker="^", s=100, color=colour, edgecolors="white",
                   linewidths=1.5, zorder=5)

    _scatter_coloured(ax_map, traj1, COLOURS["vessel1"], result["name1"])
    _scatter_coloured(ax_map, traj2, COLOURS["vessel2"], result["name2"])

    # Collision point
    ax_map.scatter(
        result["collision_lon"], result["collision_lat"],
        marker="*", s=350, color=COLOURS["collision"],
        edgecolors="black", linewidths=0.8,
        zorder=6, label="Collision point"
    )

    ax_map.set_xlabel("Longitude (°E)", fontsize=9)
    ax_map.set_ylabel("Latitude (°N)", fontsize=9)
    ax_map.set_title("Geographic Trajectories (±10 min)", fontsize=10)
    ax_map.legend(fontsize=8, loc="best")
    ax_map.tick_params(labelsize=8)
    ax_map.set_aspect("equal", adjustable="datalim")

    # ── Panel 2: time-series — SOG ─────────────────────────────────────
    ax_sog = fig.add_subplot(1, 2, 2)

    if not traj1.empty:
        ax_sog.plot(traj1["ts"], traj1["sog"],
                    color=COLOURS["vessel1"], linewidth=1.8, marker=".",
                    markersize=4, label=result["name1"])
    if not traj2.empty:
        ax_sog.plot(traj2["ts"], traj2["sog"],
                    color=COLOURS["vessel2"], linewidth=1.8, marker=".",
                    markersize=4, label=result["name2"])

    ax_sog.axvline(result["collision_ts"], color=COLOURS["collision"],
                   linewidth=2, linestyle="--", label="Collision time")

    ax_sog.set_xlabel("Time (UTC)", fontsize=9)
    ax_sog.set_ylabel("Speed over Ground (knots)", fontsize=9)
    ax_sog.set_title("Speed over Ground vs. Time (±10 min)", fontsize=10)
    ax_sog.legend(fontsize=8)
    ax_sog.tick_params(labelsize=8)
    ax_sog.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax_sog.xaxis.set_major_locator(mdates.MinuteLocator(interval=2))
    plt.setp(ax_sog.xaxis.get_majorticklabels(), rotation=30, ha="right")
    ax_sog.grid(True, linestyle="--", linewidth=0.4, alpha=0.6)

    plt.tight_layout(rect=[0, 0, 1, 0.94])

    out_file = os.path.join(output_path, "trajectory_plot.png")
    fig.savefig(out_file, bbox_inches="tight")
    plt.close(fig)
    print(f"[visualization] Static plot saved → {out_file}")


# ---------------------------------------------------------------------------
# 2 — Interactive Folium map
# ---------------------------------------------------------------------------
def _plot_folium(traj1: pd.DataFrame, traj2: pd.DataFrame,
                 result: dict, output_path: str):
    """Save an interactive Folium HTML map."""

    fmap = folium.Map(
        location=[result["collision_lat"], result["collision_lon"]],
        zoom_start=13,
        tiles="CartoDB positron"
    )

    def _add_track(traj, colour, name, mmsi):
        if traj.empty:
            return
        coords = list(zip(traj["lat"].tolist(), traj["lon"].tolist()))
        folium.PolyLine(
            coords, color=colour, weight=3, opacity=0.8,
            tooltip=f"{name} (MMSI {mmsi})"
        ).add_to(fmap)

        # Individual position markers
        for _, row in traj.iterrows():
            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=4, color=colour, fill=True, fill_opacity=0.7,
                popup=folium.Popup(
                    f"<b>{name}</b><br>"
                    f"Time: {row['ts'].strftime('%H:%M:%S')}<br>"
                    f"SOG: {row['sog']:.1f} kn<br>"
                    f"COG: {row['cog']:.1f}°",
                    max_width=200
                )
            ).add_to(fmap)

        # Start / end
        folium.Marker(
            location=[coords[0][0],  coords[0][1]],
            icon=folium.Icon(color="green", icon="play"),
            tooltip=f"{name} — start"
        ).add_to(fmap)
        folium.Marker(
            location=[coords[-1][0], coords[-1][1]],
            icon=folium.Icon(color="gray", icon="stop"),
            tooltip=f"{name} — end"
        ).add_to(fmap)

    _add_track(traj1, COLOURS["vessel1"], result["name1"], result["mmsi1"])
    _add_track(traj2, COLOURS["vessel2"], result["name2"], result["mmsi2"])

    # Collision marker
    folium.Marker(
        location=[result["collision_lat"], result["collision_lon"]],
        icon=folium.Icon(color="orange", icon="warning-sign", prefix="glyphicon"),
        popup=folium.Popup(
            f"<b>⚠ Collision Event</b><br>"
            f"Time: {result['collision_ts'].strftime('%Y-%m-%d %H:%M:%S UTC')}<br>"
            f"Distance: {result['min_distance_m']:.1f} m<br>"
            f"Vessel 1: {result['name1']} ({result['mmsi1']})<br>"
            f"Vessel 2: {result['name2']} ({result['mmsi2']})",
            max_width=280
        ),
        tooltip="⚠ Collision point"
    ).add_to(fmap)

    # Legend
    legend_html = f"""
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 10px 16px; border-radius: 8px;
                border: 1px solid #ccc; font-size: 13px; font-family: sans-serif;">
        <b>Collision: {result['collision_ts'].strftime('%Y-%m-%d %H:%M UTC')}</b><br>
        Distance: {result['min_distance_m']:.1f} m<br><br>
        <span style="color:{COLOURS['vessel1']};">&#9632;</span>
        {result['name1']} ({result['mmsi1']})<br>
        <span style="color:{COLOURS['vessel2']};">&#9632;</span>
        {result['name2']} ({result['mmsi2']})<br>
        <span style="color:{COLOURS['collision']};">&#9733;</span> Collision point
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))

    out_file = os.path.join(output_path, "trajectory_map.html")
    fmap.save(out_file)
    print(f"[visualization] Interactive map saved → {out_file}")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def plot_trajectories(df, result: dict, output_path: str):
    """
    Generate both the static PNG and interactive HTML visualisations.

    Parameters
    ----------
    df          : cleaned Spark DataFrame from preprocessing
    result      : dict returned by collision_detection.detect_collision()
    output_path : directory where output files are written
    """
    os.makedirs(output_path, exist_ok=True)

    ts = result["collision_ts"]

    print(f"[visualization] Fetching trajectories for ±{WINDOW_MINUTES} min window …")
    traj1 = _get_trajectory(df, result["mmsi1"], ts, WINDOW_MINUTES)
    traj2 = _get_trajectory(df, result["mmsi2"], ts, WINDOW_MINUTES)

    print(f"[visualization]   {result['name1']}: {len(traj1)} AIS fixes")
    print(f"[visualization]   {result['name2']}: {len(traj2)} AIS fixes")

    _plot_static(traj1, traj2, result, output_path)
    _plot_folium(traj1, traj2, result, output_path)
