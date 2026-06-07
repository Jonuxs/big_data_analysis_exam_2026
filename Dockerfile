# ─────────────────────────────────────────────────────────────────────────────
# AIS Collision Detection — Dockerfile
# Base: official Python 3.11 slim + OpenJDK 21 (Trixie ships 21, not 17;
#       PySpark 3.5 is fully compatible with JDK 21)
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# --- System dependencies -----------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    # Create an arch-agnostic symlink so JAVA_HOME works on amd64 and arm64
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

# Fixed path via symlink — works on both amd64 and arm64
ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Python dependencies ------------------------------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Application source -------------------------------------------------------
COPY src/ /app/src/
WORKDIR /app/src

# --- Runtime environment variables -------------------------------------------
# These can be overridden via  docker run -e DATA_PATH=...
ENV DATA_PATH=/data
ENV OUTPUT_PATH=/output
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Spark local mode — uses all available cores
ENV SPARK_MASTER=local[*]

# JVM heap is set via JAVA_TOOL_OPTIONS in docker-compose.yml (-Xmx6g).
# JAVA_TOOL_OPTIONS is processed by the JVM itself at startup, so it works
# regardless of how the process is launched.

# --- Volumes (mount at runtime) -----------------------------------------------
# /data   — place your AIS CSV files here (read-only)
# /output — trajectory_plot.png, trajectory_map.html, collision_result.json
VOLUME ["/data", "/output"]

# --- Entry point --------------------------------------------------------------
CMD ["python", "main.py"]
