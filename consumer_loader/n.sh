#!/usr/bin/env bash
set -euo pipefail

### ----- Config (edit if needed) -----
CSV_FILE="ConsumerData.csv"   # set with env var or edit here
CH_HOST="localhost"
CH_PORT="9000"
CH_USER="default"
CH_PASS="nyros"            # empty default
CH_DB="device_tracking"
CH_TABLE="consumers"

# Choose mode: "stream" (recommended for ClickHouse) or "chunks"
LOAD_MODE="${LOAD_MODE:-stream}"

# Chunk settings (only used when LOAD_MODE=chunks)
LINES_PER_CHUNK="${LINES_PER_CHUNK:-2000000}"  # ~2M rows per chunk; tune as needed
WORKDIR="${WORKDIR:-/tmp/consumer_chunks}"

# Common ClickHouse input settings for CSV
CH_SETTINGS=(
  "--input_format_with_names_use_header=1"
  "--input_format_parallel_parsing=1"
  "--max_insert_block_size=1000000"
  "--max_threads=$(nproc)"
  "--input_format_csv_use_best_effort_in_schema_inference=1"
  "--input_format_defaults_for_omitted_fields=1"
  "--input_format_allow_errors_ratio=0.01"
)
### -----------------------------------

if [[ ! -f "$CSV_FILE" ]]; then
  echo "[ERROR] CSV file not found: $CSV_FILE" >&2
  exit 1
fi

echo "[INFO] Host=$CH_HOST DB=$CH_DB Table=$CH_TABLE File=$CSV_FILE Mode=$LOAD_MODE"

# Quick header sanity check (first 1 line)
echo "[INFO] CSV header preview:"
head -n 1 "$CSV_FILE"

if [[ "$LOAD_MODE" == "stream" ]]; then
  echo "[INFO] Streaming entire file into ClickHouse..."
  # Stream insert; ClickHouse will read from stdin in a single pass
  clickhouse-client \
    -h "$CH_HOST" --port "$CH_PORT" -u "$CH_USER" --password "$CH_PASS" \
    --database "$CH_DB" \
    --query "INSERT INTO $CH_TABLE FORMAT CSVWithNames" \
    "${CH_SETTINGS[@]}" \
    < "$CSV_FILE"

  echo "[SUCCESS] Streaming load completed."

elif [[ "$LOAD_MODE" == "chunks" ]]; then
  echo "[INFO] Loading in chunks of ~$LINES_PER_CHUNK lines per file..."
  mkdir -p "$WORKDIR"
  rm -f "$WORKDIR"/chunk_* || true

  # Extract header
  HEADER_FILE="$WORKDIR/header.csv"
  head -n 1 "$CSV_FILE" > "$HEADER_FILE"

  # Split without header
  TAIL_FILE="$WORKDIR/body.csv"
  tail -n +2 "$CSV_FILE" > "$TAIL_FILE"

  # Make chunks (no header yet)
  split -l "$LINES_PER_CHUNK" -d -a 5 "$TAIL_FILE" "$WORKDIR/chunk_"

  # Prepend header to each chunk
  for c in "$WORKDIR"/chunk_*; do
    mv "$c" "$c.body"
    cat "$HEADER_FILE" "$c.body" > "$c"
    rm "$c.body"
  done

  # Load each chunk sequentially (safe) — you can background & parallelize if desired
  total=0
  for c in "$WORKDIR"/chunk_*; do
    rows=$(($(wc -l < "$c") - 1)) # minus header
    echo "[INFO] Loading $(printf '%'"'"'d' "$rows") rows from $(basename "$c") ..."
    clickhouse-client \
      -h "$CH_HOST" --port "$CH_PORT" -u "$CH_USER" --password "$CH_PASS" \
      --database "$CH_DB" \
      --query "INSERT INTO $CH_TABLE FORMAT CSVWithNames" \
      "${CH_SETTINGS[@]}" \
      < "$c"
    total=$((total + rows))
  done

  echo "[SUCCESS] Chunked load completed. Total rows inserted: $total"
  # Cleanup (optional)
  rm -rf "$WORKDIR"
else
  echo "[ERROR] Unknown LOAD_MODE: $LOAD_MODE (use 'stream' or 'chunks')" >&2
  exit 1
fi