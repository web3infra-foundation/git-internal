#!/usr/bin/env bash
set -euo pipefail

# Compare git pack-objects vs our Rust impl at identical thread counts.
# Usage:
#   examples/bench_threads_compare.sh <path-to-.git> [thread_list] [window_size]
#
# Example:
#   examples/bench_threads_compare.sh /Users/bhz/Code/rk8s/.git "1,8" 10
#
# Default thread_list: "1,8"
# Default window_size: 10

usage() {
  cat >&2 <<'USAGE'
usage: examples/bench_threads_compare.sh <path-to-.git> [thread_list] [window_size]

Benchmark git pack-objects AND the Rust git-internal pack encoder at the
same thread counts for apples-to-apples comparison.

thread_list is a comma-separated list of thread counts (default: "1,8").
window_size is the delta window (default: 10, 0 disables delta).
USAGE
}

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage
  exit 2
fi

git_dir=$1
thread_list=${2:-"1,8"}
window_size=${3:-10}

if [[ ! -d "$git_dir/objects" ]]; then
  echo "$git_dir does not look like a .git directory (no objects/ subdir)" >&2
  exit 1
fi

if ! [[ "$window_size" =~ ^[0-9]+$ ]]; then
  echo "invalid window_size: $window_size" >&2
  exit 2
fi

# Split thread_list
IFS=',' read -ra THREADS <<< "$thread_list"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/bench-threads.XXXXXX")
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

oids_file="$tmp_dir/oids"
stats_file="$tmp_dir/stats"

format_bytes() {
  awk -v b="$1" '
    BEGIN {
      if (b >= 1024 * 1024 * 1024) {
        printf "%.2f GiB", b / (1024 * 1024 * 1024)
      } else if (b >= 1024 * 1024) {
        printf "%.2f MiB", b / (1024 * 1024)
      } else if (b >= 1024) {
        printf "%.2f KiB", b / 1024
      } else {
        printf "%d B", b
      }
    }
  '
}

echo "Repository : $git_dir"
echo "Window     : $window_size"
echo "Thread(s)  : ${thread_list}"
echo

# ── Extract object list once ────────────────────────────────────────────────
git --git-dir="$git_dir" cat-file \
  --batch-all-objects \
  --batch-check='%(objectname) %(objectsize)' \
  --unordered |
  awk '
    {
      print $1 > oids
      count += 1
      raw += $2
    }
    END {
      printf "objects %d\nraw_bytes %.0f\n", count, raw > stats
    }
  ' oids="$oids_file" stats="$stats_file"

objects=$(awk '$1 == "objects" { print $2 }' "$stats_file")
raw_bytes=$(awk '$1 == "raw_bytes" { print $2 }' "$stats_file")

if [[ "$objects" == "0" ]]; then
  echo "no objects found in repository" >&2
  exit 1
fi

echo "Unique objects (deduped) : $objects"
echo "Raw object bytes         : $(format_bytes "$raw_bytes")"
echo

# ── Git pack-objects for each thread count ──────────────────────────────────
echo "============================================================"
echo "  GIT pack-objects (--no-reuse-delta --no-reuse-object)"
echo "============================================================"
for n in "${THREADS[@]}"; do
  pack_path="$tmp_dir/git-pack-t${n}.pack"
  time_file="$tmp_dir/git-time-t${n}"

  echo
  echo "--- git pack-objects --threads=$n (window=$window_size) ---"
  if ! /usr/bin/time -p git --git-dir="$git_dir" pack-objects \
    --stdout \
    --window="$window_size" \
    --depth=10 \
    --threads="$n" \
    --no-reuse-delta \
    --no-reuse-object \
    <"$oids_file" >"$pack_path" 2>"$time_file"; then
    cat "$time_file" >&2
    echo "git FAILED" >&2
    continue
  fi

  pack_bytes=$(stat -f '%z' "$pack_path" 2>/dev/null || stat -c '%s' "$pack_path")
  wall=$(awk '$1 == "real" { print $2 }' "$time_file")
  ratio_pct=$(awk -v pack="$pack_bytes" -v raw="$raw_bytes" 'BEGIN {
    if (raw > 0) printf "%.2f", pack / raw * 100
    else printf "0.00"
  }')

  printf '  wall: %8.3f s | pack: %s (%s) | compression: %s%%\n' \
    "$wall" "$(format_bytes "$pack_bytes")" "$pack_bytes" "$ratio_pct"
done

# ── Rust impl for each thread count ──────────────────────────────────────────
echo
echo "============================================================"
echo "  RUST git-internal (rabin, no-prefilter)"
echo "============================================================"

# Ensure binary is up to date
echo
echo "Building release binary ..."
cargo build --release --features diff_rabin --example grading_bot_encode_pack_bench 2>&1 | tail -1

for n in "${THREADS[@]}"; do
  echo
  echo "--- rust rabin no-prefilter PACK_THREADS=$n (window=$window_size) ---"

  rust_start=$(date +%s.%N 2>/dev/null || echo 0)
  PACK_THREADS="$n" cargo run --release --features diff_rabin --quiet \
    --example grading_bot_encode_pack_bench -- \
    --rabin --no-prefilter "$git_dir" "$window_size" 2>&1
  rust_end=$(date +%s.%N 2>/dev/null || echo 0)
done

echo
echo "Done. tmp_dir = $tmp_dir"
