#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: examples/git_pack_objects_bench.sh <path-to-.git> [window_size]

Bench Git's own pack encoder against all objects in a repository object
database. The ratio uses the same raw-object payload-size denominator as
examples/grading_bot_encode_pack_bench.rs.

Git depth is fixed at 50 to match the current Rust encoder chain limit.
USAGE
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 2
fi

git_dir=$1
window_size=${2:-10}
git_depth=50

if [[ ! -d "$git_dir/objects" ]]; then
  echo "$git_dir does not look like a .git directory (no objects/ subdir)" >&2
  exit 1
fi

if ! [[ "$window_size" =~ ^[0-9]+$ ]]; then
  echo "invalid window_size: $window_size" >&2
  exit 2
fi

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/git-pack-objects-bench.XXXXXX")
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

oids_file="$tmp_dir/oids"
stats_file="$tmp_dir/stats"
time_file="$tmp_dir/time"
pack_path="$tmp_dir/git-pack.pack"

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

file_size_bytes() {
  if stat -c '%s' "$1" >/dev/null 2>&1; then
    stat -c '%s' "$1"
  else
    stat -f '%z' "$1"
  fi
}

echo "Repository : $git_dir"
echo "Window     : $window_size"
echo "Depth      : $git_depth"
echo

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
echo "Encoding pack with git pack-objects ($objects objects, window=$window_size) ..."

if ! /usr/bin/time -p git --git-dir="$git_dir" pack-objects \
  --stdout \
  --window="$window_size" \
  --depth="$git_depth" \
  --no-reuse-delta \
  --no-reuse-object \
  <"$oids_file" >"$pack_path" 2>"$time_file"; then
  cat "$time_file" >&2
  exit 1
fi

pack_bytes=$(file_size_bytes "$pack_path")
wall=$(awk '$1 == "real" { print $2 }' "$time_file")

throughput=$(awk -v raw="$raw_bytes" -v sec="$wall" 'BEGIN {
  if (sec > 0) {
    printf "%.2f", raw / sec / (1024 * 1024)
  } else {
    printf "0.00"
  }
}')
ratio_pct=$(awk -v pack="$pack_bytes" -v raw="$raw_bytes" 'BEGIN {
  if (raw > 0) {
    printf "%.2f", pack / raw * 100
  } else {
    printf "0.00"
  }
}')
inverse=$(awk -v pack="$pack_bytes" -v raw="$raw_bytes" 'BEGIN {
  if (pack > 0) {
    printf "%.2f", raw / pack
  } else {
    printf "0.00"
  }
}')

echo "------------------------------------------------------------"
echo "mode:          git-pack-objects"
echo "input:         $git_dir"
echo "objects:       $objects"
echo "window:        $window_size"
echo "depth:         $git_depth"
echo "raw bytes:     $(format_bytes "$raw_bytes") ($raw_bytes bytes)"
printf 'wall:          %.3f s\n' "$wall"
echo "throughput:    $throughput MiB/s (raw input)"
echo "pack written:  $pack_path"
echo "pack size:     $(format_bytes "$pack_bytes") ($pack_bytes bytes)"
echo "compression:   $ratio_pct% (pack/raw), ${inverse}x (raw/pack)"
