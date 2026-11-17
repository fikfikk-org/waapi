#!/usr/bin/env bash
# Memeriksa seluruh tautan markdown relatif di file *.md menunjuk berkas yang benar-benar ada.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

status=0
checked=0

while IFS= read -r md_file; do
  md_dir="$(dirname "$md_file")"

  # Ekstrak seluruh tautan ](target) di luar fenced code block dalam satu pass awk.
  while IFS=$'\t' read -r line_no target; do
    [ -z "$target" ] && continue

    case "$target" in
      "#"*|http://*|https://*|mailto:*|tel:*)
        continue
        ;;
    esac

    path_only="${target%%#*}"
    [ -z "$path_only" ] && continue

    checked=$((checked + 1))
    if [ "${path_only#/}" != "$path_only" ]; then
      resolved="${ROOT}${path_only}"
    else
      resolved="${md_dir}/${path_only}"
    fi

    if [ ! -e "$resolved" ]; then
      printf 'BROKEN LINK: %s:%s -> %s (dicari: %s)\n' "$md_file" "$line_no" "$target" "$resolved" >&2
      status=1
    fi
  done < <(awk '
    /^[[:space:]]*```/ || /^[[:space:]]*~~~/ { fence = !fence; next }
    fence { next }
    {
      s = $0
      while (match(s, /\]\([^)]+\)/)) {
        raw = substr(s, RSTART + 2, RLENGTH - 3)
        sub(/[[:space:]].*$/, "", raw)
        if (raw != "") {
          printf "%d\t%s\n", NR, raw
        }
        s = substr(s, RSTART + RLENGTH)
      }
    }
  ' "$md_file")
done < <(find . -type f -name '*.md' -not -path './.git/*' | sort)

if [ "$status" -eq 0 ]; then
  printf 'OK: %d tautan relatif di berkas markdown valid.\n' "$checked"
fi

exit "$status"
