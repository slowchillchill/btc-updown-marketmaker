#!/usr/bin/env bash

set -euo pipefail

readonly ROOT_FILESYSTEM="/"
readonly USED_BYTES_THRESHOLD=30000000000
readonly TYPED_RAW_ROOT="/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw"

prune_oldest_utc_day() {
    local typed_raw_root=$1
    local current_utc_day=$2
    local used_bytes=$3
    local observed_at
    observed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    if ((used_bytes < USED_BYTES_THRESHOLD)); then
        printf 'prune_oldest_utc_day observed_at=%s used_bytes=%s result=below_threshold\n' \
            "$observed_at" "$used_bytes"
        return 0
    fi

    local yesterday
    yesterday=$(date -u -d "$current_utc_day - 1 day" +%F)
    local oldest_day=""
    local candidate=""

    if [[ -d "$typed_raw_root" ]]; then
        local path
        while IFS= read -r -d '' path; do
            local day=${path##*/utc_day=}
            local normalized_day
            if ! normalized_day=$(date -u -d "$day" +%F 2>/dev/null); then
                continue
            fi
            if [[ "$normalized_day" != "$day" || ! "$day" < "$yesterday" ]]; then
                continue
            fi
            if [[ -z "$oldest_day" || "$day" < "$oldest_day" ]]; then
                oldest_day=$day
                candidate=$path
            fi
        done < <(
            find "$typed_raw_root" -mindepth 1 -maxdepth 1 -type d \
                -name 'utc_day=????-??-??' -print0
        )
    fi

    if [[ -z "$candidate" ]]; then
        printf 'prune_oldest_utc_day observed_at=%s used_bytes=%s result=no_candidate\n' \
            "$observed_at" "$used_bytes"
        return 0
    fi

    if rm -rf --one-file-system --preserve-root=all -- "$candidate"; then
        printf 'prune_oldest_utc_day observed_at=%s used_bytes=%s selected_day=%s path=%s result=deleted\n' \
            "$observed_at" "$used_bytes" "$oldest_day" "$candidate"
    else
        local status=$?
        printf 'prune_oldest_utc_day observed_at=%s used_bytes=%s selected_day=%s path=%s result=delete_failed\n' \
            "$observed_at" "$used_bytes" "$oldest_day" "$candidate" >&2
        return "$status"
    fi
}

main() {
    if (($# != 0)); then
        printf 'prune_oldest_utc_day does not accept arguments\n' >&2
        return 2
    fi

    local used_bytes
    used_bytes=$(df -B1 --output=used "$ROOT_FILESYSTEM" | tail -n 1)
    local current_utc_day
    current_utc_day=$(date -u +%F)
    prune_oldest_utc_day \
        "$TYPED_RAW_ROOT" \
        "$current_utc_day" \
        "$used_bytes"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
