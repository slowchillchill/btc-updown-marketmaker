#!/usr/bin/env bash

set -euo pipefail

TEST_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly TEST_DIR
source "$TEST_DIR/../scripts/prune-oldest-utc-day.sh"

FIXTURE_ROOT=$(mktemp -d)
readonly FIXTURE_ROOT
trap 'rm -rf -- "$FIXTURE_ROOT"' EXIT

low_water_root="$FIXTURE_ROOT/low-water"
mkdir -p \
    "$low_water_root/utc_day=2026-08-04" \
    "$low_water_root/utc_day=2026-08-05"
prune_oldest_utc_day "$low_water_root" 2026-08-08 29999999999
test -d "$low_water_root/utc_day=2026-08-04"
test -d "$low_water_root/utc_day=2026-08-05"

high_water_root="$FIXTURE_ROOT/high-water"
mkdir -p \
    "$high_water_root/utc_day=2026-08-04" \
    "$high_water_root/utc_day=2026-08-05" \
    "$high_water_root/utc_day=2026-08-07" \
    "$high_water_root/utc_day=2026-08-08" \
    "$high_water_root/not-a-utc-day"
prune_oldest_utc_day "$high_water_root" 2026-08-08 30000000000
test ! -e "$high_water_root/utc_day=2026-08-04"
test -d "$high_water_root/utc_day=2026-08-05"
test -d "$high_water_root/utc_day=2026-08-07"
test -d "$high_water_root/utc_day=2026-08-08"
test -d "$high_water_root/not-a-utc-day"

printf 'prune-oldest-utc-day fixtures passed\n'
