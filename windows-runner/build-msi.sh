#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-1.0.0}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$ROOT/dist"
PAYLOAD_ROOT="$OUTPUT_DIR/payload"
SCRIPTS_ROOT="$PAYLOAD_ROOT/scripts"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"

rm -rf "$OUTPUT_DIR"
mkdir -p "$SCRIPTS_ROOT"

cp "$ROOT/MmaMdtRunner.ps1" "$PAYLOAD_ROOT/"
cp "$ROOT/MmaMdtRunner.Common.ps1" "$PAYLOAD_ROOT/"
cp "$ROOT/config.sample.json" "$PAYLOAD_ROOT/"
cp "$ROOT/config.sample.json" "$PAYLOAD_ROOT/config.json"
cp "$ROOT/technicians.json" "$PAYLOAD_ROOT/"
cp "$ROOT/Register-MmaMdtRunnerSyncTask.ps1" "$PAYLOAD_ROOT/"

cp "$REPO_ROOT/scripts/mdt-report.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/mdt-laptop.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/mdt-desktop.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/mdt-stress.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/mdt-outbox-sync.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/keyboard_capture.ps1" "$SCRIPTS_ROOT/"
cp "$REPO_ROOT/scripts/camera.exe" "$SCRIPTS_ROOT/"

(
  cd "$OUTPUT_DIR"
  wixl \
    -D Version="$VERSION" \
    -D PayloadRoot="payload" \
    -a x64 \
    -o "MmaMdtRunner-$VERSION.msi" \
    "$ROOT/installer/MmaMdtRunner.wixl.wxs"
)

echo "$OUTPUT_DIR/MmaMdtRunner-$VERSION.msi"
