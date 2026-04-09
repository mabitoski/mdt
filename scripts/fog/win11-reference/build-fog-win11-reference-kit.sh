#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)
WORK_ROOT=${WORK_ROOT:-"$REPO_ROOT/.tmp/fog-win11-reference-kit"}
OUTPUT_ISO=${OUTPUT_ISO:-"$REPO_ROOT/dist/fog-win11-reference-kit.iso"}
API_URL=${API_URL:-"http://10.1.10.27:3000/api/ingest"}
TECHNICIAN=${TECHNICIAN:-""}
CATEGORY=${CATEGORY:-"auto"}
TEST_MODE=${TEST_MODE:-"quick"}

rm -rf "$WORK_ROOT"
mkdir -p "$WORK_ROOT/payload/scripts/fog" "$WORK_ROOT/payload/scripts"
mkdir -p "$WORK_ROOT/sources"
mkdir -p "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog"
mkdir -p "$WORK_ROOT/sources/\$OEM\$/\$\$/Temp"
mkdir -p "$(dirname "$OUTPUT_ISO")"

cp "$SCRIPT_DIR/Autounattend.xml" "$WORK_ROOT/Autounattend.xml"
cp "$SCRIPT_DIR/Autounattend.xml" "$WORK_ROOT/sources/unattend.xml"
cp "$SCRIPT_DIR/Install-MmaFogRef.ps1" "$WORK_ROOT/Install-MmaFogRef.ps1"
cp "$SCRIPT_DIR/Install-MmaFogRef.ps1" "$WORK_ROOT/sources/\$OEM\$/\$\$/Temp/Install-MmaFogRef.ps1"
printf 'MMA FOG reference kit\n' > "$WORK_ROOT/MMA_FOG_REF.TAG"

cat > "$WORK_ROOT/sources/ei.cfg" <<'EOF'
[EditionID]
Professional
[Channel]
Retail
[VL]
0
EOF

cat > "$WORK_ROOT/sources/pid.txt" <<'EOF'
[PID]
Value=W269N-WFGWX-YVC9B-4J6C9-T83GX
EOF

cp "$REPO_ROOT/scripts/mdt-report.ps1" "$WORK_ROOT/payload/scripts/mdt-report.ps1"
cp "$REPO_ROOT/scripts/mdt-outbox-sync.ps1" "$WORK_ROOT/payload/scripts/mdt-outbox-sync.ps1"
cp "$REPO_ROOT/scripts/keyboard_capture.ps1" "$WORK_ROOT/payload/scripts/keyboard_capture.ps1"
cp "$REPO_ROOT/scripts/camera.exe" "$WORK_ROOT/payload/scripts/camera.exe"
cp "$REPO_ROOT/scripts/fog/fog-common.ps1" "$WORK_ROOT/payload/scripts/fog/fog-common.ps1"
cp "$REPO_ROOT/scripts/fog/fog-report.ps1" "$WORK_ROOT/payload/scripts/fog/fog-report.ps1"
cp "$REPO_ROOT/scripts/fog/fog-desktop.ps1" "$WORK_ROOT/payload/scripts/fog/fog-desktop.ps1"
cp "$REPO_ROOT/scripts/fog/fog-laptop.ps1" "$WORK_ROOT/payload/scripts/fog/fog-laptop.ps1"
cp "$REPO_ROOT/scripts/fog/fog-stress.ps1" "$WORK_ROOT/payload/scripts/fog/fog-stress.ps1"
cp "$REPO_ROOT/scripts/fog/fog-firstboot.ps1" "$WORK_ROOT/payload/scripts/fog/fog-firstboot.ps1"
cp "$REPO_ROOT/scripts/fog/install-fog-bootstrap.ps1" "$WORK_ROOT/payload/scripts/fog/install-fog-bootstrap.ps1"

cp "$REPO_ROOT/scripts/mdt-report.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/mdt-report.ps1"
cp "$REPO_ROOT/scripts/mdt-outbox-sync.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/mdt-outbox-sync.ps1"
cp "$REPO_ROOT/scripts/keyboard_capture.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/keyboard_capture.ps1"
cp "$REPO_ROOT/scripts/camera.exe" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/camera.exe"
cp "$REPO_ROOT/scripts/fog/fog-common.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-common.ps1"
cp "$REPO_ROOT/scripts/fog/fog-report.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-report.ps1"
cp "$REPO_ROOT/scripts/fog/fog-desktop.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-desktop.ps1"
cp "$REPO_ROOT/scripts/fog/fog-laptop.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-laptop.ps1"
cp "$REPO_ROOT/scripts/fog/fog-stress.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-stress.ps1"
cp "$REPO_ROOT/scripts/fog/fog-firstboot.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/fog-firstboot.ps1"
cp "$REPO_ROOT/scripts/fog/install-fog-bootstrap.ps1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/scripts/fog/install-fog-bootstrap.ps1"

cat > "$WORK_ROOT/payload/fog-bootstrap.config.psd1" <<EOF
@{
  ApiUrl = '$API_URL'
  Category = '$CATEGORY'
  TestMode = '$TEST_MODE'
  Technician = '$TECHNICIAN'
  QueueOnUploadFailure = \$true
  SkipKeyboardCapture = \$true
  SkipDebugWinRM = \$false
  ArtifactRoot = 'C:\ProgramData\MMA\FogBootstrap\Artifacts'
  OutboxRoot = 'C:\ProgramData\MMA\FogBootstrap\Outbox'
  ReportTag = 'En cours'
}
EOF

cp "$WORK_ROOT/payload/fog-bootstrap.config.psd1" "$WORK_ROOT/sources/\$OEM\$/\$1/MMA_FOG_REF/payload/fog-bootstrap.config.psd1"

genisoimage \
  -quiet \
  -iso-level 3 \
  -D \
  -R \
  -full-iso9660-filenames \
  -volid MMA_FOG_REF \
  -output "$OUTPUT_ISO" \
  "$WORK_ROOT"

echo "FOG Windows 11 reference kit created: $OUTPUT_ISO"
