#!/usr/bin/env bash
set -euo pipefail

VMID=${VMID:-360}
BASE_ISO=${BASE_ISO:-/var/lib/vz/template/iso/Win11_24H2.iso}
TARGET_ISO=${TARGET_ISO:-/var/lib/vz/template/iso/Win11_24H2_MMA_FOG.iso}
OVERLAY=${OVERLAY:-/tmp/fog-win11-reference-overlay.tgz}
WORKDIR=$(mktemp -d /tmp/fog-win11-build.XXXXXX)
SRC_MNT="$WORKDIR/src"
BUILD_DIR="$WORKDIR/build"
OUT_TMP="$WORKDIR/Win11_24H2_MMA_FOG.iso"

cleanup() {
  set +e
  if mountpoint -q "$SRC_MNT"; then
    sudo umount "$SRC_MNT"
  fi
  rm -rf "$WORKDIR"
}

trap cleanup EXIT

mkdir -p "$SRC_MNT" "$BUILD_DIR"

if sudo qm status "$VMID" | grep -q 'status: running'; then
  sudo qm stop "$VMID" --skiplock 1
  for _ in $(seq 1 60); do
    if sudo qm status "$VMID" | grep -q 'status: stopped'; then
      break
    fi
    sleep 1
  done
fi

sudo mount -o loop,ro "$BASE_ISO" "$SRC_MNT"
rsync -a "$SRC_MNT"/ "$BUILD_DIR"/
sudo umount "$SRC_MNT"
find "$BUILD_DIR" -type d -exec chmod u+rwx {} +
find "$BUILD_DIR" -type f -exec chmod u+rw {} +

tar -C "$BUILD_DIR" -xzf "$OVERLAY"

test -f "$BUILD_DIR/Autounattend.xml"
test -f "$BUILD_DIR/sources/unattend.xml"
test -f "$BUILD_DIR/sources/pid.txt"
test -f "$BUILD_DIR/sources/ei.cfg"

EFI_BOOT_IMAGE=efi/microsoft/boot/efisys.bin
if [ -f "$BUILD_DIR/efi/microsoft/boot/efisys_noprompt.bin" ]; then
  EFI_BOOT_IMAGE=efi/microsoft/boot/efisys_noprompt.bin
fi

mkisofs \
  -iso-level 4 \
  -udf \
  -allow-limited-size \
  -J \
  -joliet-long \
  -D \
  -N \
  -relaxed-filenames \
  -V 'CCCOMA_X64FRE_FR-FR_DV9' \
  -b boot/etfsboot.com \
  -c boot/boot.cat \
  -no-emul-boot \
  -boot-load-size 8 \
  -boot-info-table \
  -eltorito-alt-boot \
  -e "$EFI_BOOT_IMAGE" \
  -no-emul-boot \
  -o "$OUT_TMP" \
  "$BUILD_DIR" >/tmp/fog-win11-mkisofs.log 2>&1

sudo mv "$OUT_TMP" "$TARGET_ISO"
ls -lh "$TARGET_ISO"
sha256sum "$TARGET_ISO"
