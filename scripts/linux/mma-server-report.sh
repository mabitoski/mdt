#!/usr/bin/env bash
set -euo pipefail

API_URL="${MMA_SERVER_API_URL:-http://127.0.0.1:3000/api/ingest}"
TECHNICIAN="${MMA_SERVER_TECHNICIAN:-Infra}"
TAG_NAME="${MMA_SERVER_TAG:-}"
PING_TARGET="${MMA_SERVER_PING_TARGET:-1.1.1.1}"
PING_TIMEOUT_SECONDS="${MMA_SERVER_PING_TIMEOUT_SECONDS:-2}"
FS_USAGE_NOK_THRESHOLD="${MMA_SERVER_FS_USAGE_NOK_THRESHOLD:-95}"
CURL_TIMEOUT_SECONDS="${MMA_SERVER_CURL_TIMEOUT_SECONDS:-20}"
REQUEST_INSECURE="${MMA_SERVER_CURL_INSECURE:-0}"
OUTPUT_FILE="${MMA_SERVER_OUTPUT_FILE:-}"
DRY_RUN=0
EXTRA_SERVICE_NAMES=()

usage() {
  cat <<'EOF'
Usage: mma-server-report.sh [options]

Collecte un diagnostic Linux orienté serveur et l'envoie vers MMA Web via /api/ingest.

Options:
  --api-url URL          URL d'ingest MMA (defaut: MMA_SERVER_API_URL ou http://127.0.0.1:3000/api/ingest)
  --technician NAME      Nom operateur/technicien associe au rapport
  --tag NAME             Tag de production facultatif
  --ping-target HOST     Cible du ping de controle (defaut: 1.1.1.1)
  --output FILE          Sauvegarde aussi le payload JSON dans un fichier local
  --service NAME         Ajoute un service systemd a superviser, option repetable
  --insecure             Desactive la verification TLS pour curl
  --dry-run              Genere le payload sans l'envoyer
  --help                 Affiche cette aide

Variables d'environnement utiles:
  MMA_SERVER_API_URL
  MMA_SERVER_TECHNICIAN
  MMA_SERVER_TAG
  MMA_SERVER_PING_TARGET
  MMA_SERVER_PING_TIMEOUT_SECONDS
  MMA_SERVER_FS_USAGE_NOK_THRESHOLD
  MMA_SERVER_CURL_TIMEOUT_SECONDS
  MMA_SERVER_CURL_INSECURE
  MMA_SERVER_OUTPUT_FILE
  MMA_SERVER_SERVICE_NAMES   Liste separee par des virgules
EOF
}

trim() {
  local value="${1-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

json_escape() {
  local value="${1-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

json_quote() {
  printf '"%s"' "$(json_escape "$1")"
}

json_build_array() {
  local first=1
  printf '['
  local item
  for item in "$@"; do
    [[ -n "$item" ]] || continue
    if (( first )); then
      first=0
    else
      printf ','
    fi
    printf '%s' "$item"
  done
  printf ']'
}

json_build_object() {
  local first=1
  printf '{'
  local item
  for item in "$@"; do
    [[ -n "$item" ]] || continue
    if (( first )); then
      first=0
    else
      printf ','
    fi
    printf '%s' "$item"
  done
  printf '}'
}

append_json_string_field() {
  local -n fields_ref=$1
  local key=$2
  local value="${3-}"
  [[ -n "$value" ]] || return 0
  fields_ref+=("$(json_quote "$key"):$(json_quote "$value")")
}

append_json_number_field() {
  local -n fields_ref=$1
  local key=$2
  local value="${3-}"
  [[ -n "$value" ]] || return 0
  if [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    fields_ref+=("$(json_quote "$key"):$value")
  fi
}

append_json_boolean_field() {
  local -n fields_ref=$1
  local key=$2
  local value="${3-}"
  [[ -n "$value" ]] || return 0
  if [[ "$value" == "true" || "$value" == "false" ]]; then
    fields_ref+=("$(json_quote "$key"):$value")
  fi
}

append_json_raw_field() {
  local -n fields_ref=$1
  local key=$2
  local raw_value="${3-}"
  [[ -n "$raw_value" ]] || return 0
  fields_ref+=("$(json_quote "$key"):$raw_value")
}

have_command() {
  command -v "$1" >/dev/null 2>&1
}

read_first_file_value() {
  local path
  for path in "$@"; do
    if [[ -r "$path" ]]; then
      local value
      value="$(<"$path")"
      value="$(trim "$value")"
      if [[ -n "$value" ]]; then
        printf '%s' "$value"
        return 0
      fi
    fi
  done
  return 1
}

safe_number_from_string() {
  local raw="${1-}"
  raw="$(trim "$raw")"
  raw="${raw//,/.}"
  if [[ "$raw" =~ ^-?[0-9]+([.][0-9]+)?$ ]]; then
    printf '%s' "$raw"
    return 0
  fi
  return 1
}

bytes_to_gb() {
  local value="${1-}"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf ''
    return 0
  fi
  awk -v bytes="$value" 'BEGIN { printf "%.1f", bytes / 1073741824 }'
}

parse_kv_line() {
  local line="${1-}"
  local -n result_ref=$2
  result_ref=()
  while [[ $line =~ ^([A-Z0-9_]+)=\"(([^\"\\]|\\.)*)\"[[:space:]]*(.*)$ ]]; do
    local key="${BASH_REMATCH[1]}"
    local value="${BASH_REMATCH[2]}"
    line="${BASH_REMATCH[4]}"
    value="${value//\\\"/\"}"
    value="${value//\\\\/\\}"
    result_ref["$key"]="$value"
  done
}

collect_report_id() {
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    tr -d '\n' </proc/sys/kernel/random/uuid
    return 0
  fi
  if have_command uuidgen; then
    uuidgen | tr '[:upper:]' '[:lower:]'
    return 0
  fi
  date -u +%s | awk '{ printf "00000000-0000-4000-8000-%012d\n", $1 % 1000000000000 }'
}

collect_hostname() {
  local value=""
  if have_command hostname; then
    value="$(hostname -s 2>/dev/null || hostname 2>/dev/null || true)"
  fi
  value="$(trim "$value")"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
    return 0
  fi
  uname -n
}

collect_os_version() {
  if [[ -r /etc/os-release ]]; then
    local pretty=""
    pretty="$(awk -F= '$1 == "PRETTY_NAME" { gsub(/^"/, "", $2); gsub(/"$/, "", $2); print $2; exit }' /etc/os-release)"
    pretty="$(trim "$pretty")"
    if [[ -n "$pretty" ]]; then
      printf '%s' "$pretty"
      return 0
    fi
  fi
  uname -srmo
}

collect_primary_route_interface() {
  if have_command ip; then
    ip route show default 2>/dev/null | awk '/default/ { for (i = 1; i <= NF; i++) if ($i == "dev") { print $(i + 1); exit } }'
  fi
}

collect_mac_array_json() {
  local preferred_iface="${1-}"
  local macs=()
  local iface
  for iface_path in /sys/class/net/*; do
    [[ -e "$iface_path" ]] || continue
    iface="${iface_path##*/}"
    [[ "$iface" != "lo" ]] || continue
    local mac
    mac="$(tr '[:lower:]' '[:upper:]' <"$iface_path/address" 2>/dev/null || true)"
    mac="$(trim "$mac")"
    if [[ -z "$mac" || "$mac" == "00:00:00:00:00:00" ]]; then
      continue
    fi
    if [[ " ${macs[*]} " != *" $mac "* ]]; then
      if [[ -n "$preferred_iface" && "$iface" == "$preferred_iface" ]]; then
        macs=("$mac" "${macs[@]}")
      else
        macs+=("$mac")
      fi
    fi
  done

  local json_items=()
  local mac
  for mac in "${macs[@]}"; do
    json_items+=("$(json_quote "$mac")")
  done
  json_build_array "${json_items[@]}"
}

collect_primary_mac() {
  local preferred_iface="${1-}"
  if [[ -n "$preferred_iface" && -r "/sys/class/net/$preferred_iface/address" ]]; then
    local preferred_mac
    preferred_mac="$(tr '[:lower:]' '[:upper:]' <"/sys/class/net/$preferred_iface/address" 2>/dev/null || true)"
    preferred_mac="$(trim "$preferred_mac")"
    if [[ -n "$preferred_mac" && "$preferred_mac" != "00:00:00:00:00:00" ]]; then
      printf '%s' "$preferred_mac"
      return 0
    fi
  fi

  local iface
  for iface_path in /sys/class/net/*; do
    [[ -e "$iface_path" ]] || continue
    iface="${iface_path##*/}"
    [[ "$iface" != "lo" ]] || continue
    local mac
    mac="$(tr '[:lower:]' '[:upper:]' <"$iface_path/address" 2>/dev/null || true)"
    mac="$(trim "$mac")"
    if [[ -n "$mac" && "$mac" != "00:00:00:00:00:00" ]]; then
      printf '%s' "$mac"
      return 0
    fi
  done
}

collect_cpu_json() {
  local name vendor architecture sockets cores threads
  name="$(awk -F: '/model name/ {sub(/^[ \t]+/, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null || true)"
  vendor="$(awk -F: '/vendor_id/ {sub(/^[ \t]+/, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null || true)"
  architecture="$(uname -m 2>/dev/null || true)"
  threads="$(nproc --all 2>/dev/null || awk '/^processor/ { count += 1 } END { print count + 0 }' /proc/cpuinfo 2>/dev/null || true)"
  sockets=""
  cores=""
  if have_command lscpu; then
    sockets="$(LC_ALL=C lscpu 2>/dev/null | awk -F: '$1 ~ /^Socket\\(s\\)$/ { gsub(/^[ \t]+/, "", $2); print $2; exit }')"
    local cores_per_socket
    cores_per_socket="$(LC_ALL=C lscpu 2>/dev/null | awk -F: '$1 ~ /^Core\\(s\\) per socket$/ { gsub(/^[ \t]+/, "", $2); print $2; exit }')"
    if [[ "$sockets" =~ ^[0-9]+$ && "$cores_per_socket" =~ ^[0-9]+$ ]]; then
      cores=$(( sockets * cores_per_socket ))
    else
      cores="$(LC_ALL=C lscpu 2>/dev/null | awk -F: '$1 ~ /^CPU\\(s\\)$/ { gsub(/^[ \t]+/, "", $2); print $2; exit }')"
    fi
  fi

  local fields=()
  append_json_string_field fields "name" "$(trim "$name")"
  append_json_string_field fields "vendor" "$(trim "$vendor")"
  append_json_string_field fields "architecture" "$(trim "$architecture")"
  append_json_number_field fields "sockets" "$(trim "$sockets")"
  append_json_number_field fields "cores" "$(trim "$cores")"
  append_json_number_field fields "threads" "$(trim "$threads")"
  json_build_object "${fields[@]}"
}

collect_gpu_json() {
  [[ -e /sys/class/drm ]] || { printf ''; return 0; }
  local name=""
  if have_command lspci; then
    name="$(lspci 2>/dev/null | awk -F': ' '/VGA compatible controller|3D controller|Display controller/ { print $2; exit }')"
  fi
  name="$(trim "$name")"
  [[ -n "$name" ]] || { printf ''; return 0; }
  local fields=()
  append_json_string_field fields "name" "$name"
  json_build_object "${fields[@]}"
}

smart_health_for_disk() {
  local disk_name="${1-}"
  [[ -n "$disk_name" ]] || return 0
  if ! have_command smartctl; then
    return 0
  fi
  local output=""
  output="$(smartctl -H "/dev/$disk_name" 2>/dev/null || true)"
  output="$(printf '%s' "$output" | awk -F: '
    /SMART overall-health self-assessment test result/ { gsub(/^[ \t]+/, "", $2); print $2; exit }
    /SMART Health Status/ { gsub(/^[ \t]+/, "", $2); print $2; exit }
  ')"
  output="$(trim "$output")"
  if [[ -z "$output" ]]; then
    return 0
  fi
  printf '%s' "$output"
}

smart_health_is_ok() {
  local value
  value="$(trim "${1-}")"
  value="${value,,}"
  [[ -n "$value" ]] || return 1
  [[ "$value" == *passed* || "$value" == *ok* ]]
}

collect_disk_smart_status() {
  if ! have_command lsblk || ! have_command smartctl; then
    printf 'not_tested'
    return 0
  fi
  local saw_any=0
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    declare -A row=()
    parse_kv_line "$line" row
    [[ "${row[TYPE]:-}" == "disk" ]] || continue
    local health
    health="$(smart_health_for_disk "${row[NAME]:-}")"
    health="$(trim "$health")"
    [[ -n "$health" ]] || continue
    saw_any=1
    if ! smart_health_is_ok "$health"; then
      printf 'nok'
      return 0
    fi
  done < <(lsblk -P -b -d -o NAME,TYPE 2>/dev/null || true)

  if (( saw_any )); then
    printf 'ok'
  else
    printf 'not_tested'
  fi
}

collect_disks_json() {
  have_command lsblk || { printf '[]'; return 0; }
  local disk_items=()
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    declare -A row=()
    parse_kv_line "$line" row
    [[ "${row[TYPE]:-}" == "disk" ]] || continue

    local size_gb
    size_gb="$(bytes_to_gb "${row[SIZE]:-}")"
    local media_detail="HDD"
    if [[ "${row[TRAN]:-}" == "nvme" ]]; then
      media_detail="NVMe"
    elif [[ "${row[ROTA]:-}" == "0" ]]; then
      media_detail="SSD"
    fi

    local smart_health
    smart_health="$(smart_health_for_disk "${row[NAME]:-}")"
    local fields=()
    append_json_string_field fields "name" "$(trim "${row[NAME]:-}")"
    append_json_string_field fields "model" "$(trim "${row[MODEL]:-}")"
    append_json_string_field fields "vendor" "$(trim "${row[VENDOR]:-}")"
    append_json_string_field fields "serialNumber" "$(trim "${row[SERIAL]:-}")"
    append_json_string_field fields "interface" "$(trim "${row[TRAN]:-}")"
    append_json_string_field fields "mediaTypeDetail" "$media_detail"
    append_json_string_field fields "tag" "$(trim "${row[NAME]:-}")"
    append_json_number_field fields "sizeGb" "$size_gb"
    append_json_boolean_field fields "rotational" "$([[ "${row[ROTA]:-}" == "1" ]] && printf 'true' || printf 'false')"
    append_json_string_field fields "smartHealth" "$smart_health"
    disk_items+=("$(json_build_object "${fields[@]}")")
  done < <(lsblk -P -b -d -o NAME,SIZE,ROTA,TYPE,MODEL,SERIAL,TRAN,VENDOR 2>/dev/null || true)

  json_build_array "${disk_items[@]}"
}

collect_volumes_json() {
  local volume_items=()
  local line
  while IFS=$'\t' read -r filesystem fstype size_bytes used_bytes avail_bytes percent mountpoint; do
    [[ -n "$mountpoint" ]] || continue
    local options
    options="$(findmnt -no OPTIONS --target "$mountpoint" 2>/dev/null || true)"
    local read_only="false"
    if [[ ",$options," == *,ro,* ]]; then
      read_only="true"
    fi
    local fields=()
    append_json_string_field fields "source" "$(trim "$filesystem")"
    append_json_string_field fields "fileSystem" "$(trim "$fstype")"
    append_json_string_field fields "mountPoint" "$(trim "$mountpoint")"
    append_json_number_field fields "sizeGb" "$(bytes_to_gb "$size_bytes")"
    append_json_number_field fields "usedGb" "$(bytes_to_gb "$used_bytes")"
    append_json_number_field fields "freeGb" "$(bytes_to_gb "$avail_bytes")"
    append_json_number_field fields "percentUsed" "${percent%%%}"
    append_json_boolean_field fields "readOnly" "$read_only"
    volume_items+=("$(json_build_object "${fields[@]}")")
  done < <(
    df -P -B1T -x tmpfs -x devtmpfs 2>/dev/null |
      awk 'NR > 1 { print $1 "\t" $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 "\t" $7 }'
  )
  json_build_array "${volume_items[@]}"
}

collect_memory_inventory_json() {
  have_command dmidecode || { printf '[]'; return 0; }
  local items=()
  local current_locator="" current_serial="" current_size="" current_type="" current_speed=""
  local in_device=0
  while IFS= read -r raw_line; do
    local line
    line="$(trim "$raw_line")"
    if [[ "$line" == "Memory Device" ]]; then
      if (( in_device )) && [[ -n "$current_locator$current_serial$current_size$current_type$current_speed" ]]; then
        local fields=()
        append_json_string_field fields "bankLabel" "$current_locator"
        append_json_string_field fields "serialNumber" "$current_serial"
        append_json_string_field fields "size" "$current_size"
        append_json_string_field fields "type" "$current_type"
        append_json_string_field fields "speed" "$current_speed"
        items+=("$(json_build_object "${fields[@]}")")
      fi
      in_device=1
      current_locator=""
      current_serial=""
      current_size=""
      current_type=""
      current_speed=""
      continue
    fi
    (( in_device )) || continue
    case "$line" in
      Locator:*)
        current_locator="$(trim "${line#Locator:}")"
        ;;
      Serial\ Number:*)
        current_serial="$(trim "${line#Serial Number:}")"
        ;;
      Size:*)
        current_size="$(trim "${line#Size:}")"
        ;;
      Type:*)
        current_type="$(trim "${line#Type:}")"
        ;;
      Speed:*)
        current_speed="$(trim "${line#Speed:}")"
        ;;
    esac
  done < <(dmidecode -t memory 2>/dev/null || true)

  if (( in_device )) && [[ -n "$current_locator$current_serial$current_size$current_type$current_speed" ]]; then
    local fields=()
    append_json_string_field fields "bankLabel" "$current_locator"
    append_json_string_field fields "serialNumber" "$current_serial"
    append_json_string_field fields "size" "$current_size"
    append_json_string_field fields "type" "$current_type"
    append_json_string_field fields "speed" "$current_speed"
    items+=("$(json_build_object "${fields[@]}")")
  fi

  json_build_array "${items[@]}"
}

collect_network_interfaces_json() {
  local items=()
  local iface
  for iface_path in /sys/class/net/*; do
    [[ -e "$iface_path" ]] || continue
    iface="${iface_path##*/}"
    [[ "$iface" != "lo" ]] || continue
    local mac operstate speed ipv4 ipv6
    mac="$(tr '[:lower:]' '[:upper:]' <"$iface_path/address" 2>/dev/null || true)"
    operstate="$(<"$iface_path/operstate" 2>/dev/null || true)"
    speed="$(<"$iface_path/speed" 2>/dev/null || true)"
    ipv4=""
    ipv6=""
    if have_command ip; then
      ipv4="$(ip -o -4 addr show dev "$iface" 2>/dev/null | awk '{ print $4 }' | paste -sd ',' -)"
      ipv6="$(ip -o -6 addr show dev "$iface" scope global 2>/dev/null | awk '{ print $4 }' | paste -sd ',' -)"
    fi
    local fields=()
    append_json_string_field fields "name" "$iface"
    append_json_string_field fields "macAddress" "$(trim "$mac")"
    append_json_string_field fields "state" "$(trim "$operstate")"
    append_json_number_field fields "speedMbps" "$(trim "$speed")"
    append_json_string_field fields "ipv4" "$ipv4"
    append_json_string_field fields "ipv6" "$ipv6"
    items+=("$(json_build_object "${fields[@]}")")
  done
  json_build_array "${items[@]}"
}

collect_ping_status() {
  if ! have_command ping; then
    printf 'not_tested'
    return 0
  fi
  if ping -c 1 -W "$PING_TIMEOUT_SECONDS" "$PING_TARGET" >/dev/null 2>&1; then
    printf 'ok'
  else
    printf 'nok'
  fi
}

collect_fs_check_status() {
  local threshold="$FS_USAGE_NOK_THRESHOLD"
  local result="ok"
  local line
  while IFS=$'\t' read -r filesystem _fstype _size _used _avail percent mountpoint; do
    [[ -n "$mountpoint" ]] || continue
    local used_percent="${percent%%%}"
    if [[ "$used_percent" =~ ^[0-9]+$ ]] && (( used_percent >= threshold )); then
      result="nok"
      break
    fi
    local options
    options="$(findmnt -no OPTIONS --target "$mountpoint" 2>/dev/null || true)"
    if [[ ",$options," == *,ro,* ]]; then
      result="nok"
      break
    fi
  done < <(
    df -P -B1T -x tmpfs -x devtmpfs 2>/dev/null |
      awk 'NR > 1 { print $1 "\t" $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 "\t" $7 }'
  )
  printf '%s' "$result"
}

collect_failed_services_json() {
  if ! have_command systemctl; then
    printf '[]'
    return 0
  fi
  local items=()
  local service
  while IFS= read -r service; do
    service="$(trim "$service")"
    [[ -n "$service" ]] || continue
    items+=("$(json_quote "$service")")
  done < <(systemctl --failed --plain --no-legend --type=service 2>/dev/null | awk '{ print $1 }')
  json_build_array "${items[@]}"
}

collect_selected_services_json() {
  local configured=()
  if [[ -n "${MMA_SERVER_SERVICE_NAMES:-}" ]]; then
    IFS=',' read -r -a configured <<<"${MMA_SERVER_SERVICE_NAMES}"
  fi
  configured+=("${EXTRA_SERVICE_NAMES[@]}")
  if ((${#configured[@]} == 0)); then
    printf '[]'
    return 0
  fi
  local items=()
  local service
  for service in "${configured[@]}"; do
    service="$(trim "$service")"
    [[ -n "$service" ]] || continue
    local active_state="unknown" sub_state="unknown"
    if have_command systemctl; then
      active_state="$(systemctl is-active "$service" 2>/dev/null || true)"
      active_state="$(trim "$active_state")"
      [[ -n "$active_state" ]] || active_state="unknown"
      sub_state="$(systemctl show -p SubState --value "$service" 2>/dev/null || true)"
      sub_state="$(trim "$sub_state")"
      [[ -n "$sub_state" ]] || sub_state="unknown"
    fi
    local fields=()
    append_json_string_field fields "name" "$service"
    append_json_string_field fields "activeState" "$active_state"
    append_json_string_field fields "subState" "$sub_state"
    items+=("$(json_build_object "${fields[@]}")")
  done
  json_build_array "${items[@]}"
}

collect_raid_json() {
  if [[ ! -r /proc/mdstat ]]; then
    printf ''
    return 0
  fi
  local mdstat raid_status
  mdstat="$(< /proc/mdstat)"
  mdstat="$(trim "$mdstat")"
  [[ -n "$mdstat" ]] || return 0
  raid_status="ok"
  if grep -Eq '\[[U_]+\]' /proc/mdstat && grep -Eq '_' /proc/mdstat; then
    raid_status="nok"
  fi
  local fields=()
  append_json_string_field fields "status" "$raid_status"
  append_json_string_field fields "mdstat" "$mdstat"
  json_build_object "${fields[@]}"
}

collect_thermal_json() {
  local max_millic=0
  local found=0
  local zone temp
  for zone in /sys/class/thermal/thermal_zone*; do
    [[ -r "$zone/temp" ]] || continue
    temp="$(<"$zone/temp" 2>/dev/null || true)"
    if [[ "$temp" =~ ^-?[0-9]+$ ]]; then
      found=1
      if (( temp > max_millic )); then
        max_millic=$temp
      fi
    fi
  done
  if (( ! found )); then
    printf ''
    return 0
  fi
  local max_celsius
  max_celsius="$(awk -v value="$max_millic" 'BEGIN { printf "%.1f", value / 1000 }')"
  local status="ok"
  awk -v value="$max_celsius" 'BEGIN { exit !(value >= 85) }' && status="nok"
  local fields=()
  append_json_string_field fields "status" "$status"
  append_json_number_field fields "maxCelsius" "$max_celsius"
  json_build_object "${fields[@]}"
}

collect_baseboard_json() {
  local board_vendor board_name board_serial
  board_vendor="$(read_first_file_value /sys/class/dmi/id/board_vendor 2>/dev/null || true)"
  board_name="$(read_first_file_value /sys/class/dmi/id/board_name 2>/dev/null || true)"
  board_serial="$(read_first_file_value /sys/class/dmi/id/board_serial 2>/dev/null || true)"
  local fields=()
  append_json_string_field fields "vendor" "$board_vendor"
  append_json_string_field fields "name" "$board_name"
  append_json_string_field fields "serialNumber" "$board_serial"
  json_build_object "${fields[@]}"
}

collect_primary_ips() {
  local ipv4="" ipv6=""
  if have_command ip; then
    ipv4="$(ip route get 1.1.1.1 2>/dev/null | awk '{
      for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit }
    }' || true)"
    ipv6="$(ip -6 route get 2606:4700:4700::1111 2>/dev/null | awk '{
      for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit }
    }' || true)"
  fi
  printf '%s\t%s\n' "$(trim "$ipv4")" "$(trim "$ipv6")"
}

collect_default_gateway() {
  if have_command ip; then
    ip route show default 2>/dev/null | awk '/default/ { print $3; exit }'
  fi
}

collect_ram_mb() {
  awk '/MemTotal:/ { printf "%d\n", int($2 / 1024); exit }' /proc/meminfo 2>/dev/null || true
}

collect_uptime_seconds() {
  awk '{ printf "%d\n", int($1); exit }' /proc/uptime 2>/dev/null || true
}

collect_load_averages() {
  awk '{ print $1 "\t" $2 "\t" $3; exit }' /proc/loadavg 2>/dev/null || true
}

collect_serial_number() {
  read_first_file_value \
    /sys/class/dmi/id/product_serial \
    /sys/class/dmi/id/board_serial \
    /sys/class/dmi/id/chassis_serial 2>/dev/null || true
}

collect_vendor_name() {
  read_first_file_value /sys/class/dmi/id/sys_vendor 2>/dev/null || true
}

collect_model_name() {
  local product version
  product="$(read_first_file_value /sys/class/dmi/id/product_name 2>/dev/null || true)"
  version="$(read_first_file_value /sys/class/dmi/id/product_version 2>/dev/null || true)"
  if [[ -n "$product" && -n "$version" && "$version" != "None" ]]; then
    printf '%s %s' "$product" "$version"
    return 0
  fi
  printf '%s' "$product"
}

post_payload() {
  local payload_file=$1
  local response_file http_code
  response_file="$(mktemp)"
  local curl_args=(
    --silent
    --show-error
    --output "$response_file"
    --write-out '%{http_code}'
    --max-time "$CURL_TIMEOUT_SECONDS"
    --connect-timeout 5
    -H 'Content-Type: application/json'
    --data-binary "@$payload_file"
  )
  if [[ "$REQUEST_INSECURE" == "1" ]]; then
    curl_args+=(-k)
  fi
  http_code="$(curl "${curl_args[@]}" "$API_URL")"
  if [[ ! "$http_code" =~ ^2 ]]; then
    printf 'Ingest failed (HTTP %s)\n' "$http_code" >&2
    cat "$response_file" >&2
    rm -f "$response_file"
    return 1
  fi
  cat "$response_file"
  rm -f "$response_file"
}

while (($# > 0)); do
  case "$1" in
    --api-url)
      API_URL="${2-}"
      shift 2
      ;;
    --technician)
      TECHNICIAN="${2-}"
      shift 2
      ;;
    --tag)
      TAG_NAME="${2-}"
      shift 2
      ;;
    --ping-target)
      PING_TARGET="${2-}"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="${2-}"
      shift 2
      ;;
    --service)
      EXTRA_SERVICE_NAMES+=("${2-}")
      shift 2
      ;;
    --insecure)
      REQUEST_INSECURE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

hostname_value="$(collect_hostname)"
preferred_iface="$(collect_primary_route_interface)"
primary_mac="$(collect_primary_mac "$preferred_iface")"
mac_array_json="$(collect_mac_array_json "$preferred_iface")"
serial_number="$(collect_serial_number)"
vendor_name="$(collect_vendor_name)"
model_name="$(collect_model_name)"
os_version="$(collect_os_version)"
ram_mb="$(collect_ram_mb)"
report_id="$(collect_report_id)"
generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ping_status="$(collect_ping_status)"
fs_check_status="$(collect_fs_check_status)"
cpu_json="$(collect_cpu_json)"
gpu_json="$(collect_gpu_json)"
disks_json="$(collect_disks_json)"
volumes_json="$(collect_volumes_json)"
memory_json="$(collect_memory_inventory_json)"
network_interfaces_json="$(collect_network_interfaces_json)"
failed_services_json="$(collect_failed_services_json)"
selected_services_json="$(collect_selected_services_json)"
raid_json="$(collect_raid_json)"
thermal_json="$(collect_thermal_json)"
disk_smart_status="$(collect_disk_smart_status)"
baseboard_json="$(collect_baseboard_json)"
default_gateway="$(collect_default_gateway)"
read -r primary_ipv4 primary_ipv6 < <(collect_primary_ips)
uptime_seconds="$(collect_uptime_seconds)"
read -r load1 load5 load15 < <(collect_load_averages)

network_fields=()
append_json_string_field network_fields "defaultGateway" "$(trim "$default_gateway")"
append_json_string_field network_fields "primaryIpv4" "$(trim "$primary_ipv4")"
append_json_string_field network_fields "primaryIpv6" "$(trim "$primary_ipv6")"
append_json_raw_field network_fields "interfaces" "$network_interfaces_json"
network_json="$(json_build_object "${network_fields[@]}")"

inventory_fields=()
append_json_raw_field inventory_fields "baseboard" "$baseboard_json"
append_json_raw_field inventory_fields "disks" "$disks_json"
append_json_raw_field inventory_fields "memory" "$memory_json"
append_json_raw_field inventory_fields "networkInterfaces" "$network_interfaces_json"
inventory_json="$(json_build_object "${inventory_fields[@]}")"

tests_fields=()
append_json_string_field tests_fields "networkPing" "$ping_status"
append_json_string_field tests_fields "networkPingTarget" "$PING_TARGET"
append_json_string_field tests_fields "fsCheck" "$fs_check_status"
tests_json="$(json_build_object "${tests_fields[@]}")"

server_fields=()
append_json_number_field server_fields "uptimeSeconds" "$uptime_seconds"
append_json_number_field server_fields "loadAverage1m" "$load1"
append_json_number_field server_fields "loadAverage5m" "$load5"
append_json_number_field server_fields "loadAverage15m" "$load15"
append_json_raw_field server_fields "failedServices" "$failed_services_json"
append_json_raw_field server_fields "selectedServices" "$selected_services_json"
append_json_raw_field server_fields "raid" "$raid_json"
server_json="$(json_build_object "${server_fields[@]}")"

component_fields=()
append_json_string_field component_fields "networkPing" "$ping_status"
append_json_string_field component_fields "fsCheck" "$fs_check_status"
if [[ "$disk_smart_status" != "not_tested" ]]; then
  append_json_string_field component_fields "diskSmart" "$disk_smart_status"
fi
if [[ -n "$raid_json" ]]; then
  raid_status=""
  if [[ "$raid_json" =~ \"status\":\"([^\"]+)\" ]]; then
    raid_status="${BASH_REMATCH[1]}"
  fi
  raid_status="$(trim "$raid_status")"
  if [[ -n "$raid_status" ]]; then
    append_json_string_field component_fields "serverRaid" "$raid_status"
  fi
fi
server_services_status=""
if [[ "$selected_services_json" != "[]" ]]; then
  server_services_status="ok"
fi
if [[ "$failed_services_json" != "[]" ]]; then
  server_services_status="nok"
fi
if [[ "$selected_services_json" =~ \"activeState\":\"([^\"]+)\" ]]; then
  selected_services_scan="$selected_services_json"
  while [[ "$selected_services_scan" =~ \"activeState\":\"([^\"]+)\" ]]; do
    active_state="${BASH_REMATCH[1]}"
    if [[ "$(trim "$active_state")" != "active" ]]; then
      server_services_status="nok"
      break
    fi
    selected_services_scan="${selected_services_scan#*\"activeState\":\"$active_state\"}"
  done
fi
if [[ -n "$server_services_status" ]]; then
  append_json_string_field component_fields "serverServices" "$server_services_status"
fi
if [[ -n "$thermal_json" ]]; then
  thermal_status=""
  if [[ "$thermal_json" =~ \"status\":\"([^\"]+)\" ]]; then
    thermal_status="${BASH_REMATCH[1]}"
  fi
  thermal_status="$(trim "$thermal_status")"
  if [[ -n "$thermal_status" ]]; then
    append_json_string_field component_fields "thermal" "$thermal_status"
  fi
fi
components_json="$(json_build_object "${component_fields[@]}")"

diag_count=2
[[ -n "$thermal_json" ]] && diag_count=$((diag_count + 1))
[[ "$disk_smart_status" != "not_tested" ]] && diag_count=$((diag_count + 1))
[[ -n "${raid_status:-}" ]] && diag_count=$((diag_count + 1))
[[ -n "$server_services_status" ]] && diag_count=$((diag_count + 1))
diag_fields=()
append_json_string_field diag_fields "type" "linux_server"
append_json_number_field diag_fields "diagnosticsPerformed" "$diag_count"
append_json_string_field diag_fields "appVersion" "mma-server-bash/1.0"
append_json_string_field diag_fields "completedAt" "$generated_at"
diag_json="$(json_build_object "${diag_fields[@]}")"

payload_fields=()
append_json_string_field payload_fields "reportId" "$report_id"
append_json_string_field payload_fields "generatedAt" "$generated_at"
append_json_string_field payload_fields "hostname" "$hostname_value"
append_json_string_field payload_fields "serialNumber" "$serial_number"
append_json_string_field payload_fields "macAddress" "$primary_mac"
append_json_raw_field payload_fields "macAddresses" "$mac_array_json"
append_json_string_field payload_fields "category" "server"
append_json_string_field payload_fields "vendor" "$vendor_name"
append_json_string_field payload_fields "model" "$model_name"
append_json_string_field payload_fields "technician" "$TECHNICIAN"
append_json_string_field payload_fields "osVersion" "$os_version"
append_json_string_field payload_fields "tag" "$TAG_NAME"
append_json_number_field payload_fields "ramMb" "$ram_mb"
append_json_raw_field payload_fields "cpu" "$cpu_json"
append_json_raw_field payload_fields "gpu" "$gpu_json"
append_json_raw_field payload_fields "disks" "$disks_json"
append_json_raw_field payload_fields "volumes" "$volumes_json"
append_json_raw_field payload_fields "tests" "$tests_json"
append_json_raw_field payload_fields "network" "$network_json"
append_json_raw_field payload_fields "inventory" "$inventory_json"
append_json_raw_field payload_fields "server" "$server_json"
append_json_raw_field payload_fields "thermal" "$thermal_json"
append_json_raw_field payload_fields "diag" "$diag_json"
append_json_raw_field payload_fields "components" "$components_json"

payload_json="$(json_build_object "${payload_fields[@]}")"

payload_file=""
cleanup_payload_file=0
if [[ -n "$OUTPUT_FILE" ]]; then
  payload_file="$OUTPUT_FILE"
  mkdir -p "$(dirname "$payload_file")"
else
  payload_file="$(mktemp)"
  cleanup_payload_file=1
fi

printf '%s\n' "$payload_json" >"$payload_file"

if (( DRY_RUN )); then
  if [[ -z "$OUTPUT_FILE" ]]; then
    cat "$payload_file"
  else
    printf 'Payload saved to %s\n' "$payload_file"
  fi
else
  response="$(post_payload "$payload_file")"
  printf '%s\n' "$response"
fi

if (( cleanup_payload_file )); then
  rm -f "$payload_file"
fi
