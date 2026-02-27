#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import uuid
import time
from urllib import request, error


STATUS_OK = {"OK", "OUI", "O", "YES", "Y", "BON", "GOOD", "C"}
STATUS_NOK = {"NOK", "KO", "NON", "N", "NO", "BAD", "HS"}
STATUS_NT = {"NT", "N/T", "NA", "N/A", "?", "NONTESTE", "NONTESTEE"}
STATUS_ABSENT = {"ABSENT", "ABS", "MISSING"}


def normalize_header(value):
    if value is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()


def normalize_lookup_key(value):
    if value is None:
        return ""
    return re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()


def normalize_serial(value):
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned.replace(" ", "").upper()


def normalize_mac(value):
    if value is None:
        return None
    raw = re.sub(r"[^0-9A-Fa-f]", "", str(value))
    if len(raw) != 12:
        return None
    raw = raw.upper()
    return ":".join(raw[i : i + 2] for i in range(0, 12, 2))


def normalize_status(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = raw.upper().replace(" ", "")
    if cleaned in STATUS_OK:
        return "ok"
    if cleaned in STATUS_NOK:
        return "nok"
    if cleaned in STATUS_NT:
        return "not_tested"
    if cleaned in STATUS_ABSENT:
        return "absent"
    return raw


def parse_date(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.replace(".", "/").replace("-", "/")
    parts = [p for p in raw.split("/") if p]
    if len(parts) != 3:
        return None
    try:
        a, b, c = parts
        if len(c) == 2:
            c = "20" + c
        day = int(a)
        month = int(b)
        year = int(c)
        if month > 12 and day <= 12:
            day, month = month, day
        if day > 31 or month > 12:
            return None
        return dt.datetime(year, month, day, 12, 0, 0, tzinfo=dt.timezone.utc).isoformat()
    except ValueError:
        return None


def is_serial_candidate(value):
    if value is None:
        return False
    raw = str(value).strip()
    if not raw:
        return False
    if ("/" in raw or "-" in raw) and parse_date(raw):
        return False
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw)
    if len(cleaned) < 5 or len(cleaned) > 20:
        return False
    if not re.search(r"\d", cleaned):
        return False
    return True


def guess_category(model, filename):
    text = f"{model or ''} {filename or ''}".lower()
    desktop_keys = [
        "optiplex",
        "prodesk",
        "pro desk",
        "thinkcentre",
        "esprimo",
        "micro",
        "mt",
        "g4 mt",
        "g5 mt",
        "m720q",
    ]
    if any(key in text for key in desktop_keys):
        return "desktop"
    laptop_keys = ["thinkpad", "latitude", "probook", "elitebook", "x1", "t490", "t14", "l13", "l390"]
    if any(key in text for key in laptop_keys):
        return "laptop"
    return "unknown"


def derive_tag(filename):
    if not filename:
        return "En cours"
    base = os.path.splitext(os.path.basename(filename))[0]
    base = re.sub(r"^prod\\s*-\\s*", "", base, flags=re.IGNORECASE).strip()
    return base or "En cours"


def safe_key(value):
    if not value:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9._-]", "-", value)


def pick_first(row, header_map, keys):
    for key in keys:
        norm = normalize_header(key)
        if norm not in header_map:
            continue
        for idx in header_map[norm]:
            if idx < len(row):
                value = row[idx]
                if value is not None and str(value).strip():
                    return value
    return None


def build_artifact_index(roots):
    index = {}
    for root in roots:
        if not root or not os.path.exists(root):
            continue
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                base, _ = os.path.splitext(name)
                key = normalize_lookup_key(base)
                if not key or len(key) < 5:
                    continue
                index.setdefault(key, []).append(os.path.join(dirpath, name))
    return index


def ensure_mc_alias(mc_path, endpoint, access_key, secret_key):
    cmd = [mc_path, "alias", "set", "diagobj", endpoint, access_key, secret_key]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


def mirror_artifacts(mc_path, src_dir, bucket, prefix, tag, mac_serial_key, report_id):
    tag_segment = safe_key(tag) if tag else "en-cours"
    dest = f"diagobj/{bucket}/{prefix}/{tag_segment}/{safe_key(mac_serial_key)}/{report_id}"
    cmd = [mc_path, "mirror", "--overwrite", src_dir, dest]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0, dest


def post_payload(url, payload, insecure):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = request.Request(url, data=data, headers=headers, method="POST")
    context = None
    if insecure and url.startswith("https://"):
        context = ssl._create_unverified_context()
    try:
        with request.urlopen(req, context=context, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body, resp.headers
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body, exc.headers
    except Exception as exc:
        return None, str(exc), {}


def main():
    parser = argparse.ArgumentParser(description="Import legacy CSVs into hydra-dev ingest API.")
    parser.add_argument("--csv-dir", default="/home/christopher/tmp")
    parser.add_argument("--api-url", default="https://hydra-dev.local/api/ingest")
    parser.add_argument("--insecure", action="store_true", help="Skip TLS verification for https.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--artifact-root", default="/mnt/diag")
    parser.add_argument("--upload-artifacts", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=400, help="Delay between requests (ms).")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries on 429/5xx.")
    parser.add_argument(
        "--skip-file",
        action="append",
        default=[],
        help="CSV filename to skip (repeatable, basename match)."
    )
    parser.add_argument(
        "--start-after",
        default="",
        help="Resume in a CSV after a marker. Format: 'filename:marker' (marker = serial or mac)."
    )
    parser.add_argument("--mc-path", default="mc")
    parser.add_argument("--bucket", default="alcyone-archive")
    parser.add_argument("--prefix", default="run")
    parser.add_argument("--endpoint", default="http://10.1.10.28:9000")
    parser.add_argument("--access-key", default="codexminio")
    parser.add_argument("--secret-key", default="semngIYo36sZq27tixYVXeFF")
    args = parser.parse_args()

    csv_dir = args.csv_dir
    if not os.path.isdir(csv_dir):
        print(f"[ERROR] CSV directory not found: {csv_dir}")
        return 1

    csv_files = sorted(
        [os.path.join(csv_dir, name) for name in os.listdir(csv_dir) if name.lower().endswith(".csv")]
    )
    if not csv_files:
        print(f"[WARN] No CSV files found in {csv_dir}")
        return 0

    artifact_index = {}
    if args.upload_artifacts and os.path.isdir(args.artifact_root):
        roots = [
            os.path.join(args.artifact_root, "Prod en cours"),
            os.path.join(args.artifact_root, "Prod termin\u00e9"),
            os.path.join(args.artifact_root, "SAV-REPARATION"),
            os.path.join(args.artifact_root, "prod secours"),
        ]
        print("[INFO] Indexing artifact files...")
        artifact_index = build_artifact_index(roots)
        print(f"[INFO] Indexed {len(artifact_index)} artifact keys.")

    if args.upload_artifacts:
        if shutil.which(args.mc_path) is None:
            print(f"[WARN] mc not found at {args.mc_path}. Artifact upload disabled.")
            args.upload_artifacts = False
        elif not ensure_mc_alias(args.mc_path, args.endpoint, args.access_key, args.secret_key):
            print("[WARN] Failed to configure mc alias. Artifact upload disabled.")
            args.upload_artifacts = False

    processed = 0
    ingested = 0
    skipped = 0
    skip_files = {os.path.basename(name) for name in args.skip_file if name}
    resume_file = ""
    resume_marker = ""
    resume_active = False
    if args.start_after:
        parts = args.start_after.split(":", 1)
        if len(parts) == 2:
            resume_file = parts[0].strip()
            resume_marker = normalize_lookup_key(parts[1])
            resume_active = bool(resume_file and resume_marker)

    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        if filename in skip_files:
            print(f"[SKIP] {filename} (skip-file)")
            continue
        with open(csv_path, newline="", encoding="utf-8-sig", errors="replace") as fh:
            reader = csv.reader(fh)
            try:
                headers = next(reader)
            except StopIteration:
                continue
            header_map = {}
            for idx, header in enumerate(headers):
                norm = normalize_header(header)
                header_map.setdefault(norm, []).append(idx)

            resume_hit = False
            for row in reader:
                if args.limit and processed >= args.limit:
                    break

                serial_raw = pick_first(
                    row, header_map, ["NUM_SERIE", "S/N", "SN", "SERIAL", "SERIALNUMBER"]
                )
                serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
                mac = normalize_mac(
                    pick_first(row, header_map, ["NUMERO_MAC", "NUM_MAC", "MAC", "MACADDRESS"])
                )
                if resume_active and filename == resume_file and not resume_hit:
                    marker = normalize_lookup_key(serial) or normalize_lookup_key(mac)
                    if marker and marker == resume_marker:
                        resume_hit = True
                    continue
                tech = pick_first(row, header_map, ["TECHNICIEN", "TECH"])
                model = pick_first(row, header_map, ["MODELE", "MODEL"])
                vendor = pick_first(row, header_map, ["FOURNISSEUR", "VENDOR"])

                if not serial and not mac:
                    skipped += 1
                    continue

                components = {}
                status_fields = {
                    "camera": ["CAMERA"],
                    "pad": ["PAD", "PAVETACTILE"],
                    "touchscreen": ["TACTILE", "TACTILEECRAN"],
                    "audio": ["SON"],
                    "keyboard": ["CLAVIER"],
                    "battery": ["BATTERIE", "BATTERIE75", "BATTERIE75%"],
                    "biosBattery": ["PILEBIOS"],
                    "usb": ["PORTSUSB", "PORUSB", "PORTUSB"],
                    "badgeReader": ["LECTEURBADGE", "BADGE"],
                    "wifi": ["WIFI"],
                    "userDiag": ["USERDIAG", "USERDIAGNO", "USERDIAGOK"],
                    "aesthetic": ["ESTHETIQUE"],
                }

                for key, cols in status_fields.items():
                    raw = pick_first(row, header_map, cols)
                    status = normalize_status(raw)
                    if status:
                        components[key] = status

                problems = []
                for col in ["PROBLEME", "PROBLEME(S)", "PROBLEMES", "PROBLEMECLIENT", "REMARQUE", "COMMENTAIRE"]:
                    value = pick_first(row, header_map, [col])
                    if value and str(value).strip():
                        problems.append(str(value).strip())

                date_traitement = pick_first(
                    row, header_map, ["DATE_TRAITEMENT", "DATE", "DATE_RECEPTION", "DATE_RETOUR_SAV"]
                )
                parsed_date = parse_date(date_traitement)

                category = guess_category(model, filename)
                tag_value = derive_tag(filename)
                report_id = str(uuid.uuid4())
                payload = {
                    "reportId": report_id,
                    "serialNumber": serial,
                    "macAddress": mac,
                    "technician": tech,
                    "model": model,
                    "vendor": vendor,
                    "category": category,
                    "tag": tag_value,
                    "components": components,
                    "cameraStatus": components.get("camera"),
                    "padStatus": components.get("pad"),
                    "keyboardStatus": components.get("keyboard"),
                    "usbStatus": components.get("usb"),
                    "badgeReaderStatus": components.get("badgeReader"),
                    "source": "legacy-csv-import",
                    "legacy": {
                        "file": filename,
                        "row": {headers[i] if i < len(headers) else f"col_{i}": row[i] for i in range(len(row))},
                        "dateTraitement": date_traitement,
                        "dateParsed": parsed_date,
                        "problems": problems,
                    },
                }

                mac_serial_key = None
                if serial:
                    mac_serial_key = f"sn:{serial}"
                elif mac:
                    mac_serial_key = f"mac:{mac}"

                upload_info = None
                if args.upload_artifacts and mac_serial_key:
                    lookup_keys = [normalize_lookup_key(serial), normalize_lookup_key(mac)]
                    matches = []
                    for key in lookup_keys:
                        if key:
                            matches.extend(artifact_index.get(key, []))
                    matches = list(dict.fromkeys(matches))
                    if matches:
                        run_dir = tempfile.mkdtemp(prefix="mdt-legacy-")
                        for path in matches:
                            try:
                                shutil.copy2(path, os.path.join(run_dir, os.path.basename(path)))
                            except OSError:
                                continue
                        manifest_path = os.path.join(run_dir, "manifest.json")
                        with open(manifest_path, "w", encoding="utf-8") as mf:
                            json.dump({"sourcePaths": matches}, mf, ensure_ascii=False, indent=2)
                        success, dest = mirror_artifacts(
                            args.mc_path,
                            run_dir,
                            args.bucket,
                            args.prefix,
                            tag_value,
                            mac_serial_key,
                            report_id
                        )
                        upload_info = {
                            "uploaded": success,
                            "endpoint": args.endpoint,
                            "bucket": args.bucket,
                            "prefix": args.prefix,
                            "tag": tag_value,
                            "destination": dest,
                        }
                        shutil.rmtree(run_dir, ignore_errors=True)

                if upload_info:
                    payload["rawArtifacts"] = upload_info

                processed += 1
                if args.dry_run:
                    print(f"[DRY] {serial or mac} -> {payload['category']} {filename}")
                    ingested += 1
                    continue

                attempt = 0
                while True:
                    code, body, resp_headers = post_payload(args.api_url, payload, args.insecure)
                    if code and 200 <= code < 300:
                        ingested += 1
                        print(f"[OK] {serial or mac} ({filename})")
                        break
                    retryable = code in {429, 500, 502, 503, 504} or code is None
                    if retryable and attempt < args.max_retries:
                        attempt += 1
                        retry_after = None
                        if resp_headers:
                            retry_after = resp_headers.get("Retry-After")
                        wait = None
                        if retry_after:
                            try:
                                wait = float(retry_after)
                            except ValueError:
                                wait = None
                        if wait is None:
                            wait = max(1.0, (args.sleep_ms / 1000.0) * (1 + attempt))
                        print(f"[WARN] {serial or mac} ({filename}) -> {code}, retry in {wait:.1f}s")
                        time.sleep(wait)
                        continue
                    print(f"[ERR] {serial or mac} ({filename}) -> {code} {body}")
                    break

                if args.sleep_ms and args.sleep_ms > 0:
                    time.sleep(args.sleep_ms / 1000.0)

        if args.limit and processed >= args.limit:
            break

    print(f"[DONE] processed={processed} ingested={ingested} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
