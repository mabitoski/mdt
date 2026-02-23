import csv
import datetime as dt
import json
import os
import re
import ssl
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import request, error

STATUS_OK = {"OK", "OUI", "O", "YES", "Y", "BON", "GOOD", "C"}
STATUS_NOK = {"NOK", "KO", "NON", "N", "NO", "BAD", "HS"}
STATUS_NT = {"NT", "N/T", "NA", "N/A", "?", "NONTESTE", "NONTESTEE", "NC"}
STATUS_ABSENT = {"ABSENT", "ABS", "MISSING"}

HEADER_TOKENS = {
    "S/N", "SN", "NUM_SERIE", "NUMEROSERIE", "NUMSERIE", "NUMERO_MAC", "NUM_MAC", "MAC",
    "DATE", "DATE_TRAITEMENT", "DATE_RECEPTION", "DATE_RETOUR_SAV", "MODELE", "MODEL",
    "TECHNICIEN", "ESTHETIQUE", "USER_DIAG", "USERDIAG", "CAMERA", "PAD", "TACTILE",
    "SON", "CLAVIER", "BATTERIE", "PILE BIOS", "PIECE A COMMANDER", "PROBLEME(S)",
    "PROBLEME", "COMMENTAIRE", "OK ?", "OK?", "OK APRES REPARATION ?", "REPARATEUR",
    "DOUBLE CHECK", "PC OK ?", "REPARATION FAITE", "PROBLEME CLIENT", "PIECES A CHANGER",
    "FOURNISSEUR", "PROCESSEUR", "AUTRES", "LECTEUR BADGE", "WIFI",
}


def clean_string(value, limit=128):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


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
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return cleaned or None


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


def normalize_technician(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw in {"?", "OK ?", "OK?"}:
        return None
    lowered = raw.lower()
    return lowered.title()


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
    base = re.sub(r"^prod\s*-\s*", "", base, flags=re.IGNORECASE).strip()
    return base or "En cours"


def read_csv_rows(path):
    with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            class D:  # type: ignore
                delimiter = ","
            dialect = D
        reader = csv.reader(fh, dialect)
        rows = list(reader)
    return rows


def row_is_blank(row):
    return not any(str(cell or "").strip() for cell in row)


def looks_like_header(row):
    score = 0
    for cell in row:
        token = str(cell or "").strip()
        if not token:
            continue
        norm = token.upper()
        if norm in HEADER_TOKENS:
            score += 1
    return score >= 3


def header_positions(header):
    positions = defaultdict(list)
    for idx, cell in enumerate(header):
        key = normalize_header(cell)
        if key:
            positions[key].append(idx)
    return positions


def resolve_spec(row, positions, spec):
    if spec is None:
        return None
    if isinstance(spec, int):
        return row[spec] if spec < len(row) else None
    if isinstance(spec, tuple) and len(spec) == 2:
        key, occurrence = spec
        key_norm = normalize_header(key)
        idxs = positions.get(key_norm, [])
        if len(idxs) >= occurrence:
            idx = idxs[occurrence - 1]
            return row[idx] if idx < len(row) else None
        return None
    if isinstance(spec, list):
        for item in spec:
            value = resolve_spec(row, positions, item)
            if value is not None and str(value).strip() != "":
                return value
        return None
    key_norm = normalize_header(spec)
    idxs = positions.get(key_norm, [])
    if idxs:
        idx = idxs[0]
        return row[idx] if idx < len(row) else None
    return None


def row_to_dict(header, row):
    out = {}
    counts = defaultdict(int)
    for idx, name in enumerate(header):
        key = str(name).strip() or f"col_{idx}"
        counts[key] += 1
        suffix = f"__{counts[key]}" if counts[key] > 1 else ""
        out[f"{key}{suffix}"] = row[idx] if idx < len(row) else ""
    return out


def make_payload(
    *,
    serial,
    mac,
    technician,
    model,
    vendor,
    category,
    tag,
    components,
    legacy,
):
    payload = {
        "reportId": str(uuid.uuid4()),
        "serialNumber": serial,
        "macAddress": mac,
        "technician": technician,
        "model": model,
        "vendor": vendor,
        "category": category,
        "tag": tag,
        "components": components,
        "legacy": legacy,
    }
    if components:
        payload["cameraStatus"] = components.get("camera")
        payload["padStatus"] = components.get("pad")
        payload["keyboardStatus"] = components.get("keyboard")
        payload["usbStatus"] = components.get("usb")
        payload["badgeReaderStatus"] = components.get("badgeReader")
    return payload


def post_payload(url, payload, insecure):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = request.Request(url, data=data, headers=headers, method="POST")
    context = None
    if insecure and url.startswith("https://"):
        context = ssl._create_unverified_context()
    try:
        with request.urlopen(req, context=context, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body, resp.headers
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body, exc.headers
    except Exception as exc:
        return None, str(exc), {}


def submit_payloads(payloads, *, api_url, insecure=False, dry_run=False, sleep_ms=0, max_retries=5, workers=1):
    def send_one(payload):
        if dry_run:
            return True, "dry_run"
        attempt = 0
        while True:
            code, body, headers = post_payload(api_url, payload, insecure)
            if code and 200 <= code < 300:
                return True, body
            retryable = code in {429, 500, 502, 503, 504} or code is None
            if retryable and attempt < max_retries:
                attempt += 1
                retry_after = None
                if headers:
                    retry_after = headers.get("Retry-After")
                wait = None
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = None
                if wait is None:
                    wait = max(1.0, (sleep_ms / 1000.0) * (1 + attempt))
                time.sleep(wait)
                continue
            return False, f"{code} {body}"

    ok = 0
    fail = 0
    if workers <= 1:
        for payload in payloads:
            success, _ = send_one(payload)
            if success:
                ok += 1
            else:
                fail += 1
            if sleep_ms and sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(send_one, payload): payload for payload in payloads}
            for fut in as_completed(futures):
                success, _ = fut.result()
                if success:
                    ok += 1
                else:
                    fail += 1
    return ok, fail
