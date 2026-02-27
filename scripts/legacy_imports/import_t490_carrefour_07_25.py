import argparse
import os

from common import (
    clean_string,
    derive_tag,
    guess_category,
    header_positions,
    is_serial_candidate,
    looks_like_header,
    make_payload,
    normalize_mac,
    normalize_serial,
    normalize_status,
    normalize_technician,
    parse_date,
    read_csv_rows,
    resolve_spec,
    row_is_blank,
    row_to_dict,
    submit_payloads,
)


def find_header_row(rows, token):
    token_upper = token.upper()
    for idx, row in enumerate(rows):
        if not row:
            continue
        first = str(row[0]).strip().upper()
        if first == token_upper:
            return idx
    return None


def parse_segment_one(rows, path, tag, category_map):
    header = rows[0]
    positions = header_positions(header)
    payloads = []
    for idx, row in enumerate(rows[1:], start=2):
        if row_is_blank(row) or looks_like_header(row):
            continue
        serial_raw = resolve_spec(row, positions, "NUM_SERIE")
        serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
        mac = normalize_mac(resolve_spec(row, positions, "NUMERO_MAC"))
        if not serial and not mac:
            continue
        technician = normalize_technician(resolve_spec(row, positions, ("TECHNICIEN", 1)))
        model = clean_string(resolve_spec(row, positions, "MODELE"))
        vendor = clean_string(resolve_spec(row, positions, "FOURNISSEUR"))
        components = {}
        for key, spec in {
            "aesthetic": "ESTHETIQUE",
            "userDiag": "USER_DIAG",
            "camera": "CAMERA",
            "pad": "PAD",
            "touchscreen": "TACTILE",
            "audio": "SON",
            "keyboard": "CLAVIER",
            "battery": "BATTERIE",
            "biosBattery": "PILE BIOS",
        }.items():
            status = normalize_status(resolve_spec(row, positions, spec))
            if status:
                components[key] = status
        overall = normalize_status(resolve_spec(row, positions, ("OK ?", 1)))
        if overall:
            components["overall"] = overall
        components = components or None
        problems = []
        value = resolve_spec(row, positions, "PROBLEME(S)")
        if value and str(value).strip():
            problems.append(str(value).strip())
        date_value = resolve_spec(row, positions, "DATE_RECEPTION")
        legacy = {
            "source": "legacy-t490-carrefour",
            "file": os.path.basename(path),
            "rowIndex": idx,
            "row": row_to_dict(header, row),
            "segment": "initial",
        }
        if date_value:
            legacy["date"] = date_value
            legacy["dateParsed"] = parse_date(date_value)
        if problems:
            legacy["problems"] = problems
        for key, spec in {
            "pieceACommander": "PIECE A COMMANDER",
            "dateRetourSAV": "DATE_RETOUR_SAV",
            "problemeClient": "PROBLEME CLIENT",
            "reparationFaite": "REPARATION FAITE",
            "pcOk": "PC OK ?",
            "piecesAChanger": "PIECES A CHANGER",
        }.items():
            val = resolve_spec(row, positions, spec)
            if val and str(val).strip():
                legacy[key] = val

        category = guess_category(model, os.path.basename(path))
        if category == "unknown":
            if serial and serial in category_map:
                category = category_map[serial]
            elif mac and mac in category_map:
                category = category_map[mac]
        payloads.append(
            make_payload(
                serial=serial,
                mac=mac,
                technician=technician,
                model=model,
                vendor=vendor,
                category=category,
                tag=tag,
                components=components,
                legacy=legacy,
            )
        )

        repair_tech = normalize_technician(resolve_spec(row, positions, "REPARATEUR"))
        if repair_tech:
            repair_ok = resolve_spec(row, positions, "OK APRES REPARATION ?")
            repair_components = None
            status = normalize_status(repair_ok)
            if status:
                repair_components = {"overall": status}
            repair_legacy = dict(legacy)
            repair_legacy["role"] = "repair"
            if repair_ok:
                repair_legacy["ok"] = repair_ok
            payloads.append(
                make_payload(
                    serial=serial,
                    mac=mac,
                    technician=repair_tech,
                    model=model,
                    vendor=vendor,
                    category=category,
                    tag=tag,
                    components=repair_components,
                    legacy=repair_legacy,
                )
            )

        follow_tech = normalize_technician(resolve_spec(row, positions, ("TECHNICIEN", 2)))
        if follow_tech:
            follow_ok = resolve_spec(row, positions, "PC OK ?")
            follow_components = None
            status = normalize_status(follow_ok)
            if status:
                follow_components = {"overall": status}
            follow_legacy = dict(legacy)
            follow_legacy["role"] = "followup"
            if follow_ok:
                follow_legacy["ok"] = follow_ok
            payloads.append(
                make_payload(
                    serial=serial,
                    mac=mac,
                    technician=follow_tech,
                    model=model,
                    vendor=vendor,
                    category=category,
                    tag=tag,
                    components=follow_components,
                    legacy=follow_legacy,
                )
            )
    return payloads


def parse_segment_two(rows, path, tag, category_map):
    header = rows[0]
    positions = header_positions(header)
    payloads = []
    for idx, row in enumerate(rows[1:], start=2):
        if row_is_blank(row) or looks_like_header(row):
            continue
        serial_raw = resolve_spec(row, positions, "NUM_SERIE")
        serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
        mac = normalize_mac(resolve_spec(row, positions, "NUMERO_MAC"))
        if not serial and not mac:
            continue
        technician = normalize_technician(resolve_spec(row, positions, "TECHNICIEN"))
        model = clean_string(resolve_spec(row, positions, "MODELE"))
        components = None
        overall = normalize_status(resolve_spec(row, positions, ("OK ?", 1)))
        if overall:
            components = {"overall": overall}
        date_value = resolve_spec(row, positions, "DATE RETOUR")
        legacy = {
            "source": "legacy-t490-carrefour",
            "file": os.path.basename(path),
            "rowIndex": idx,
            "row": row_to_dict(header, row),
            "segment": "retour",
        }
        if date_value:
            legacy["date"] = date_value
            legacy["dateParsed"] = parse_date(date_value)
        for key, spec in {
            "problemeClient": "PROBLEME CLIENT",
            "reparationNecessaire": "REPARATION NECESSAIRE ?",
            "reparationFaite": "REPARATION FAITE",
            "piecesAChanger": "PIECES A CHANGER",
            "secondRetour": "2nd RETOUR SAV",
            "apresVerification": "Après vérification",
            "pieceACommander": "Pièce a commander",
        }.items():
            val = resolve_spec(row, positions, spec)
            if val and str(val).strip():
                legacy[key] = val
        category = guess_category(model, os.path.basename(path))
        if category == "unknown":
            if serial and serial in category_map:
                category = category_map[serial]
            elif mac and mac in category_map:
                category = category_map[mac]
        payloads.append(
            make_payload(
                serial=serial,
                mac=mac,
                technician=technician,
                model=model,
                vendor=None,
                category=category,
                tag=tag,
                components=components,
                legacy=legacy,
            )
        )
    return payloads


def main():
    parser = argparse.ArgumentParser(description="Import PROD - T490 Carrefour 07_25.csv")
    parser.add_argument("--csv", default="/home/christopher/tmp/PROD - T490 Carrefour 07_25.csv")
    parser.add_argument("--api-url", default="https://hydra-dev.local/api/ingest")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    rows = read_csv_rows(args.csv)
    if not rows:
        print("[WARN] empty CSV")
        return 0
    split_idx = find_header_row(rows, "DATE RETOUR")
    if split_idx is None:
        split_idx = len(rows)
    segment_one = rows[:split_idx]
    segment_two = rows[split_idx:]

    tag = derive_tag(os.path.basename(args.csv))
    category_map = {}
    if segment_one:
        header = segment_one[0]
        positions = header_positions(header)
        for row in segment_one[1:]:
            if row_is_blank(row) or looks_like_header(row):
                continue
            serial_raw = resolve_spec(row, positions, "NUM_SERIE")
            serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
            mac = normalize_mac(resolve_spec(row, positions, "NUMERO_MAC"))
            if not serial and not mac:
                continue
            model = clean_string(resolve_spec(row, positions, "MODELE"))
            category = guess_category(model, os.path.basename(args.csv))
            if category != "unknown":
                if serial:
                    category_map.setdefault(serial, category)
                if mac:
                    category_map.setdefault(mac, category)
    payloads = []
    if segment_one:
        payloads.extend(parse_segment_one(segment_one, args.csv, tag, category_map))
    if segment_two:
        payloads.extend(parse_segment_two(segment_two, args.csv, tag, category_map))

    ok, fail = submit_payloads(
        payloads,
        api_url=args.api_url,
        insecure=args.insecure,
        dry_run=args.dry_run,
        sleep_ms=args.sleep_ms,
        max_retries=args.max_retries,
        workers=args.workers,
    )
    print(f"[DONE] ok={ok} fail={fail} total={len(payloads)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
