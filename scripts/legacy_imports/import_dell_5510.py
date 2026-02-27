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


def main():
    parser = argparse.ArgumentParser(description="Import PROD - dell 5510.csv")
    parser.add_argument("--csv", default="/home/christopher/tmp/PROD - dell 5510.csv")
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
    header = rows[0]
    positions = header_positions(header)
    tag = derive_tag(os.path.basename(args.csv))
    payloads = []

    for idx, row in enumerate(rows[1:], start=2):
        if row_is_blank(row) or looks_like_header(row):
            continue
        serial_raw = resolve_spec(row, positions, "NUM_SERIE")
        serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
        mac = normalize_mac(resolve_spec(row, positions, ["NUMERO_MAC", "NUM_MAC", " MAC", " NUM_MAC"]))
        if not serial and not mac:
            continue
        technician = normalize_technician(resolve_spec(row, positions, "TECHNICIEN"))
        model = clean_string(resolve_spec(row, positions, "MODELE"))
        vendor = clean_string(resolve_spec(row, positions, "FOURNISSEUR"))
        category = guess_category(model, os.path.basename(args.csv))
        components = {}
        for key, spec in {
            "aesthetic": "ESTHETIQUE",
            "userDiag": "USER-DIAG",
            "camera": "CAMERA",
            "pad": "PAD",
            "audio": "SON",
            "keyboard": "CLAVIER",
            "battery": "BATTERIE (75%)",
            "badgeReader": "LECTEUR BADGE",
            "wifi": "WIFI",
        }.items():
            raw = resolve_spec(row, positions, spec)
            status = normalize_status(raw)
            if status:
                components[key] = status
        overall = normalize_status(resolve_spec(row, positions, ("OK?", 1)))
        if overall:
            components["overall"] = overall
        components = components or None

        problems = []
        for spec in ["PROBLEME", "PROBLEME "]:
            value = resolve_spec(row, positions, spec)
            if value and str(value).strip():
                problems.append(str(value).strip())
        notes = []
        for spec in ["REMARQUE", ("COMMENTAIRE", 1), ("COMMENTAIRE ", 1)]:
            value = resolve_spec(row, positions, spec)
            if value and str(value).strip():
                notes.append(str(value).strip())
        date_value = resolve_spec(row, positions, "DATE_TRAITEMENT")
        legacy = {
            "source": "legacy-dell-5510",
            "file": os.path.basename(args.csv),
            "rowIndex": idx,
            "row": row_to_dict(header, row),
        }
        if date_value:
            legacy["date"] = date_value
            legacy["dateParsed"] = parse_date(date_value)
        if problems:
            legacy["problems"] = problems
        if notes:
            legacy["notes"] = notes
        for key, spec in {
            "dateReception": "DATE_RECEPTION",
            "repair": "REPARATION",
            "repairExtra": "Reparation",
        }.items():
            value = resolve_spec(row, positions, spec)
            if value and str(value).strip():
                legacy[key] = value

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

        # Double check
        double_tech = normalize_technician(resolve_spec(row, positions, "DOUBLE CHECK"))
        if double_tech:
            double_ok_raw = resolve_spec(row, positions, ("OK?", 2))
            double_components = None
            if double_ok_raw:
                ok_status = normalize_status(double_ok_raw)
                if ok_status:
                    double_components = {"overall": ok_status}
            double_legacy = dict(legacy)
            double_legacy["role"] = "double_check"
            if double_ok_raw:
                double_legacy["ok"] = double_ok_raw
            comment = resolve_spec(row, positions, ("COMMENTAIRE", 1))
            if comment and str(comment).strip():
                double_legacy["note"] = comment
            payloads.append(
                make_payload(
                    serial=serial,
                    mac=mac,
                    technician=double_tech,
                    model=model,
                    vendor=vendor,
                    category=category,
                    tag=tag,
                    components=double_components,
                    legacy=double_legacy,
                )
            )

        # Triple check
        triple_tech = normalize_technician(resolve_spec(row, positions, "TRIPLE CHECK"))
        if triple_tech:
            triple_ok_raw = resolve_spec(row, positions, ("OK?", 3))
            triple_components = None
            if triple_ok_raw:
                ok_status = normalize_status(triple_ok_raw)
                if ok_status:
                    triple_components = {"overall": ok_status}
            triple_legacy = dict(legacy)
            triple_legacy["role"] = "triple_check"
            if triple_ok_raw:
                triple_legacy["ok"] = triple_ok_raw
            comment = resolve_spec(row, positions, ("COMMENTAIRE", 2))
            if comment and str(comment).strip():
                triple_legacy["note"] = comment
            payloads.append(
                make_payload(
                    serial=serial,
                    mac=mac,
                    technician=triple_tech,
                    model=model,
                    vendor=vendor,
                    category=category,
                    tag=tag,
                    components=triple_components,
                    legacy=triple_legacy,
                )
            )

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
