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


def build_components(row, positions, mapping):
    components = {}
    for key, spec in mapping.items():
        raw = resolve_spec(row, positions, spec)
        status = normalize_status(raw)
        if status:
            components[key] = status
    return components or None


def build_overall_component(row, positions, spec):
    raw = resolve_spec(row, positions, spec)
    status = normalize_status(raw)
    if status:
        return {"overall": status}
    return None


def build_payloads(path, config):
    rows = read_csv_rows(path)
    if not rows:
        return []
    header = rows[0]
    positions = header_positions(header)
    tag = config.get("tag") or derive_tag(path)
    payloads = []
    category_map = config.get("category_map") or {}

    for row in rows[1:]:
        if row_is_blank(row) or looks_like_header(row):
            continue
        serial_raw = resolve_spec(row, positions, config.get("serial"))
        serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
        mac = normalize_mac(resolve_spec(row, positions, config.get("mac")))
        if not serial and not mac:
            continue
        model = clean_string(resolve_spec(row, positions, config.get("model")))
        category = guess_category(model, os.path.basename(path))
        if category != "unknown":
            if serial:
                category_map.setdefault(serial, category)
            if mac:
                category_map.setdefault(mac, category)

    for idx, row in enumerate(rows[1:], start=2):
        if row_is_blank(row) or looks_like_header(row):
            continue
        serial_raw = resolve_spec(row, positions, config.get("serial"))
        serial = normalize_serial(serial_raw) if is_serial_candidate(serial_raw) else None
        mac = normalize_mac(resolve_spec(row, positions, config.get("mac")))
        if not serial and not mac:
            continue
        technician = normalize_technician(resolve_spec(row, positions, config.get("technician")))
        model = clean_string(resolve_spec(row, positions, config.get("model")))
        vendor = clean_string(resolve_spec(row, positions, config.get("vendor")))
        category = guess_category(model, os.path.basename(path))
        if category == "unknown":
            if serial and serial in category_map:
                category = category_map[serial]
            elif mac and mac in category_map:
                category = category_map[mac]
        components = build_components(row, positions, config.get("components", {}))
        overall_component = build_overall_component(row, positions, config.get("overall"))
        if overall_component:
            if components:
                components.update(overall_component)
            else:
                components = overall_component
        problems = []
        for spec in config.get("problems", []):
            value = resolve_spec(row, positions, spec)
            if value and str(value).strip():
                problems.append(str(value).strip())
        legacy_notes = []
        for spec in config.get("notes", []):
            value = resolve_spec(row, positions, spec)
            if value and str(value).strip():
                legacy_notes.append(str(value).strip())
        date_value = resolve_spec(row, positions, config.get("date"))
        legacy = {
            "source": config.get("source", "legacy-csv"),
            "file": os.path.basename(path),
            "rowIndex": idx,
            "row": row_to_dict(header, row),
        }
        if date_value:
            legacy["date"] = date_value
            legacy["dateParsed"] = parse_date(date_value)
        if problems:
            legacy["problems"] = problems
        if legacy_notes:
            legacy["notes"] = legacy_notes
        if config.get("extra"):
            for key, spec in config["extra"].items():
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

        for followup in config.get("followups", []):
            tech_value = resolve_spec(row, positions, followup.get("technician"))
            follow_tech = normalize_technician(tech_value)
            if not follow_tech:
                continue
            follow_components = None
            mode = followup.get("components", "overall")
            ok_spec = followup.get("overall")
            if mode == "copy":
                follow_components = dict(components) if components else None
            elif mode == "overall":
                follow_components = build_overall_component(row, positions, ok_spec)
            elif mode == "none":
                follow_components = None
            follow_legacy = dict(legacy)
            follow_legacy["role"] = followup.get("role", "followup")
            if ok_spec:
                ok_raw = resolve_spec(row, positions, ok_spec)
                if ok_raw and str(ok_raw).strip():
                    follow_legacy["ok"] = ok_raw
            note_spec = followup.get("note")
            if note_spec:
                note_val = resolve_spec(row, positions, note_spec)
                if note_val and str(note_val).strip():
                    follow_legacy["note"] = note_val
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


def main():
    parser = argparse.ArgumentParser(description="Generic legacy CSV importer")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--api-url", default="https://hydra-dev.local/api/ingest")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    print("[ERROR] generic_import.py should be called by a per-CSV wrapper.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
