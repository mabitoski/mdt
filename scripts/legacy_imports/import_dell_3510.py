import argparse
import os

from common import derive_tag, submit_payloads
from generic_import import build_payloads

CONFIG = {
    "source": "legacy-dell-3510",
    "serial": "NUM_SERIE",
    "mac": ["NUMERO_MAC", "NUM_MAC", "MAC"],
    "technician": ("TECHNICIEN", 1),
    "model": "MODELE",
    "vendor": "FOURNISSEUR",
    "date": "DATE_TRAITEMENT",
    "components": {
        "aesthetic": "ESTHETIQUE",
        "userDiag": "USER_DIAG",
        "camera": "CAMERA",
        "pad": "PAD",
        "touchscreen": "TACTILE",
        "audio": "SON",
        "keyboard": "CLAVIER",
        "usb": "POR USB",
        "battery": "BATTERIE",
    },
    "overall": ("OK ?", 1),
    "problems": ["PROBLEME(S)"],
    "notes": ["PIECE A COMMANDER"],
    "extra": {
        "dateReception": "DATE_RECEPTION",
    },
    "followups": [
        {
            "role": "repair",
            "technician": "REPARATEUR",
            "overall": "OK APRES REPARATION ?",
            "note": "PIECE A COMMANDER",
        },
        {
            "role": "double_check",
            "technician": "DOUBLE CHECK",
            "overall": ("OK ?", 2),
        },
    ],
}


def main():
    parser = argparse.ArgumentParser(description="Import PROD - DELL 3510.csv")
    parser.add_argument(
        "--csv",
        default="/home/christopher/tmp/PROD - DELL 3510.csv",
    )
    parser.add_argument("--api-url", default="https://hydra-dev.local/api/ingest")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    CONFIG["tag"] = derive_tag(os.path.basename(args.csv))
    payloads = build_payloads(args.csv, CONFIG)
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


if __name__ == "__main__":
    main()
