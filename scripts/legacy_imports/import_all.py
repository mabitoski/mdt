import argparse
import subprocess
import sys

SCRIPTS = [
    "import_amso_checkup.py",
    "import_sav_amso.py",
    "import_dell_3510.py",
    "import_dell_5510.py",
    "import_digo.py",
    "import_fujitsu_esprimo.py",
    "import_hp_prodesk_600_g4_mt.py",
    "import_optiplex_7070.py",
    "import_t490_carrefour_07_25.py",
    "import_thinkcentre_m720q.py",
    "import_thinkpad_l13_l390_t490_dell_5410.py",
]


def main():
    parser = argparse.ArgumentParser(description="Run all legacy CSV imports")
    parser.add_argument("--api-url", default="https://hydra-dev.local/api/ingest")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    base = __file__.rsplit("/", 1)[0]
    for script in SCRIPTS:
        cmd = [
            sys.executable,
            f"{base}/{script}",
            "--api-url",
            args.api_url,
            "--sleep-ms",
            str(args.sleep_ms),
            "--max-retries",
            str(args.max_retries),
            "--workers",
            str(args.workers),
        ]
        if args.insecure:
            cmd.append("--insecure")
        if args.dry_run:
            cmd.append("--dry-run")
        print(f"[RUN] {script}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"[WARN] {script} failed with {result.returncode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
