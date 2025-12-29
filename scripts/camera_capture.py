import argparse
import json
import os
import sys
import time
import urllib.request
from urllib.error import URLError, HTTPError
from pathlib import Path
from datetime import datetime

import cv2


EXIT_OK = 0
EXIT_CAMERA_FAIL = 1
EXIT_OPEN_FAIL = 2
EXIT_CAPTURE_FAIL = 3
EXIT_SAVE_FAIL = 4


def get_pc_name() -> str:
    name = os.environ.get("COMPUTERNAME") or "PC"
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)


BLACK_PIXEL_THRESHOLD = 20
MAX_BLACK_RATIO = 0.7


def resolve_out_dir(cli_out_dir) -> Path:
    if cli_out_dir:
        return Path(cli_out_dir)
    env_dir = os.environ.get("CAMERA_OUTDIR") or os.environ.get("DIAG_TOOL_OUTDIR")
    if env_dir:
        return Path(env_dir)
    return Path.cwd()


def post_status(
    success: bool,
    pc_name: str,
    timestamp: str,
    details: dict,
    api_base_url: str,
    api_token: str,
    report: bool,
) -> None:
    if not report or not api_base_url or not api_token:
        return
    payload = {
        "action": "camera_capture",
        "actor": pc_name,
        "details": {
            "success": success,
            "timestamp": timestamp,
            **details,
        },
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{api_base_url}/api/audit-log",
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-API-Token": api_token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except (HTTPError, URLError) as exc:
        print(f"Echec d'envoi API: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Camera capture check")
    parser.add_argument("--outdir", default="", help="Output directory for logs/photos")
    parser.add_argument("--index", type=int, default=0, help="Camera index (default 0)")
    parser.add_argument("--report", action="store_true", help="Post status to Hydra audit-log")
    parser.add_argument("--api-url", default=os.environ.get("DIAG_HYDRA_URL", ""), help="Hydra base URL")
    parser.add_argument("--api-token", default=os.environ.get("HYDRA_API_TOKEN", ""), help="Hydra API token")
    args = parser.parse_args()

    pc_name = get_pc_name()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = resolve_out_dir(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print("Demarrage du script camera.")
    print(f"PC detecte: {pc_name}")
    print(f"Ouverture de la camera (index={args.index}).")
    cap = cv2.VideoCapture(args.index, cv2.CAP_DSHOW)
    if not cap.isOpened():
        message = f"{timestamp} - echec ouverture camera (index={args.index})"
        log_path = out_dir / "camera_error.log"
        try:
            with log_path.open("a", encoding="utf-8") as f:
                f.write(message + "\n")
        except OSError as exc:
            print(f"Echec d'ecriture log: {log_path} ({exc})")
        print(f"Impossible d'ouvrir la camera (index={args.index}).")
        post_status(
            success=False,
            pc_name=pc_name,
            timestamp=timestamp,
            details={"error": "open_failed", "message": message, "out_dir": str(out_dir)},
            api_base_url=args.api_url,
            api_token=args.api_token,
            report=args.report,
        )
        sys.exit(EXIT_OPEN_FAIL)

    print("Camera ouverte, autofocus 5 secondes...")
    time.sleep(5)
    ret, frame = cap.read()
    cap.release()

    if not ret or frame is None:
        print("Capture echouee (aucune frame).")
        post_status(
            success=False,
            pc_name=pc_name,
            timestamp=timestamp,
            details={"error": "capture_failed", "out_dir": str(out_dir)},
            api_base_url=args.api_url,
            api_token=args.api_token,
            report=args.report,
        )
        sys.exit(EXIT_CAPTURE_FAIL)
    print("Capture OK, analyse de luminosite...")
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    black_ratio = float((gray <= BLACK_PIXEL_THRESHOLD).mean())
    camera_ok = black_ratio < MAX_BLACK_RATIO
    if not camera_ok:
        print(f"Probleme camera: image trop sombre (noir={black_ratio:.2%}).")
    else:
        print(f"Luminosite OK (noir={black_ratio:.2%}).")

    filename = f"{pc_name}.{timestamp}.jpg"
    target = out_dir / filename
    print("Enregistrement de la photo...")

    saved_any = False
    saved_paths = []
    try:
        ok = cv2.imwrite(str(target), frame)
        if ok:
            print(f"Photo enregistree ici: {target}")
            if not camera_ok:
                issue_path = target.with_suffix(".problem.txt")
                message = (
                    "Camera en probleme (image trop sombre).\n"
                    f"pc_name={pc_name}\n"
                    f"timestamp={timestamp}\n"
                    f"black_ratio={black_ratio:.4f}\n"
                    f"photo={target}\n"
                )
                try:
                    with issue_path.open("w", encoding="utf-8") as f:
                        f.write(message)
                    print(f"Fichier probleme cree: {issue_path}")
                except OSError as exc:
                    print(f"Echec d'ecriture probleme: {issue_path} ({exc})")
            saved_any = True
            saved_paths.append(str(target))
        else:
            print(f"Echec d'ecriture: {target}")
    except OSError as exc:
        print(f"Echec d'ecriture: {target} ({exc})")

    if not saved_any:
        print("Aucune photo n'a pu etre enregistree.")
        post_status(
            success=False,
            pc_name=pc_name,
            timestamp=timestamp,
            details={"error": "save_failed", "out_dir": str(out_dir)},
            api_base_url=args.api_url,
            api_token=args.api_token,
            report=args.report,
        )
        sys.exit(EXIT_SAVE_FAIL)
    else:
        if args.report and args.api_url and args.api_token:
            print("Envoi du statut a l'API...")
        post_status(
            success=camera_ok,
            pc_name=pc_name,
            timestamp=timestamp,
            details={
                "paths": saved_paths,
                "black_ratio": round(black_ratio, 4),
                "camera_ok": camera_ok,
                "out_dir": str(out_dir),
                **({"error": "image_too_dark"} if not camera_ok else {}),
            },
            api_base_url=args.api_url,
            api_token=args.api_token,
            report=args.report,
        )
        sys.exit(EXIT_OK if camera_ok else EXIT_CAMERA_FAIL)


if __name__ == "__main__":
    main()
