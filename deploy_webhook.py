#!/usr/bin/env python3
import http.server
import json
import os
import subprocess
import sys
import threading

SECRET_PATH = "/etc/mdt-fusion/main/webhook_secret"
WORKDIR = "/etc/mdt-fusion/main"
PORT = 9002
BRANCH = "main"

secret = os.environ.get("WEBHOOK_SECRET")
if not secret:
    try:
        secret = open(SECRET_PATH, "r", encoding="utf-8").read().strip()
    except FileNotFoundError:
        print("[error] secret file missing", file=sys.stderr)
        sys.exit(1)

COMPOSE_BASE = [
    "docker",
    "compose",
    "--project-directory",
    WORKDIR,
    "-f",
    f"{WORKDIR}/docker-compose.yml",
]

COMMANDS = [
    ["git", "-C", WORKDIR, "fetch", "--all", "--prune"],
    ["git", "-C", WORKDIR, "checkout", "-B", BRANCH, f"origin/{BRANCH}"],
    ["git", "-C", WORKDIR, "pull", "--rebase", "--autostash", "origin", BRANCH],
    COMPOSE_BASE + ["up", "-d", "--build"],
    COMPOSE_BASE + ["restart", "mdt-web"],
    ["docker", "builder", "prune", "-af", "--filter", "until=168h"],
    ["docker", "image", "prune", "-af", "--filter", "until=168h"],
]

RUN_LOCK = threading.Lock()


class Handler(http.server.BaseHTTPRequestHandler):
    def _respond(self, code: int, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != "/hook":
            self._respond(404, {"error": "not found"})
            return
        token = self.headers.get("X-Gitlab-Token", "")
        if token != secret:
            self._respond(403, {"error": "forbidden"})
            return
        if not RUN_LOCK.acquire(blocking=False):
            self._respond(409, {"error": "deploy_in_progress"})
            return
        results = []
        try:
            for cmd in COMMANDS:
                try:
                    proc = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        check=True,
                        cwd=WORKDIR,
                    )
                    results.append(
                        {
                            "cmd": cmd,
                            "stdout": proc.stdout,
                            "stderr": proc.stderr,
                            "returncode": proc.returncode,
                        }
                    )
                except subprocess.CalledProcessError as exc:
                    self._respond(
                        500,
                        {
                            "error": "command failed",
                            "cmd": cmd,
                            "stdout": exc.stdout,
                            "stderr": exc.stderr,
                            "returncode": exc.returncode,
                        },
                    )
                    return
            self._respond(200, {"status": "ok", "results": results})
        finally:
            RUN_LOCK.release()

    def log_message(self, format, *args):
        sys.stdout.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), format % args)
        )
        sys.stdout.flush()


if __name__ == "__main__":
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[info] Webhook server listening on :{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
