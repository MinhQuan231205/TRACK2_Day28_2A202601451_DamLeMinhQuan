"""Redact the ephemeral vLLM tunnel host from evidence before it is committed.

The Kaggle T4 endpoint is reached over a public tunnel whose hostname is a
short-lived secret (``vllm.env``). Prometheus records it as a scrape URL, so a
freshly generated ``ip09-prometheus-targets.json`` would carry it into git.
This script replaces the host — read from ``LAB28_VLLM_BASE_URL`` or the
generated ``monitoring/targets/vllm.yml`` — with a stable placeholder wherever
it appears under ``evidence/``.
"""

from __future__ import annotations

import os
import re
import sys
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
PLACEHOLDER = "vllm-endpoint.redacted"


def _tunnel_host() -> str | None:
    base = os.environ.get("LAB28_VLLM_BASE_URL")
    if base:
        host = urllib.parse.urlparse(base).hostname
        if host and host not in {"localhost", "host.docker.internal", "127.0.0.1"}:
            return host
    target = ROOT / "monitoring" / "targets" / "vllm.yml"
    if target.is_file():
        match = re.search(r'([\w-]+\.trycloudflare\.com|[\w.-]+\.ngrok[\w.-]*)', target.read_text())
        if match:
            return match.group(1)
    return None


def main() -> int:
    host = _tunnel_host()
    if not host:
        print("no tunnel host to scrub (local endpoint or nothing configured)")
        return 0

    touched = 0
    for path in sorted(EVIDENCE.glob("*.json")):
        text = path.read_text(encoding="utf-8")
        if host in text:
            path.write_text(text.replace(host, PLACEHOLDER), encoding="utf-8")
            touched += 1
            print(f"scrubbed {path.name}")
    print(f"done: {touched} file(s) redacted, host hidden behind {PLACEHOLDER!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
