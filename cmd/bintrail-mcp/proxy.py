#!/usr/bin/env python3
"""
bintrail MCP proxy — bridges Claude Desktop's MCP stdio protocol to a remote
bintrail-mcp HTTP server (started with: bintrail-mcp --http :8080).

Usage — add to Claude Desktop's mcpServers config
(~/Library/Application Support/Claude/claude_desktop_config.json):

  "bintrail": {
    "command": "python3",
    "args": ["/path/to/proxy.py"],
    "env": { "BINTRAIL_SERVER": "http://192.168.1.37:8080/mcp" }
  }

Requires: Python 3.7+, no third-party packages.
"""

import json
import os
import sys
import threading
import urllib.request
import urllib.error

SERVER_URL = os.environ.get("BINTRAIL_SERVER", "http://localhost:8080/mcp")

_session_id = None  # type: str
_lock = threading.Lock()


def _get_session():
    with _lock:
        return _session_id


def _set_session(sid):
    global _session_id
    with _lock:
        _session_id = sid


def _emit(line):
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _error(msg_id, message):
    # Notifications have no id and expect no response — drop silently.
    if msg_id is None:
        return
    _emit(json.dumps({
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32000, "message": message},
    }))


def _post(body: bytes) -> None:
    """Send one JSON-RPC message to the HTTP server; forward all responses to stdout."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    sid = _get_session()
    if sid:
        headers["Mcp-Session-Id"] = sid

    req = urllib.request.Request(SERVER_URL, data=body, headers=headers, method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        try:
            msg_id = json.loads(body).get("id")
        except Exception:
            msg_id = None
        _error(msg_id, f"HTTP {e.code}: {e.reason}")
        return
    except Exception as e:
        _error(None, str(e))
        return

    with resp:
        new_sid = resp.headers.get("Mcp-Session-Id")
        if new_sid:
            _set_session(new_sid)

        ct = resp.headers.get("Content-Type", "")
        if "text/event-stream" in ct:
            # Server responded with an SSE stream — forward every data line.
            buf = b""
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    idx = buf.index(b"\n")
                    raw = buf[:idx].decode("utf-8").rstrip("\r")
                    buf = buf[idx + 1:]
                    if raw.startswith("data:"):
                        data = raw[5:].strip()
                        if data and data != "[DONE]":
                            _emit(data)
        else:
            data = resp.read().decode("utf-8").strip()
            if data:
                _emit(data)


def main() -> None:
    for raw in sys.stdin:
        line = raw.strip()
        if line:
            _post(line.encode("utf-8"))


if __name__ == "__main__":
    main()
