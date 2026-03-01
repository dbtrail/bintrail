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

Requires: Python 3.7+, no third-party packages. This file is fully
self-contained — copy it alone to the remote machine.

Set BINTRAIL_LOG_LEVEL=DEBUG to enable diagnostic output on stderr.
"""

import json
import logging
import os
import sys
import threading
import urllib.request
import urllib.error


def _setup_logging():
    # type: () -> logging.Logger
    level_name = os.environ.get("BINTRAIL_LOG_LEVEL", "WARNING").upper()
    level = getattr(logging, level_name, logging.WARNING)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    logger = logging.getLogger("bintrail-proxy")
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


_log = _setup_logging()

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
        _log.debug("dropped error for notification (no id): %s", message)
        return
    _emit(json.dumps({
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32000, "message": message},
    }))


def _post(body):
    # type: (bytes) -> None
    """Send one JSON-RPC message to the HTTP server; forward all responses to stdout."""
    try:
        parsed = json.loads(body)
        method = parsed.get("method", "?")
        msg_id = parsed.get("id")
    except Exception:
        method = "?"
        msg_id = None

    _log.debug("-> %s (id=%s)", method, msg_id)

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
        _log.warning("HTTP %s %s for method %s", e.code, e.reason, method)
        _error(msg_id, "HTTP {}: {}".format(e.code, e.reason))
        return
    except Exception as e:
        _log.error("request failed for method %s: %s", method, e)
        _error(msg_id, str(e))
        return

    with resp:
        new_sid = resp.headers.get("Mcp-Session-Id")
        if new_sid and new_sid != sid:
            _set_session(new_sid)
            _log.debug("session acquired: %s", new_sid)

        ct = resp.headers.get("Content-Type", "")
        _log.debug("<- %s content-type=%s", resp.status, ct)

        if "text/event-stream" in ct:
            # Server responded with an SSE stream — forward every data line.
            buf = b""
            forwarded = 0
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    idx = buf.index(b"\n")
                    try:
                        raw = buf[:idx].decode("utf-8").rstrip("\r")
                    except UnicodeDecodeError:
                        _log.warning("skipping non-UTF-8 SSE line")
                        buf = buf[idx + 1:]
                        continue
                    buf = buf[idx + 1:]
                    if raw.startswith("data:"):
                        data = raw[5:].strip()
                        if data and data != "[DONE]":
                            _emit(data)
                            forwarded += 1
            _log.debug("forwarded %d SSE event(s) for method %s", forwarded, method)
        else:
            data = resp.read().decode("utf-8").strip()
            if data:
                _emit(data)


def main():
    # type: () -> None
    _log.info("bintrail proxy starting — server=%s python=%s", SERVER_URL, sys.version.split()[0])
    for raw in sys.stdin:
        line = raw.strip()
        if line:
            _post(line.encode("utf-8"))


if __name__ == "__main__":
    main()
