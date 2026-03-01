"""
Logging setup for bintrail MCP proxy.
Writes to stderr so stdout stays clean for the MCP protocol channel.

Level controlled by BINTRAIL_LOG_LEVEL env var (default: WARNING).
Valid values: DEBUG, INFO, WARNING, ERROR
"""

import logging
import os
import sys


def setup_logging():
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
