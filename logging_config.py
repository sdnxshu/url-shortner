"""
logging_config.py — configure structlog for the whole app.

Call `setup_logging()` once at startup. After that, every module gets
a structured logger via:

    import structlog
    logger = structlog.get_logger()

Every log line is emitted as JSON in production (LOG_FORMAT=json, the default)
or as a colourised human-friendly string in development (LOG_FORMAT=console).

A `request_id` is injected per-request by the RequestTracingMiddleware in
main.py and bound into the context-local logger automatically.
"""

import logging
import os
import sys

import structlog

LOG_LEVEL  = os.getenv("LOG_LEVEL",  "INFO").upper()
LOG_FORMAT = os.getenv("LOG_FORMAT", "json")   # "json" | "console"


def setup_logging() -> None:
    # ------------------------------------------------------------------ #
    # 1. Shared processors that run on every log record                   #
    # ------------------------------------------------------------------ #
    shared_processors: list = [
        structlog.contextvars.merge_contextvars,          # picks up request_id etc.
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # ------------------------------------------------------------------ #
    # 2. Final renderer — JSON in prod, pretty in dev                     #
    # ------------------------------------------------------------------ #
    if LOG_FORMAT == "console":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    # ------------------------------------------------------------------ #
    # 3. Wire structlog to stdlib so third-party libs (uvicorn, sqlalchemy)
    #    are also captured and formatted consistently                      #
    # ------------------------------------------------------------------ #
    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(LOG_LEVEL)

    # Quieten noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
