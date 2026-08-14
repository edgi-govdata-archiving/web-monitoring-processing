import logging
import os
import sentry_sdk
from sentry_sdk.integrations.logging import ignore_logger as sentry_ignore_logger
from typing import Sequence


def configure_logging():
    # Keep config simple by using basicConfig for the root logger and adding a
    # special level for *our* logs. In the future we may need something more
    # complex like dictConfig.
    level = (os.getenv('LOG_LEVEL') or 'WARNING').upper()
    root_level = level if level in ('WARNING', 'ERROR', 'CRITICAL') else 'WARNING'

    logging.basicConfig(
        level=root_level,
        style='{',
        format='{asctime} {levelname} [{name}] {message}',
    )
    logging.getLogger('web_monitoring').setLevel(level)


def configure_sentry(ignore_loggers: Sequence[str] = (), **options) -> None:
    environment = os.getenv('SENTRY_ENVIRONMENT')
    traces_rate = float(os.getenv('SENTRY_TRACES_SAMPLE_RATE') or (
        '1.0' if environment == 'development' else '0.5'
    ))

    sentry_sdk.init(traces_sample_rate=traces_rate, **options)
    for name in ignore_loggers:
        sentry_ignore_logger(name)
