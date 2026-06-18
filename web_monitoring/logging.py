import logging
import os


def configure_logging():
    # Keep config simple by using basicConfig for the root logger and adding a
    # special level for *our* logs. In the future we may need something more
    # complex like dictConfig.
    level = (os.environ.get('LOG_LEVEL') or 'WARNING').upper()
    root_level = level if level in ('WARNING', 'ERROR', 'CRITICAL') else 'WARNING'

    logging.basicConfig(
        level=root_level,
        style='{',
        format='{asctime} {levelname} [{name}] {message}',
    )
    logging.getLogger('web_monitoring').setLevel(level)
