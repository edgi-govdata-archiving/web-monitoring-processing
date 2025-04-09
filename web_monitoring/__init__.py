import logging
import os
from ._version import __version__, __version_tuple__ # noqa: F401


if os.environ.get('LOG_LEVEL'):
    logging.basicConfig(level=os.environ['LOG_LEVEL'].upper())
