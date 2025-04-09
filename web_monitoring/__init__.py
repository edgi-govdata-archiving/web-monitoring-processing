import logging
import os
from ._version import __version__, __version_tuple__  # noqa: F401

# Satisfy pyflakes
assert __version__
assert __version_tuple__


if os.environ.get('LOG_LEVEL'):
    logging.basicConfig(level=os.environ['LOG_LEVEL'].upper())
