import logging
import os
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


if os.environ.get('LOG_LEVEL'):
    logging.basicConfig(level=os.environ['LOG_LEVEL'].upper())
    # Urllib3 logs a warning every time a retry happens. Way too noisy.
    # TODO: is this the best way to configure? We should probably revisit this.
    # Maybe the right answer is filtering when *reading* the output.
    subcomponent_level = os.environ.get('LOG_LEVEL_SUBCOMPONENT', 'ERROR')
    logging.getLogger('urllib3.connectionpool').setLevel(subcomponent_level)
