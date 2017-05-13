from datetime import datetime
import re
import requests



class WebMonitoringException(Exception):
    # All exceptions raised directly by this package inherit from this.
    ...


class UnexpectedResponseFormat(WebMonitoringException):
    ...


DATE_FMT = '%a, %d %b %Y %H:%M:%S %Z'
URL_CHUNK_PATTERN = re.compile('\<(.*)\>')
DATETIME_CHUNK_PATTERN = re.compile(' datetime="(.*)",')


def list_versions(url):
    """
    Yield (version_datetime, version_uri) for all versions of a url.

    Parameters
    ----------
    url : string

    Examples
    --------

    Grab the datetime and URL of the version nasa.gov snapshot.
    >>> pairs = list_versions('nasa.gov'):
    >>> dt, url = next(pairs)
    >>> dt
    datetime.datetime(1996, 12, 31, 23, 58, 47)
    >>> url
    'http://web.archive.org/web/19961231235847/http://www.nasa.gov:80/'

    Loop through all the snapshots.
    >>> for dt, url in list_versions('nasa.gov'):
    ...     # do something
    """
    # Get a list of all the 'Versions' (timestamps when the url was captured).
    res = requests.get('http://web.archive.org/web/timemap/link/{}'.format(url))
    content = res.iter_lines()

    # The first three lines contain no information we need.
    for _ in range(3):
        next(content)
    for line in content:
        # Lines are made up semicolon-separated chunks:
        # b'<http://web.archive.org/web/19961231235847/http://www.nasa.gov:80/>; rel="memento"; datetime="Tue, 31 Dec 1996 23:58:47 GMT",'

        # Split by semicolon. Fail with an informative error if there are not
        # exactly three chunks.
        try:
            url_chunk, rel_chunk, dt_chunk = line.decode().split(';')
        except ValueError:
            raise UnexpectedResponseFormat(line.decode())

        # Extract the URL and the datetime from the surrounding characters.
        # Again, fail with an informative error.
        try:
            version_uri, = URL_CHUNK_PATTERN.match(url_chunk).groups()
            version_dt_str, = DATETIME_CHUNK_PATTERN.match(dt_chunk).groups()
        except AttributeError:
            raise UnexpectedResponseFormat(line.decode())

        version_dt = datetime.strptime(version_dt_str, DATE_FMT)
        yield version_dt, version_uri
