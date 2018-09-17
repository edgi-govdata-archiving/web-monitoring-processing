from collections import defaultdict
from contextlib import contextmanager
import hashlib
import io
import lxml.html
import logging
import requests
import threading
import time
import urllib.parse

from requests.exceptions import ConnectionError
from urllib3.exceptions import (
    ConnectTimeoutError,
    MaxRetryError,
    ReadTimeoutError
)


logger = logging.getLogger(__name__)
_backoff_locks = defaultdict(threading.Lock)


def extract_title(content_bytes, encoding='utf-8'):
    "Return content of <title> tag as string. On failure return empty string."
    content_str = content_bytes.decode(encoding=encoding, errors='ignore')
    # The parser expects a file-like, so we mock one.
    content_as_file = io.StringIO(content_str)
    try:
        title = lxml.html.parse(content_as_file).find(".//title")
    except Exception:
        return ''
    if title is None:
        return ''
    else:
        return title.text


def hash_content(content_bytes):
    "Create a version_hash for the content of a snapshot."
    return hashlib.sha256(content_bytes).hexdigest()


def _should_retry(response):
    return response.status_code == 503 or response.status_code == 504


def retryable_request(method, url, retries=4, backoff=30,
                      should_retry=_should_retry, session=None, **kwargs):
    """
    Make a request with the `requests` library that will be automatically
    retried up to a set number of times.

    Parameters
    ----------
    method : string
        HTTP request method to use
    url : string
        URL to request data from
    retries : int, optional
        Maximum number of retries
    backoff : int or float, optional
        Maximum number of seconds to wait before retrying. After each attempt,
        the wait time is calculated by: `backoff / retries_left`, so the final
        attempt will occur `backoff` seconds after the penultimate attempt.
    should_retry : function, optional
        A callback that receives the HTTP response and returns a boolean
        indicating whether the call should be retried. By default, it retries
        for responses with 503 and 504 status codes (gateway errors).
    session : requests.Session, optional
        A session object to use when making requests.
    **kwargs : dict, optional
        Any additional keyword parameters are passed on to `requests`

    Returns
    -------
    response : requests.Response
        The HTTP response object from `requests`
    """
    internal_session = session or requests.Session()
    retry = False
    retryable_error = None
    try:
        try:
            response = internal_session.request(method, url, **kwargs)
            retry = should_retry(response)
        except (ConnectionError, ConnectTimeoutError, MaxRetryError,
                ReadTimeoutError) as error:
            retryable_error = error
            retry = True

        if retry and retries > 0:
            # Lock all threads requesting the same domain during backoff so
            # that we are actually giving it a real break.
            domain = urllib.parse.urlparse(url).netloc
            if domain:
                with _backoff_locks[domain]:
                    time.sleep(backoff / retries)
            else:
                time.sleep(backoff / retries)

            response = retryable_request(method, url, retries - 1, backoff,
                                         session=internal_session, **kwargs)
        elif retryable_error:
            raise retryable_error
    finally:
        if internal_session is not session:
            internal_session.close()

    return response


_last_call_by_group = defaultdict(int)
_rate_limit_lock = threading.Lock()


@contextmanager
def rate_limited(calls_per_second=2, group='default'):
    """
    A context manager that restricts entries to its body to occur only N times
    per second (N can be a float). The current thread will be put to sleep in
    order to delay calls.

    Parameters
    ----------
    calls_per_second : float or int, optional
        Maximum number of calls into this context allowed per second
    group : string, optional
        Unique name to scope rate limiting. If two contexts have different
        `group` values, their timings will be tracked separately.
    """
    if calls_per_second <= 0:
        yield
    else:
        with _rate_limit_lock:
            last_call = _last_call_by_group[group]
            minimum_wait = 1.0 / calls_per_second
            current_time = time.time()
            if current_time - last_call < minimum_wait:
                time.sleep(minimum_wait - (current_time - last_call))
            _last_call_by_group[group] = time.time()
        yield


class ThreadSafeIterator:
    """
    Wraps an iterator with locks to make reading from it thread-safe.

    Parameters
    ----------
    iterator : iterator
    """
    def __init__(self, iterator):
        self.iterator = iterator
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.iterator)


def queue_iterator(queue, auto_done=True):
    """
    Create an iterator over the items in a :class:`queue.Queue`. The iterator
    will stop if it encounters a `None` item.

    Parameters
    ----------
    queue : queue.Queue
    auto_done : bool, optional
        If true (the default value), the iterator will automatically call
        `queue.task_done()` for each item. If you want to actually make use of
        this queue feature, set it to false so you can manually call
        `queue.task_done()` at the appropriate time.
    """
    while True:
        value = queue.get()
        if auto_done:
            queue.task_done()
        if value:
            yield value
        else:
            return


class DepthCountedContext:
    """
    DepthCountedContext is a mixin or base class for context managers that need
    to be perform special operations only when all nested contexts they might
    be used in have exited.

    Override the `__exit_all__(self, type, value, traceback)` method to get a
    version of `__exit__` that is only called when exiting the top context.

    As a convenience, the built-in `__enter__` returns `self`, which is fairly
    common, so in many cases you don't need to author your own `__enter__` or
    `__exit__` methods.
    """
    _context_depth = 0

    def __enter__(self):
        self._context_depth += 1
        return self

    def __exit__(self, type, value, traceback):
        if self._context_depth > 0:
            self._context_depth -= 1
        if self._context_depth == 0:
            return self.__exit_all__(type, value, traceback)

    def __exit_all__(self, type, value, traceback):
        """
        A version of the normal `__exit__` context manager method that only
        gets called when the top level context is exited. This is meant to be
        overridden in your class.
        """
        pass
