from cloudpathlib import S3Client, S3Path
import codecs
from datetime import datetime, timedelta, timezone
import dateutil.parser
import email.utils
import gzip
import hashlib
import io
import logging
import lxml.html
import multiprocessing
import os
from pypdf import PdfReader
from pypdf.errors import PyPdfError
import queue
import re
import sentry_sdk
import signal
import sys
import threading
import time
from typing import Any, Generator, Iterable, TypeVar
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit

try:
    from cchardet import detect as detect_charset
except ImportError:
    from charset_normalizer import detect as detect_charset


logger = logging.getLogger(__name__)

WHITESPACE_PATTERN = re.compile(r'\s+')

# Matches a <meta> tag in HTML used to specify the character encoding:
# <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
# <meta charset="utf-8" />
META_TAG_PATTERN = re.compile(
    b'<meta[^>]+charset\\s*=\\s*[\'"]?([^>]*?)[ /;\'">]',
    re.IGNORECASE)

# Matches an XML prolog that specifies character encoding:
# <?xml version="1.0" encoding="ISO-8859-1"?>
XML_PROLOG_PATTERN = re.compile(
    b'<?xml\\s[^>]*encoding=[\'"]([^\'"]+)[\'"].*\\?>',
    re.IGNORECASE)


def detect_encoding(content, headers, default='utf-8'):
    """
    Detect string encoding the same way browsers detect it. This will always
    return an encoding name unless you explicitly set ``default=None``.

    Parameters
    ----------
    content : bytes
    headers : dict
    default : str or None

    Returns
    -------
    str
        The name of a character encoding that is most likely to correctly
        decode ``content`` to a valid string.
    """
    encoding = None

    # Check for declarations in content.
    meta_tag_match = META_TAG_PATTERN.search(content, endpos=2048)
    if meta_tag_match:
        encoding = meta_tag_match.group(1).decode('ascii', errors='ignore').strip()
    if not encoding:
        prolog_match = XML_PROLOG_PATTERN.search(content, endpos=2048)
        if prolog_match:
            encoding = prolog_match.group(1).decode('ascii', errors='ignore').strip()

    # Fall back to headers.
    content_type = headers.get('Content-Type', '').lower()
    if not encoding:
        if 'charset=' in content_type:
            encoding = content_type.split('charset=', 1)[-1].split(';')[0].strip(' "\'')

    # Make an educated guess.
    if not encoding:
        # Try to identify encoding. Use up to 18kb of the content, since it
        # could be huge otherwise (this should be enough for accuracy).
        detected = detect_charset(content[:18432])
        if detected:
            detected_encoding = detected.get('encoding')
            if detected_encoding:
                encoding = detected_encoding.lower()

    # Handle common mistakes and errors in encoding names
    if encoding == 'iso-8559-1':
        encoding = 'iso-8859-1'
    # Windows-1252 is so commonly mislabeled, WHATWG recommends assuming it's a
    # mistake: https://encoding.spec.whatwg.org/#names-and-labels
    if encoding == 'iso-8859-1' and 'html' in content_type:
        encoding = 'windows-1252'

    # Check if the selected encoding is known. If not, fall back to default.
    try:
        codecs.lookup(encoding)
    except (LookupError, ValueError, TypeError):
        encoding = default
    return encoding


def extract_html_title(content_bytes, encoding='utf-8'):
    "Return content of <title> tag as string. On failure return empty string."
    content_str = content_bytes.decode(encoding=encoding, errors='ignore')
    # The parser expects a file-like, so we mock one.
    content_as_file = io.StringIO(content_str)
    try:
        title = lxml.html.parse(content_as_file).find(".//title")
    except Exception:
        return ''

    if title is None or title.text is None:
        return ''

    # In HTML, all consecutive whitespace (including line breaks) collapses
    return WHITESPACE_PATTERN.sub(' ', title.text.strip())


def extract_pdf_title(content_bytes, password=''):
    """
    Get the title of a PDF document. If the document cannot be successfully
    opened and read, this will return `None`.

    Parameters
    ----------
    content_bytes : bytes
        The content of PDF file to read as bytes.
    password : str, optional
        Password to decrypt the PDF with, if it's encrypted. By default, this
        the empty string -- that's useful since a lot of PDFs out there are
        encrypted with an empty password.

    Returns
    -------
    str or None
    """
    try:
        pdf = PdfReader(io.BytesIO(content_bytes))
        # Lots of PDFs turn out to be encrypted with an empty password, so this
        # is always worth trying (most PDF viewers turn out to do this, too).
        # This gets its own inner `try` block (that catches all exceptions)
        # because there are a huge variety of error types that happen inside
        # the `decrypt` call, even with valid PDFs. :(
        if pdf.is_encrypted:
            try:
                pdf.decrypt(password)
            except Exception:
                return None

        return pdf.metadata.title if pdf.metadata else None
    except PyPdfError:
        return None
    except Exception as error:
        logger.exception(error)
        return None


def hash_content(content_bytes):
    "Create a version_hash for the content of a snapshot."
    return hashlib.sha256(content_bytes).hexdigest()


def normalize_netloc(url: SplitResult) -> str:
    """
    Get a parsed URL's netloc in a normalized form.
    """
    assert url.hostname

    result = ''
    if url.username:
        result += url.username
        if url.password:
            result += ':' + url.password
        result += '@'
    result += url.hostname.lower()
    if (
        url.port
        and not (url.scheme == 'https' and url.port == 443)
        and not (url.scheme == 'http' and url.port == 80)
    ):
        result += f':{url.port}'

    return result


def matchable_querystring(querystring: str) -> str:
    """
    Produce a generally matchable version of a URL's querystring. This may make
    alterations that are not strictly equivalent.

    These are mainly meant to make sure changes that Browsertrix makes to seed
    URLs are still matchable, even though they are not strictly correct.
    """
    parsed = parse_qsl(querystring, keep_blank_values=True)
    # TODO: consider bringing in some more ignorable params from our custom
    # SURT implementation in web-monitoring-db.
    parsed = [(k, v) for k, v in parsed if not k.lower().startswith('utm_')]
    result = urlencode(sorted(parsed))
    if '=' not in querystring:
        result = re.sub(r'=', '', result)
    return result


def normalize_url(url: str) -> str:
    """
    Normalize a URL into an unambiguous, standardized form. The output of this
    should always be handled by a server or HTTP library exactly the same as
    the input would have been.
    """
    parsed = urlsplit(url)
    return parsed._replace(
        netloc=normalize_netloc(parsed),
        path=(parsed.path or '/'),
        fragment=''
    ).geturl()


def matchable_url(url: str) -> str:
    """
    Normalize a URL into a generally matchable format that will equate similar
    URLs even after non-standard normalization (e.g. sorting query params).
    This is mainly meant to help with matching Browsertrix outputs, which do
    not necessarily record seed URLs as originally input.
    """
    # TODO: Consider whether any of these should do looser normalizations. We
    # already got caught out by Browsertrix sorting the querystring, and may
    # want to make sure we stay more aggressive than they are (or even go
    # full-bore and use SURT here).
    parsed = urlsplit(url)
    return parsed._replace(
        netloc=normalize_netloc(parsed),
        path=re.sub(r'//+', '/', (parsed.path or '/').rstrip('/')),
        query=matchable_querystring(parsed.query),
        fragment=''
    ).geturl()


class RateLimit:
    """
    RateLimit is a simple locking mechanism that can be used to enforce rate
    limits and is safe to use across multiple threads. It can also be used as
    a context manager.

    Calling `rate_limit_instance.wait()` blocks until a minimum time has passed
    since the last call. Using `with rate_limit_instance:` blocks entries to
    the context until a minimum time since the last context entry.

    Parameters
    ----------
    per_second : int or float
        The maximum number of calls per second that are allowed. If 0, a call
        to `wait()` will never block.

    Examples
    --------
    Slow down a tight loop to only occur twice per second:

    >>> limit = RateLimit(per_second=2)
    >>> for x in range(10):
    >>>     with limit:
    >>>         print(x)
    """
    def __init__(self, per_second=10):
        self._lock = threading.RLock()
        self._last_call_time = 0
        if per_second <= 0:
            self._minimum_wait = 0
        else:
            self._minimum_wait = 1.0 / per_second

    def wait(self):
        if self._minimum_wait == 0:
            return

        with self._lock:
            current_time = time.time()
            idle_time = current_time - self._last_call_time
            if idle_time < self._minimum_wait:
                time.sleep(self._minimum_wait - idle_time)

            self._last_call_time = time.time()

    def __enter__(self):
        self.wait()

    def __exit__(self, type, value, traceback):
        pass


def iterate_into_queue(queue, iterable):
    """
    Read items from an iterable and place them onto a FiniteQueue.

    Parameters
    ----------
    queue: FiniteQueue
    iterable: sequence
    """
    for item in iterable:
        queue.put(item)
    queue.end()


class FiniteQueue(queue.SimpleQueue):
    """
    A queue that is iterable, with a defined end.

    The end of the queue is indicated by the `FiniteQueue.QUEUE_END` object.
    If you are using the iterator interface, you won't ever encounter it, but
    if reading the queue with `queue.get`, you will receive
    `FiniteQueue.QUEUE_END` if you’ve reached the end.
    """

    # Use a class instad of `object()` for more readable names for debugging.
    class QUEUE_END:
        ...

    def __init__(self):
        super().__init__()
        self._ended = False
        # The Queue documentation suggests that put/get calls can be
        # re-entrant, so we need to use RLock here.
        self._lock = threading.RLock()

    def end(self):
        self.put(self.QUEUE_END)

    def get(self, *args, **kwargs):
        with self._lock:
            if self._ended:
                return self.QUEUE_END
            else:
                value = super().get(*args, **kwargs)
                if value is self.QUEUE_END:
                    self._ended = True

                return value

    def __iter__(self):
        return self

    def __next__(self, timeout=None):
        item = self.get()
        if item is self.QUEUE_END:
            raise StopIteration

        return item

    def iterate_with_timeout(self, timeout):
        while True:
            try:
                yield self.__next__(timeout)
            except StopIteration:
                return


class Signal:
    """
    A context manager to handle signals from the system safely. It keeps track
    of previous signal handlers and ensures that they are put back into place
    when the context exits.

    Parameters
    ----------
    signals : int or tuple of int
        The signal or list of signals to handle.
    handler : callable
        A signal handler function of the same type used with `signal.signal()`.
        See: https://docs.python.org/3.6/library/signal.html#signal.signal

    Examples
    --------
    Ignore SIGINT (ctrl+c) and print a glib message instead of quitting:

    >>> def ignore_signal(signal_type, frame):
    >>>     print("Sorry, but you can't quit this program that way!")
    >>>
    >>> with Signal((signal.SIGINT, signal.SIGTERM), ignore_signal):
    >>>     do_some_work_that_cant_be_interrupted()
    """
    def __init__(self, signals, handler):
        self.handler = handler
        self.old_handlers = {}
        try:
            self.signals = tuple(signals)
        except TypeError:
            self.signals = (signals,)

    def __enter__(self):
        for signal_type in self.signals:
            self.old_handlers[signal_type] = signal.getsignal(signal_type)
            signal.signal(signal_type, self.handler)

        return self

    def __exit__(self, type, value, traceback):
        for signal_type in self.signals:
            signal.signal(signal_type, self.old_handlers[signal_type])


T = TypeVar('T')


class QuitSignal(Signal):
    """
    A context manager that handles system signals by triggering a
    `threading.Event` instance, giving your program an opportunity to clean up
    and shut down gracefully. If the signal is repeated a second time, the
    process quits immediately.

    Parameters
    ----------
    signals : int or tuple of int
        The signal or list of signals to handle.
    graceful_message : string, optional
        A message to print to stdout when a signal is received.
    final_message : string, optional
        A message to print to stdout before exiting the process when a repeat
        signal is received.

    Examples
    --------
    Quit on SIGINT (ctrl+c) or SIGTERM:

    >>> with QuitSignal((signal.SIGINT, signal.SIGTERM)) as cancel:
    >>>     for item in some_list:
    >>>         if cancel.is_set():
    >>>             break
    >>>         do_some_work()
    """
    def __init__(self, signals=(signal.SIGINT, signal.SIGTERM), graceful_message=None, final_message=None):
        self.event = threading.Event()
        self.graceful_message = graceful_message or (
            'Attempting to finish existing work before exiting. Press ctrl+c '
            'to stop immediately.')
        self.final_message = final_message or (
            'Stopping immediately and aborting all work!')
        super().__init__(signals, self.handle_interrupt)

    def handle_interrupt(self, signal_type, frame):
        if not self.event.is_set():
            print(self.graceful_message, file=sys.stderr, flush=True)
            self.event.set()
        else:
            print(self.final_message, file=sys.stderr, flush=True)
            for child in multiprocessing.active_children():
                child.terminate()

            os._exit(128 + signal_type)

    def stop_iteration(self, iterable: Iterable[T]) -> Generator[T, None, None]:
        with self as cancel:
            for item in iterable:
                if cancel.is_set():
                    break
                else:
                    yield item

    def __enter__(self):
        super().__enter__()
        return self.event


class S3HashStore:
    """
    Store and track content-addressed data in S3. This relies on standard
    AWS environment variables or configuration files for authentication.

    Parameters
    ----------
    bucket : str
        The name of the S3 bucket to store content in.
    gzip : bool, default=False
        Whether to gzip data before storing it.
    extra_args : dict, optional
        Boto3 "extra args" that are used with various operations as applicable.
        For more info, see: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer
    dry_run : bool, default=False
        If true, calls to ``store`` do not actually upload. Allows this to be
        used in test/debug workflows without significantly altering logic.
    """

    def __init__(self, bucket: str, gzip: bool = False, extra_args: dict = {}, dry_run: bool = False) -> None:
        self.bucket = bucket
        self.extra_args = extra_args
        self.gzip = gzip
        if gzip:
            self.extra_args['ContentEncoding'] = 'gzip'
        self.seen_hashes = set()
        self.lock = threading.Lock()
        self.dry_run = dry_run

    def store(self, data: bytes, hash: str = '', content_type: str = '') -> str:
        if not hash:
            hash = hash_content(data)

        if not content_type:
            content_type = 'application/octet-stream'

        archive = S3Path(f's3://{self.bucket}', client=S3Client(
            file_cache_mode='close_file',
            extra_args={
                **self.extra_args,
                'ContentType': content_type
            },
        ))
        path = archive / hash

        upload = False
        with self.lock:
            if hash not in self.seen_hashes:
                self.seen_hashes.add(hash)
                upload = True

        if upload and not self.dry_run and not path.exists():
            logger.info(f'Uploading to S3 (hash={hash})')
            if self.gzip:
                data = gzip.compress(data)
            if not self.dry_run:
                written = path.write_bytes(data)
                # Double-check that we wrote everything we tried to. This might
                # be an unnecessary check, but it's possible it might save us
                # from some weird edge-cases related to how cloudpathlib uses
                # the local disk as an intermediary cache:
                # https://github.com/edgi-govdata-archiving/web-monitoring-processing/issues/929
                if written != len(data):
                    raise RuntimeError(
                        f'S3 write failure: only wrote {written} of '
                        f'{len(data)} bytes to S3'
                    )

        return path.as_url()


# Values correspond to `timedelta` keyword arguments.
CLI_DATETIME_UNITS = {
    'd': 'days',
    'h': 'hours',
    'm': 'minutes',
    '': 'hours'
}


def cli_datetime(raw) -> datetime:
    """
    Parse a CLI argument that should represent a datetime or time delta from
    now as a datetime. Datetimes are expected to be in ISO 8601 format, deltas
    are a number and optional unit, e.g. "5d" for 5 days. By default, deltas
    are negative (representing time *ago*), but specifying "+5d" treats as the
    future (time *from now*).

    Returned datetimes are always in UTC.
    """
    input = raw.strip()
    # Could also support ISO 8601 durations here, but probably not worthwhile.
    delta = re.match(r'^([+-]?)(\d+)([dhm]?)$', input)
    if delta:
        unit = CLI_DATETIME_UNITS[delta.group(3).lower()]
        value = float(delta.group(2))
        if delta.group(1) != '+':
            value *= -1
        return datetime.now(timezone.utc) + timedelta(**{unit: value})

    parsed = dateutil.parser.isoparse(input)
    # If it's a date, treat it as UTC.
    if re.match(r'^\d{4}-\d\d-\d\d$', input):
        return parsed.replace(tzinfo=timezone.utc)
    else:
        return parsed.astimezone(timezone.utc)


def parse_http_date(value: str | None, fallback: datetime | None) -> datetime:
    """
    Parse an HTTP Date string to a datetime object. If ``fallback`` is
    provided, it will be returned instead of raising ``ValueError`` for invalid
    date strings.
    """
    # This *could* be much simpler, but we want to track whether this parsing
    # routine is too strict in cases where we have a value worth parsing.
    # Empty strings and integers get a pass, since they are common exceptional
    # cases that we know should fail.
    # TODO: clean up or remove specialized logging after 2026-07-15, once we've
    #  had a few weeks to experience any interesting results.
    if value and not re.match(r'^\s*[+-]?[0-9]+\s*$', value):
        try:
            return email.utils.parsedate_to_datetime(value)
        except ValueError as error:
            if fallback:
                logger.warning(f'Invalid HTTP date: "{value}"')
                sentry_sdk.capture_exception(error, level='warning')

    if fallback:
        return fallback
    else:
        raise ValueError(f'Invalid HTTP date: "{value}"')


def estimate_snapshot_quality(
    url: str,
    timestamp: datetime,
    *,
    status: int,
    headers: dict,
    body: bytes | None = None,
    content_length: int | None = None,
    media_type: str | None = None,
    redirects: list[str] | tuple[str] | None = None,
) -> float:
    """
    Identify snapshots that are likely to have been blocked responses (e.g.
    a rate limit or firewall rule blocked the crawler) or intermittent errors.
    These don't represent what a regular user should have seen at the time, so
    we should avoid using them as candidates for comparison.
    """
    headers = {k.lower(): v for k, v in headers.items()}
    if content_length is None:
        if body is not None:
            content_length = len(body)
        else:
            content_length = int(headers.get('content-length', '-1'))

    if status == 200 and content_length == 0:
        status = 500

    server = headers.get('server', '').lower()

    no_cache = False
    if 'cache-control' in headers:
        cache_control = headers['cache-control'].lower()
        no_cache = 'no-cache' in cache_control or 'max-age=0' in cache_control
    if no_cache is False and 'expires' in headers:
        expires = parse_http_date(headers['expires'], fallback=timestamp)
        request_time = parse_http_date(headers.get('date'), fallback=timestamp)
        no_cache = (expires - request_time).total_seconds() < 60

    x_cache = headers.get('x-cache', '').lower()
    cache_error = 'error' in x_cache or 'n/a' in x_cache

    is_short_or_unknown = content_length < 1000
    content_type = media_type or headers.get('content-type', '')
    is_html = content_type.startswith('text/html')

    # AWS WAF sends the `x-amzn-waf-action` header for a lot of blocking
    # actions. It can come from different servers, so should be handled on its
    # own as a clear, concrete signal.
    if waf_action := headers.get('x-amzn-waf-action', '').lower():
        if waf_action in ('challenge', 'captcha'):
            return 0.0
        else:
            logger.warning(f'Unknown value for x-amzn-waf-action header: "{waf_action}"')

    if cf_mitigated := headers.get('cf-mitigated', '').lower():
        if server == 'cloudflare':
            if cf_mitigated == 'challenge':
                return 0.0
            else:
                logger.warning(f'Unknown value for cf-mitigated header: "{cf_mitigated}"')
        else:
            # We expect cf-mitigated to always come alongside a server header
            # for Cloudflare, unlike with AWS WAF above.
            logger.warning(
                'Unknown `server` for cf-mitigated header: '
                f'"server: {server}", "cf-mitigated: {cf_mitigated}" '
                '(expected "server: cloudflare")'
            )

    if status >= 400 and server.startswith('awselb/'):
        # We assume that blocking-related status code coming from directly from
        # an AWS ELB and not the origin server is really blocking.
        if status == 429:
            return 0.0
        elif status == 403 and is_short_or_unknown:
            return 0.1
        # Keeping these more fuzzy rules for other errors (e.g. 502/503/504
        # gateway errors) that are more iffy (a gateway error could be
        # intermittent, or it could be that the underlying origin server was
        # shut down) separate from the more concrete ones above. We probably
        # wouldn't want to fail to *record* these when importing even though we
        # want to treat them as suspect for task sheets.
        elif is_short_or_unknown and is_html:
            return 0.5
    elif status >= 400 and server == 'akamaighost' and is_short_or_unknown and no_cache:
        return 0.0
    elif server == 'cloudfront':
        # We're pretty confident CloudFront will never return a 404 as part of
        # its own WAF (it will return a 403). 404s only come from the origin.
        # Still... this is low confidence.
        if cache_error and status >= 400 and status != 404:
            return 0.0
    # elif server == 'cloudflare':
    #     # We don't have any special hints for Cloudlare beyond the cf-mitigated
    #     # header, which is already handled above.
    #     # NOTES: When Cloudflare provides `server-timing`, it will identify its
    #     # time with `cfEdge` and origin time with `cfOrigin`. Having edge time
    #     # but no record of origin time may also be a good hint of WAF behavior.
    #     ...
    elif 400 <= status < 500 and not server and is_short_or_unknown:
        # Very lazy server-timing header parsing. We could parse out the
        # description and the duration, but those don't matter too much here.
        server_timing = {}
        for item in headers.get('server-timing', '').split(','):
            key, _, value = item.partition(';')
            server_timing[key.lower().strip()] = value.strip()

        # Akamai Edgesuite doesn't explicitly identify itself, but it seems to
        # always include recognizable server-timing features and a 4xx status.
        #
        # Example good capture:
        #   server-timing: cdn-cache; desc=MISS, edge; dur=22, origin; dur=369, ak_p; desc="1776475897753_386075716_3264768070_38998_7536_11_0_255";dur=1
        #
        # Example bad capture:
        #   server-timing: cdn-cache; desc=HIT, edge; dur=1, ak_p; desc="1775872487192_399532111_2052555389_12_6012_263_573_-";dur=1
        #
        # (Unfortunately, can't find any examples of good cache hits.)
        if (
            'ak_p' in server_timing
            and 'cdn-cache' in server_timing
            # Expect no origin info (since WAF will have never hit the origin)
            # and single-digit milliseconds at the edge.
            and 'origin' not in server_timing
            and re.search(r'(^|;)\s*dur=\d(\.|$)', server_timing.get('edge', ''))
        ):
            return 0.25
    # TODO: see if we have any Azure CDN examples?
    elif status == 429 and is_short_or_unknown:
        return 0.1
    # TODO: More general heuristics?
    # else:
    #     content_type = media_type or headers.get('content-type', '')
    #     x_cache = headers.get('x-cache', '').lower()
    #     cache_miss = x_cache and not x_cache.startswith('hit')
    #     return content_type.startswith('text/html') and is_short_or_unknown and cache_miss

    # Special cases for redirects to known sinks that represent crawl blocking.
    if status == 200 and isinstance(redirects, (list, tuple)):
        block_url = 'unblock.federalregister.gov/'
        if (
            re.sub(r'^https?://', '', redirects[-1].lower()) == block_url
            and re.sub(r'^https?://', '', url.lower()) != block_url
        ):
            return 0.0

    return 1.0


def estimate_version_quality(version: dict[str, Any]) -> float:
    """
    A shortcut to ``estimate_snapshot_quality`` for web-monitoring-db version
    records.
    """
    timestamp = (
        dateutil.parser.isoparse(version['capture_time'])
        if isinstance(version['capture_time'], str)
        else version['capture_time']
    )
    return estimate_snapshot_quality(
        url=version['url'],
        timestamp=timestamp,
        status=version['status'] or (600 if version.get('network_error') else 200),
        headers=version['headers'] or {},
        content_length=version['content_length'],
        media_type=version['media_type'],
        redirects=version['source_metadata'].get('redirects'),
    )
