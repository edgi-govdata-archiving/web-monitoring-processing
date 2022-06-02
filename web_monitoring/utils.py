import cchardet
import codecs
import hashlib
import io
import logging
import lxml.html
import os
from PyPDF2 import PdfReader
from PyPDF2.errors import PyPdfError
import queue
import re
import signal
import sys
import threading
import time


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
        # try to identify encoding using cchardet. Use up to 18kb of the
        # content for detection. Its not necessary to use the full content
        # as it could be huge. Also, if you use too little, detection is not
        # accurate.
        detected = cchardet.detect(content[:18432])
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


# Patterns used to sniff various media types. Based on:
# - https://dev.w3.org/html5/cts/html5-type-sniffing.html
# - https://mimesniff.spec.whatwg.org/#rules-for-identifying-an-unknown-mime-type
#
# NOTE: a "verbose" regex would be nice here, but they don't seem to work with
# binary strings.
SNIFF_HTML_HINTS = (
    rb'<!DOCTYPE HTML',
    rb'<HTML',
    rb'<HEAD',
    rb'<SCRIPT',
    rb'<IFRAME',
    rb'<H1',
    rb'<DIV',
    rb'<FONT',
    rb'<TABLE',
    rb'<A',
    rb'<STYLE',
    rb'<TITLE',
    rb'<B',
    rb'<BODY',
    rb'<BR',
    rb'<P',
    rb'<!--',
)

SNIFF_HTML_PATTERN = re.compile(
    rb'^[\s\n\r]*(%s)[\s\n\r>]' % b'|'.join(SNIFF_HTML_HINTS),
    re.IGNORECASE
)

SNIFF_MEDIA_TYPE_PATTERNS = {
    SNIFF_HTML_PATTERN: 'text/html',
    re.compile(rb'^[\s\n\r]*<?xml'): 'text/xml',
    re.compile(rb'^%PDF-'): 'application/pdf',
    re.compile(rb'^%!PS-Adobe-'): 'application/postscript',
    re.compile(rb'^(GIF87a|GIF89a)'): 'image/gif',
    re.compile(rb'^\x89\x50\x4E\x47\x0D\x0A\x1A\x0A'): 'image/png',
    re.compile(rb'^\xFF\xD8\xFF'): 'image/jpeg',
    re.compile(rb'^BM'): 'image/bmp',
}


def sniff_media_type(content, default='application/octet-stream'):
    """
    Detect the media type of some content. If the media type can't be detected,
    the value of the ``default`` parameter will be returned.

    This is similar to how browsers do it and is based on:
    - https://dev.w3.org/html5/cts/html5-type-sniffing.html
    - https://mimesniff.spec.whatwg.org/#rules-for-identifying-an-unknown-mime-type

    Parameters
    ----------
    content : bytes
    default : str or None

    Returns
    -------
    str
        The detected media type of the content.
    """
    for pattern, media_type in SNIFF_MEDIA_TYPE_PATTERNS.items():
        if pattern.match(content):
            return media_type

    return default


def extract_title(content_bytes, encoding='utf-8'):
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

        return pdf.metadata.title
    except PyPdfError:
        return None


def hash_content(content_bytes):
    "Create a version_hash for the content of a snapshot."
    return hashlib.sha256(content_bytes).hexdigest()


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
    def __init__(self, signals, graceful_message=None, final_message=None):
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
            os._exit(100)

    def __enter__(self):
        super().__enter__()
        return self.event
