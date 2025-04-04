import logging
import re


logger = logging.getLogger(__name__)


# We do some parsing of HTML and PDF documents to extract the title before
# importing the document. These media types are used to determine whether and
# how to parse the document.
HTML_MEDIA_TYPES = frozenset((
    'application/html',
    'application/xhtml',
    'application/xhtml+xml',
    'application/xml',
    'application/xml+html',
    'application/xml+xhtml',
    'text/webviewhtml',
    'text/html',
    'text/x-server-parsed-html',
    'text/xhtml',
))
PDF_MEDIA_TYPES = frozenset((
    'application/pdf',
    'application/x-pdf',
))

# These media types are so meaningless that it's worth sniffing the content to
# see if we can determine an actual media type.
SNIFF_MEDIA_TYPES = frozenset((
    'application/octet-stream',
    'application/x-download',
    'binary/octet-stream',
))

# Identifies a bare media type (that is, one without parameters)
MEDIA_TYPE_EXPRESSION = re.compile(r'^\w+/\w[\w+_\-.]+$')

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


def read_media_type(headers: dict[str, str], url: str = '') -> tuple[str, str]:
    """
    Read media type and media type parameters from a set of HTTP headers. Pass
    an optional URL for better debug logging.
    """
    media, *parameters = headers.get('Content-Type', '').split(';')

    # Clean up media type
    media = media.strip().lower()
    if not MEDIA_TYPE_EXPRESSION.match(media):
        url_info = f' for "{url}"' if url else ''
        logger.info('Unknown media type "%s"%s', media, url_info)
        media = ''

    # Clean up whitespace, remove empty parameters, etc.
    clean_parameters = (param.strip() for param in parameters)
    parameters = [param for param in clean_parameters if param]
    parameter_string = '; '.join(parameters)

    return media, parameter_string


def find_media_type(headers: dict[str, str], content: bytes = b'', url: str = '') -> tuple[str, str]:
    """
    Determine the media type of an HTTP response. This will attempt to read an
    expicitly set media type from the headers, but will fall back to sniffing
    if necessary.

    This may return empty strings if a media type cannot be found.
    """
    media_type, media_type_parameters = read_media_type(headers, url)
    if not media_type or media_type in SNIFF_MEDIA_TYPES:
        media_type = sniff_media_type(content, media_type)

    return media_type, media_type_parameters
