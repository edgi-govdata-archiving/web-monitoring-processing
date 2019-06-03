# Tools for checking the type of content and making sure it's acceptable for
# a given diffing algorithm

import re


# Content Types that we know represent HTML
ACCEPTABLE_CONTENT_TYPES = (
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
)

# Matches Content Types that *could* be acceptable for diffing as HTML
UNKNOWN_CONTENT_TYPE_PATTERN = re.compile(r'^(%s)$' % '|'.join((
    r'application/octet-stream',
    r'text/.+'
)))


def detect_binary_type(data):
    """Detect binary types from well known first bytes values.
    Supports: JPEG, PDF, Postscript, GIF, PNG. Return None if no binary type
    is detected.
    """
    if len(data) == 0:
        return None
    b0 = data[0:1]
    if b0 == b'\xFF':
        if data[1:2] == b'\xD8':
            return "image/jpeg"
    elif b0 == b'%':
        if data[1:5] == b'PDF-':
            return "application/pdf"
        elif data[1:5] == b'!PS-':
            return "application/postscript"
    elif b0 == b'G':
        if data[1:4] == b'IF8':
            return "image/gif"
    elif b0 == b'\x89':
        if data[1:4] == b'PNG':
            return "image/png"


def is_not_html(raw_body, headers=None, check_options='normal'):
    """
    Determine whether an HTTP response is not HTML by its headers. In general,
    this errs on the side of leniency; it should have few false positives, but
    many false negatives.

    Parameters
    ----------
    raw_body : raw binary response
    headers : dict
        Any HTTP headers
    check_options : string
        Control content type detection. Options are:
        - `normal` uses the `Content-Type` header and then falls back to
          sniffing to determine content type.
        - `nocheck` ignores the `Content-Type` header but still sniffs.
        - `nosniff` uses the `Content-Type` header but does not sniff.
        - `ignore` doesn’t do any checking at all.
    """
    if headers and (check_options == 'normal' or check_options == 'nosniff'):
        content_type = headers.get('Content-Type', '').split(';', 1)[0].strip()
        if content_type:
            if content_type in ACCEPTABLE_CONTENT_TYPES:
                return False
            elif not UNKNOWN_CONTENT_TYPE_PATTERN.match(content_type):
                return True
    if check_options == 'normal' or check_options == 'nocheck':
        if detect_binary_type(raw_body):
            return True
        else:
            return False
    return False
