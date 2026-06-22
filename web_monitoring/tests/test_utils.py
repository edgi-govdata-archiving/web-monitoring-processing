from datetime import datetime, timezone
import json
from pathlib import Path
import pytest
import queue
from support import get_fixture_bytes, get_fixture_paths
import threading
from web_monitoring.db import DbJsonDecoder
from web_monitoring.utils import (estimate_snapshot_quality,
                                  estimate_version_quality,
                                  extract_html_title, extract_pdf_title,
                                  normalize_url, RateLimit, FiniteQueue)


def test_extract_html_title():
    title = extract_html_title(b'''<html>
        <head><title>THIS IS THE TITLE</title></head>
        <body>Blah</body>
    </html>''')
    assert title == 'THIS IS THE TITLE'


def test_extract_html_title_from_titleless_page():
    title = extract_html_title(b'''<html>
        <head><meta charset="utf-8"></head>
        <body>Blah</body>
    </html>''')
    assert title == ''


def test_extract_html_title_handles_whitespace():
    title = extract_html_title(b'''<html>
        <head>
            <meta charset="utf-8">
            <title>

                THIS IS
                THE  TITLE
            </title>
        </head>
        <body>Blah</body>
    </html>''')
    assert title == 'THIS IS THE TITLE'


def test_extract_pdf_title():
    pdf_bytes = get_fixture_bytes('basic_title.pdf')
    title = extract_pdf_title(pdf_bytes)
    assert title == 'Basic PDF Title'


def test_extract_pdf_title_malformed_pdf():
    title = extract_pdf_title(b'This is not a PDF.')
    assert title is None


def test_extract_pdf_title_encrypted_no_password():
    """
    Get the title of a PDF that's encrypted with an empty password. This should
    successfully decrypt the PDF and get the title.
    """
    pdf_bytes = get_fixture_bytes('encrypted_with_empty_password.pdf')
    title = extract_pdf_title(pdf_bytes)
    assert title == 'Empty Password Title Field'


@pytest.mark.skip("""
    pypdf has maintainers again, and we no longer have any examples of
    unsupported encryption schemes! Bring this test back if we
    encounter new, undecryptable PDFs.
""")
def test_extract_pdf_title_encrypted_unsupported_algorithm():
    """
    Get the title of a PDF that's encrypted with an unsupported algorithm. This
    should return `None` since the PDF can not be read.
    """
    pdf_bytes = get_fixture_bytes('encrypted_with_unsupported_algorithm.pdf')
    title = extract_pdf_title(pdf_bytes)
    assert title is None


def test_extract_pdf_title_no_eof():
    """
    Get the title of a PDF that has no end-of-file marker. This is malformed,
    but we should still be able to extract some metadata.
    """
    pdf_bytes = get_fixture_bytes('no_eof_marker.pdf')
    title = extract_pdf_title(pdf_bytes)
    assert title == 'Untitled'


def test_extract_pdf_title_no_metadata():
    """
    Get the title of a PDF that has no metadata at all without raising an
    exception.
    """
    pdf_bytes = get_fixture_bytes('no_metadata.pdf')
    title = extract_pdf_title(pdf_bytes)
    assert title is None


class TestNormalizeUrl:
    def test_normalizes_scheme(self):
        assert normalize_url('hTTps://whatever.com/') == 'https://whatever.com/'

    def test_normalizes_domain(self):
        assert normalize_url('https://whatEVER.com/') == 'https://whatever.com/'

    def test_removes_redundant_https_port(self):
        assert normalize_url('https://whatever.com:443/') == 'https://whatever.com/'

    def test_removes_redundant_http_port(self):
        assert normalize_url('http://whatever.com:80/') == 'http://whatever.com/'

    def test_leaves_credentials_along(self):
        assert normalize_url('https://aBc:DeF@whatEVER.com/') == 'https://aBc:DeF@whatever.com/'

    def test_ensures_a_path(self):
        assert normalize_url('https://whatever.com') == 'https://whatever.com/'

    def test_removes_fragment(self):
        assert normalize_url('https://whatever.com/x#y') == 'https://whatever.com/x'

    def test_keeps_existing_path(self):
        assert normalize_url('https://whatever.com/X/y') == 'https://whatever.com/X/y'

    def test_keeps_www(self):
        assert normalize_url('https://www.whatever.com/') == 'https://www.whatever.com/'
        assert normalize_url('https://www3.whatever.com/') == 'https://www3.whatever.com/'


class TestRateLimit:
    def test_rate_limit(self):
        limiter = RateLimit(per_second=2)
        start_time = datetime.utcnow()
        for i in range(2):
            with limiter:
                1 + 1
        duration = datetime.utcnow() - start_time
        assert duration.total_seconds() > 0.5

    def test_separate_rate_limits_do_not_affect_each_other(self):
        start_time = datetime.utcnow()

        limit_a = RateLimit(2)
        limit_b = RateLimit(2)
        with limit_a:
            1 + 1
        with limit_b:
            1 + 1
        with limit_a:
            1 + 1
        with limit_b:
            1 + 1

        duration = datetime.utcnow() - start_time
        assert duration.total_seconds() > 0.5
        assert duration.total_seconds() < 0.55

    def test_rate_limit_does_not_interfere_with_exceptions(self):
        with pytest.raises(ValueError):
            with RateLimit(2):
                raise ValueError('OH NO!')


class TestFiniteQueue:
    def test_queue_ends_with_QUEUE_END(self):
        test_queue = FiniteQueue()
        test_queue.put('First')
        test_queue.end()

        assert test_queue.get() == 'First'
        assert test_queue.get() is FiniteQueue.QUEUE_END
        # We should keep getting QUEUE_END from now on, too.
        assert test_queue.get() is FiniteQueue.QUEUE_END

    def test_queue_is_iterable(self):
        test_queue = FiniteQueue()
        test_queue.put('First')
        test_queue.end()

        result = list(test_queue)
        assert result == ['First']

    def test_queue_can_be_safely_read_from_multiple_threads(self):
        # We want to make sure that a thread can't get stuck waiting for the
        # next item on a queue that is already ended.
        test_queue = FiniteQueue()
        results = queue.SimpleQueue()

        def read_one_item():
            try:
                results.put(test_queue.get(timeout=1))
            except queue.Empty as error:
                results.put(error)

        threads = [threading.Thread(target=read_one_item) for i in range(3)]
        [thread.start() for thread in threads]
        test_queue.put('First')
        test_queue.end()
        [thread.join() for thread in threads]

        assert results.get() == 'First'
        assert results.get() is FiniteQueue.QUEUE_END
        assert results.get() is FiniteQueue.QUEUE_END


XFAIL_SNAPSHOT_QUALITY = [
    # No great way to identify this without access to the body.
    'b47ca1d6-0f4e-4015-9940-dc666f755eb1.json',
]


class TestEstimateSnapshotQuality:
    # Test files are in `snapshot_quality/<server_type>-<expected>/*.json`. They
    # are version records from web-monitoring-db, as captured from the API.
    @pytest.mark.parametrize('file,expected', [
        pytest.param(
            f'{file.parent.name}/{file.name}',
            float(file.parent.name.rsplit('-', 1)[1]),
            marks=pytest.mark.xfail if file.name in XFAIL_SNAPSHOT_QUALITY else []
        )
        for file in get_fixture_paths('snapshot_quality/*/*.json')
    ])
    def test_estimate_version_quality(self, file, expected):
        version = json.loads(
            get_fixture_bytes(Path('snapshot_quality') / file),
            cls=DbJsonDecoder
        )['data']
        assert expected == estimate_version_quality(version)

    def test_estimate_snapshot_quality_accepts_integer_expires_header(self):
        assert 1.0 == estimate_snapshot_quality(
            url='https://websoilsurver.sc.egov.usda.gov/',
            timestamp=datetime(2026, 3, 10, 1, 31, tzinfo=timezone.utc),
            status=200,
            headers={
                'expires': '-1',
                'date': 'Tue, 10 Mar 2026 01:31:00 GMT',
                'content-type': 'text/html; charset=utf-8',
                'content-length': '543681',
            }
        )

    def test_estimate_snapshot_quality_accepts_invalid_date_header(self):
        assert 1.0 == estimate_snapshot_quality(
            url='https://websoilsurver.sc.egov.usda.gov/',
            timestamp=datetime(2026, 3, 10, 1, 31, tzinfo=timezone.utc),
            status=200,
            headers={
                'expires': 'Tue, 10 Mar 2026 01:31:00 GMT',
                'date': 'Hey hello whats up this is wrong',
                'content-type': 'text/html; charset=utf-8',
                'content-length': '543681',
            }
        )
