from datetime import datetime, timezone
import os
from pathlib import Path
from unittest.mock import patch
from vcr import VCR
from vcr.record_mode import RecordMode
from wayback import WaybackClient
from web_monitoring.cli.cli import (_filter_unchanged_versions,
                                    WaybackRecordsWorker, import_ia_db_urls,
                                    _is_valid, validate_db_credentials)


# The only matters when re-recording the tests for vcr.
AUTH_ENVIRON = {
    'WEB_MONITORING_DB_URL': 'https://api.monitoring.envirodatagov.org',
    'WEB_MONITORING_DB_EMAIL': 'public.access@envirodatagov.org',
    'WEB_MONITORING_DB_PASSWORD': 'PUBLIC_ACCESS'
}

# This stashes HTTP responses in local files (one per test) so that an actual
# server does not have to be running.
cassette_library_dir = str(Path(__file__).parent / Path('cassettes/cli'))
ia_vcr = VCR(
         serializer='yaml',
         cassette_library_dir=cassette_library_dir,
         record_mode=RecordMode.NONE if os.getenv('CI') else RecordMode.ONCE,
         match_on=['uri', 'method'],
         filter_headers=['authorization'],
)


def test_filter_unchanged_versions():
    versions = (
        {'page_url': 'http://example.com', 'version_hash': 'a'},
        {'page_url': 'http://example.com', 'version_hash': 'b'},
        {'page_url': 'http://example.com', 'version_hash': 'b'},
        {'page_url': 'http://other.com',   'version_hash': 'b'},
        {'page_url': 'http://example.com', 'version_hash': 'b'},
        {'page_url': 'http://example.com', 'version_hash': 'c'},
        {'page_url': 'http://other.com',   'version_hash': 'd'},
        {'page_url': 'http://other.com',   'version_hash': 'b'},
    )

    assert list(_filter_unchanged_versions(versions)) == [
        {'page_url': 'http://example.com', 'version_hash': 'a'},
        {'page_url': 'http://example.com', 'version_hash': 'b'},
        {'page_url': 'http://other.com',   'version_hash': 'b'},
        {'page_url': 'http://example.com', 'version_hash': 'c'},
        {'page_url': 'http://other.com',   'version_hash': 'd'},
        {'page_url': 'http://other.com',   'version_hash': 'b'},
    ]


def test_is_valid():
    valid_url = 'https://example.com'
    valid_url_without_tld = 'https://google'
    invalid_url = 'asldkfje'

    assert _is_valid(valid_url)
    assert _is_valid(valid_url_without_tld)
    assert not _is_valid(invalid_url)


@ia_vcr.use_cassette()
def test_format_memento():
    with WaybackClient() as client:
        url = 'https://www.fws.gov/birds/'
        cdx_records = client.search(url, from_date='20171124151314',
                                    to_date='20171124151316')
        record = next(cdx_records)
        memento = client.get_memento(record.raw_url, exact_redirects=False)
        worker = WaybackRecordsWorker(None, None, None, None, None)
        version = worker.format_memento(memento, record, ['maintainer'],
                                        ['tag'])

        assert isinstance(version, dict)

        assert version['page_url'] == url
        assert version['page_maintainers'] == ['maintainer']
        assert version['page_tags'] == ['tag']
        assert version['title'] == "U.S. Fish & Wildlife Service - Migratory Bird Program | Conserving America's Birds"

        assert version['capture_time'] == '2017-11-24T15:13:15Z'
        assert version['uri'] == f'https://web.archive.org/web/20171124151315id_/{url}'
        assert version['version_hash'] == 'ae433414499f91630983fc379d9bafae67250061178930b8779ee76c82485491'
        assert version['source_type'] == 'internet_archive'
        assert version['status'] == 200
        assert version['media_type'] == 'text/html'
        assert version['source_metadata'] == {
            'headers': {
                'content-type': 'text/html',
                'date': 'Fri, 24 Nov 2017 15:13:14 GMT',
                'strict-transport-security': 'max-age=31536000; includeSubDomains; preload',
                'transfer-encoding': 'chunked'
            },
            'view_url': 'https://web.archive.org/web/20171124151315/https://www.fws.gov/birds/'
        }


@ia_vcr.use_cassette()
def test_format_memento_handles_redirects():
    with WaybackClient() as client:
        url = 'https://www.epa.gov/ghgreporting/san5779-factsheet'
        final_url = 'https://www.epa.gov/ghgreporting/proposed-rule-fact-sheet-greenhouse-gas-reporting-program-addition-global-warming'

        cdx_records = client.search(url, from_date='20180808094144',
                                    to_date='20180808094145')
        record = next(cdx_records)
        memento = client.get_memento(record.raw_url, exact_redirects=False)
        worker = WaybackRecordsWorker(None, None, None, None, None)
        version = worker.format_memento(memento, record, None, None)

        assert isinstance(version, dict)
        assert version['source_metadata']['redirected_url'] == final_url
        assert len(version['source_metadata']['redirects']) == 2
        assert version['source_metadata']['redirects'][0] == url
        assert version['source_metadata']['redirects'][1] == final_url


@ia_vcr.use_cassette()
def test_format_memento_pdf():
    with WaybackClient() as client:
        url = 'https://www.epa.gov/sites/production/files/2016-08/documents/oar-climate-change-adaptation-plan.pdf'
        cdx_records = client.search(url, from_date='20200430024232',
                                    to_date='20200430024232')
        record = next(cdx_records)
        memento = client.get_memento(record.raw_url, exact_redirects=False)
        worker = WaybackRecordsWorker(None, None, None, None, None)
        version = worker.format_memento(memento, record, ['maintainer'],
                                        ['tag'])

        assert isinstance(version, dict)

        assert version['page_url'] == url
        assert version['page_maintainers'] == ['maintainer']
        assert version['page_tags'] == ['tag']
        assert version['title'] == "EPA Office of Air and Radiation Climate Change Adaptation Implementation Plan, June 2014"
        assert version['capture_time'] == '2020-04-30T02:42:32Z'
        assert version['uri'] == f'https://web.archive.org/web/20200430024232id_/{url}'
        assert version['version_hash'] == 'bdfd8c1ee22b70cd1b8bd513989822e066a9656f4578606ef3d5feb6204e3dc6'
        assert version['source_type'] == 'internet_archive'
        assert version['status'] == 200
        assert version['media_type'] == 'application/pdf'
        assert version['source_metadata'] == {
            'headers': {
                'accept-ranges': 'bytes',
                'cache-control': 'max-age=572',
                'connection': 'close',
                'content-length': '375909',
                'content-type': 'application/pdf',
                'date': 'Thu, 30 Apr 2020 02:42:32 GMT',
                'etag': '"12c958e520c9ff580f52ee11446c5e0c:1579909999.298098"',
                'expires': 'Thu, 30 Apr 2020 02:52:04 GMT',
                'last-modified': 'Tue, 16 Aug 2016 15:43:21 GMT',
                'server': 'AkamaiNetStorage',
                'server-timing': 'cdn-cache; desc=HIT',
                'strict-transport-security': 'max-age=31536000; preload;',
                'x-content-type-options': 'nosniff'
            },
            'view_url': 'https://web.archive.org/web/20200430024232/https://www.epa.gov/sites/production/files/2016-08/documents/oar-climate-change-adaptation-plan.pdf'
        }


@ia_vcr.use_cassette()
def test_html_title_parsing():
    with WaybackClient() as client:
        url = 'https://www.cdc.gov/coronavirus/2019-ncov/travelers/after-travel-precautions-japanese.html'
        cdx_records = client.search(url, from_date='20201123000000',
                                    to_date='20201124000000')
        record = next(cdx_records)
        memento = client.get_memento(record.raw_url, exact_redirects=False)
        worker = WaybackRecordsWorker(None, None, None, None, None)
        version = worker.format_memento(memento, record, [], [])
        assert version['title'] == '旅行後 | CDC'


# TODO: this test covers some of the various error cases, but probably not all
# of them, and has a pretty big cassette file. We should *probably* rewrite it
# with mock db.client and wayback.WaybackClient instances that exercise all the
# various errors (BlockedByRobots, Unplaybackable, RetryError, etc.) that could
# arise from the clients.
# NOTE: generating the cassette also requires special access.
@ia_vcr.use_cassette()
@patch.dict(os.environ, AUTH_ENVIRON)
def test_complete_import_ia_db_urls():
    # The only real goal in this test is to make sure it doesn't raise.
    import_ia_db_urls(from_date=datetime(2019, 1, 1, 3, 0, tzinfo=timezone.utc),
                      to_date=datetime(2019, 1, 2, 3, 0, tzinfo=timezone.utc),
                      skip_unchanged='resolved-response',
                      url_pattern='*epa.gov/hfstudy/epa*')


@ia_vcr.use_cassette()
@patch.dict(os.environ, AUTH_ENVIRON)
def test_validate_db_credentials():
    validate_db_credentials()
