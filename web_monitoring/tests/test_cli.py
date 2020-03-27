from datetime import datetime, timezone
import os
from pathlib import Path
from unittest.mock import patch
import vcr
from wayback import WaybackClient
from web_monitoring.cli.cli import (_filter_unchanged_versions,
                                    WaybackRecordsWorker, import_ia_db_urls,
                                    _is_valid)


# The only matters when re-recording the tests for vcr.
AUTH_ENVIRON = {
    'WEB_MONITORING_DB_URL': 'https://api-staging.monitoring.envirodatagov.org',
    'WEB_MONITORING_DB_EMAIL': 'public.access@envirodatagov.org',
    'WEB_MONITORING_DB_PASSWORD': 'PUBLIC_ACCESS'
}

# This stashes HTTP responses in local files (one per test) so that an actual
# server does not have to be running.
cassette_library_dir = str(Path(__file__).parent / Path('cassettes/cli'))
ia_vcr = vcr.VCR(
         serializer='yaml',
         cassette_library_dir=cassette_library_dir,
         record_mode='once',
         match_on=['uri', 'method'],
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
        version = WaybackRecordsWorker.format_memento(None, memento, record,
                                                      ['maintainer'], ['tag'])

        assert isinstance(version, dict)

        assert version['page_url'] == url
        assert version['page_maintainers'] == ['maintainer']
        assert version['page_tags'] == ['tag']
        assert version['title'] == "U.S. Fish & Wildlife Service - Migratory Bird Program | Conserving America's Birds"

        assert version['capture_time'] == '2017-11-24T15:13:15Z'
        assert version['uri'] == f'http://web.archive.org/web/20171124151315id_/{url}'
        assert version['version_hash'] == 'ae433414499f91630983fc379d9bafae67250061178930b8779ee76c82485491'
        assert version['source_type'] == 'internet_archive'
        assert version['status'] == 200
        assert version['source_metadata'] == {
            'encoding': 'ISO-8859-1',
            'headers': {
                'Date': 'Fri, 24 Nov 2017 15:13:14 GMT',
                'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
                'Transfer-Encoding': 'chunked'
            },
            'mime_type': 'text/html',
            'view_url': 'http://web.archive.org/web/20171124151315/https://www.fws.gov/birds/'
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
        version = WaybackRecordsWorker.format_memento(None, memento, record,
                                                      None, None)

        assert isinstance(version, dict)
        assert version['source_metadata']['redirected_url'] == final_url
        assert len(version['source_metadata']['redirects']) == 2
        assert version['source_metadata']['redirects'][0] == url
        assert version['source_metadata']['redirects'][1] == final_url


# TODO: this test covers some of the various error cases, but probably not all
# of them, and has a pretty big cassette file. We should *probably* rewrite it
# with mock db.client and wayback.WaybackCLient instances that exercise all the
# various errors (BlockedByRobots, Unplaybackable, RetryError, etc.) that could
# arise from the clients.
@ia_vcr.use_cassette()
@patch.dict(os.environ, AUTH_ENVIRON)
def test_complete_import_ia_db_urls():
    # The only real goal in this test is to make sure it doesn't raise.
    import_ia_db_urls(from_date=datetime(2019, 1, 1, 3, 22, tzinfo=timezone.utc),
                      to_date=datetime(2019, 1, 1, 3, 25, tzinfo=timezone.utc),
                      skip_unchanged='resolved-response',
                      url_pattern='*energy.gov/*')
