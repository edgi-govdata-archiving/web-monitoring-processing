# This module expects a local deployment of web-monitoring-db to be running
# with the default settings.

# The purpose is to test that the Python API can exercise all parts of the REST
# API. It is not meant to thoroughly check the correctness of the REST API.
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import pytest
from unittest.mock import patch
import urllib3.util
from web_monitoring.db import (Client,
                               MissingCredentials,
                               UnauthorizedCredentials,
                               DEFAULT_RETRIES,
                               DEFAULT_BACKOFF,
                               DEFAULT_TIMEOUT)
import vcr


# This stashes web-monitoring-dbserver responses in JSON files (one per test)
# so that an actual server does not have to be running.
cassette_library_dir = str(Path(__file__).parent / Path('cassettes/db'))
db_vcr = vcr.VCR(
         serializer='json',
         cassette_library_dir=cassette_library_dir,
         record_mode='once',
         match_on=['uri', 'method'],
)
global_stash = {}  # used to pass info between tests


# Refers to real data that is part of the 'seed' dataset in web-monitoring-db
URL = 'https://www3.epa.gov/climatechange/impacts/society.html'
SITE = 'site:EPA - www3.epa.gov'
AGENCY = 'EPA'
PAGE_ID = '3d068c64-967a-4ec7-af49-f8fa0f19e6f1'
TO_VERSION_ID = '795e6ff4-fcc0-444c-9f31-2b156f7dd4d4'
VERSIONISTA_ID = '13708349'

# This is used in new Versions that we add.
TIME = datetime(2017, 11, 15, tzinfo=timezone.utc)
NEW_VERSION_ID = '06620776-d347-4abd-a423-a871620299a9'

# This only matters when re-recording the tests for vcr.
AUTH = {'url': "http://localhost:3000",
        'email': "seed-admin@example.com",
        'password': "PASSWORD"}


def test_partial_creds():
    try:
        env = os.environ.copy()
        os.environ.clear()

        # Should work with no credentials.
        Client.from_env()

        # Should fail with partial credentials.
        with pytest.raises(MissingCredentials):
            os.environ.update({'WEB_MONITORING_DB_EMAIL': AUTH['email']})
            Client.from_env()

        # Should work with complete credentials.
        os.environ.update({'WEB_MONITORING_DB_URL': AUTH['url'],
                           'WEB_MONITORING_DB_EMAIL': AUTH['email'],
                           'WEB_MONITORING_DB_PASSWORD': AUTH['password']})
        Client.from_env()
    finally:
        os.environ.update(env)


# DEPRECATED
@db_vcr.use_cassette()
def test_list_pages():
    cli = Client(**AUTH)
    res = cli.list_pages()
    assert res['data']

    # Test datetimes are parsed correctly.
    assert isinstance(res['data'][0]['created_at'], datetime)
    assert isinstance(res['data'][0]['updated_at'], datetime)

    # Test chunk query parameters.
    res = cli.list_pages(chunk_size=2)
    assert len(res['data']) == 2
    res = cli.list_pages(chunk_size=5)
    assert len(res['data']) == 5

    # Test filtering query parameters.
    res = cli.list_pages(url='__nonexistent__')
    assert len(res['data']) == 0
    res = cli.list_pages(url=URL)
    assert len(res['data']) > 0
    res = cli.list_pages(tags=['__nonexistent__'])
    assert len(res['data']) == 0
    res = cli.list_pages(tags=[SITE])
    assert len(res['data']) > 0
    res = cli.list_pages(maintainers=['__nonexistent__'])
    assert len(res['data']) == 0
    res = cli.list_pages(maintainers=[AGENCY])
    assert len(res['data']) > 0

    # Test relations
    res = cli.list_pages(include_earliest=True)
    assert all(['earliest' in page for page in res['data']]) is True
    assert isinstance(res['data'][0]['earliest']['created_at'], datetime)
    assert isinstance(res['data'][0]['earliest']['updated_at'], datetime)
    res = cli.list_pages(include_latest=True)
    assert all(['latest' in page for page in res['data']]) is True
    assert isinstance(res['data'][0]['latest']['created_at'], datetime)
    assert isinstance(res['data'][0]['latest']['updated_at'], datetime)


@db_vcr.use_cassette()
def test_get_pages():
    cli = Client(**AUTH)
    pages = list(cli.get_pages())
    assert len(pages) > 0
    assert isinstance(pages[0]['created_at'], datetime)
    assert isinstance(pages[0]['updated_at'], datetime)


def test_get_pages_chunk_size(requests_mock):
    requests_mock.get('/api/v0/pages?chunk_size=3', json={"data": [{'fake': 'page'}]})
    cli = Client(**AUTH)
    pages = cli.get_pages(chunk_size=3)
    next(pages)


@db_vcr.use_cassette()
def test_get_pages_can_filter_url():
    cli = Client(**AUTH)

    pages = cli.get_pages(url='__nonexistent__')
    assert len(list(pages)) == 0

    pages = cli.get_pages(url=URL)
    assert len(list(pages)) > 0


@db_vcr.use_cassette()
def test_get_pages_can_filter_tags():
    cli = Client(**AUTH)

    pages = cli.get_pages(tags=['__nonexistent__'])
    assert len(list(pages)) == 0

    pages = cli.get_pages(tags=[SITE])
    assert len(list(pages)) > 0


@db_vcr.use_cassette()
def test_get_pages_can_filter_maintainers():
    cli = Client(**AUTH)

    pages = cli.get_pages(maintainers=['__nonexistent__'])
    assert len(list(pages)) == 0

    pages = cli.get_pages(maintainers=[AGENCY])
    assert len(list(pages)) > 0


@db_vcr.use_cassette()
def test_get_pages_includes_relations():
    cli = Client(**AUTH)
    pages = list(cli.get_pages(include_earliest=True))
    assert all(['earliest' in page for page in pages]) is True
    assert isinstance(pages[0]['earliest']['created_at'], datetime)
    assert isinstance(pages[0]['earliest']['updated_at'], datetime)

    pages = list(cli.get_pages(include_latest=True))
    assert all(['latest' in page for page in pages]) is True
    assert isinstance(pages[0]['latest']['created_at'], datetime)
    assert isinstance(pages[0]['latest']['updated_at'], datetime)


@db_vcr.use_cassette()
def test_get_page():
    cli = Client(**AUTH)
    res = cli.get_page(PAGE_ID)
    assert res['data']['uuid'] == PAGE_ID


# DEPRECATED
@db_vcr.use_cassette()
def test_list_page_versions():
    cli = Client(**AUTH)
    res = cli.list_versions(page_id=PAGE_ID)
    assert all([v['page_uuid'] == PAGE_ID for v in res['data']])


@db_vcr.use_cassette()
def test_get_versions_for_page():
    cli = Client(**AUTH)
    versions = cli.get_versions(page_id=PAGE_ID)
    assert all(v['page_uuid'] == PAGE_ID for v in versions)


# DEPRECATED
@db_vcr.use_cassette()
def test_list_versions():
    cli = Client(**AUTH)
    res = cli.list_versions()
    assert res['data']

    # Test relations
    res = cli.list_versions(include_change_from_previous=True)
    assert all(['change_from_previous' in item for item in res['data']]) is True
    res = cli.list_versions(include_change_from_earliest=True)
    assert all(['change_from_earliest' in item for item in res['data']]) is True


@db_vcr.use_cassette()
def test_get_versions():
    cli = Client(**AUTH)
    versions = cli.get_versions()
    first = next(versions)
    assert 'uuid' in first


@db_vcr.use_cassette()
def test_get_versions_includes_changes():
    cli = Client(**AUTH)

    versions = cli.get_versions(include_change_from_previous=True)
    first = next(versions)
    assert 'change_from_previous' in first

    versions = cli.get_versions(include_change_from_earliest=True)
    first = next(versions)
    assert 'change_from_earliest' in first


@db_vcr.use_cassette()
def test_get_version():
    cli = Client(**AUTH)
    res = cli.get_version(TO_VERSION_ID)
    assert res['data']['uuid'] == TO_VERSION_ID
    assert res['data']['page_uuid'] == PAGE_ID

    # Test relations
    res = cli.get_version(TO_VERSION_ID, include_change_from_previous=True,
                          include_change_from_earliest=True)
    assert 'change_from_previous' in res['data']
    assert 'change_from_earliest' in res['data']


@db_vcr.use_cassette()
def test_get_version_by_versionista_id():
    cli = Client(**AUTH)
    res = cli.get_version_by_versionista_id(VERSIONISTA_ID)
    assert res['data']['uuid'] == TO_VERSION_ID
    assert res['data']['page_uuid'] == PAGE_ID


@db_vcr.use_cassette()
def test_get_version_by_versionista_id_failure():
    cli = Client(**AUTH)
    with pytest.raises(ValueError):
        cli.get_version_by_versionista_id('__nonexistent__')


@db_vcr.use_cassette()
def test_add_version():
    cli = Client(**AUTH)
    cli.add_version(page_id=PAGE_ID, uuid=NEW_VERSION_ID,
                    capture_time=TIME,
                    body_url='http://example.com',
                    body_hash='hash_placeholder',
                    title='title_placeholder',
                    source_type='test')


@db_vcr.use_cassette()
def test_get_new_version():
    cli = Client(**AUTH)
    data = cli.get_version(NEW_VERSION_ID)['data']
    assert data['uuid'] == NEW_VERSION_ID
    assert data['page_uuid'] == PAGE_ID
    # Some floating-point error occurs in round-trip.
    epsilon = timedelta(seconds=0.001)
    assert data['capture_time'] - TIME < epsilon
    assert data['source_type'] == 'test'
    assert data['title'] == 'title_placeholder'


@db_vcr.use_cassette()
def test_add_versions():
    cli = Client(**AUTH)
    new_version_ids = [
        'd68c5521-0728-4098-96dd-e6330612f049',
        'db2932c4-413b-41f6-b73d-602faccf2f49',
        '4cfe3e9b-01b3-4a5f-bb45-e7657fc38849',
        'e1731130-569a-45a5-8db9-e58764e72049',
        '901feef4-91b8-4140-8dcc-a414f52bef49',
        '4cd662bc-e322-463e-9fe1-12fbccb62a49',
        '1d0e7eb7-4920-48b5-a810-d01e7ae27c49',
        '8b420ce3-ecc5-43e2-865a-b02c854f6449',
        'ae23d4f2-ab34-43da-b58f-57c4ab8bdd49',
        'b8cc3d0f-f2eb-43ef-bfc7-d0b589ee7f49']
    versions = [dict(uuid=version_id,
                     # Notice the importer needs page_url instead of page_id.
                     url='http://example.com',
                     capture_time=TIME,
                     body_url='http://example.com',
                     body_hash='hash_placeholder',
                     title='title_placeholder',
                     page_maintainers=['agency_placeholder'],
                     page_tags=['site:site_placeholder'],
                     source_type='test') for version_id in new_version_ids]
    # FIXME: need to spy on the data POSTed to DB and make sure the number of
    # lines matches the number of new_version_ids
    import_ids = cli.add_versions(versions, batch_size=5)
    global_stash['import_ids'] = import_ids


@db_vcr.use_cassette()
def test_get_import_status():
    cli = Client(**AUTH)
    import_id, *_ = global_stash['import_ids']
    result = cli.get_import_status(import_id)
    assert not result['data']['processing_errors']


@db_vcr.use_cassette()
def test_monitor_import_statuses():
    cli = Client(**AUTH)
    import_ids = global_stash['import_ids']
    errors = cli.monitor_import_statuses(import_ids)
    assert not errors


# NOTE: Even though this looks the same as the above test, the VCR fixture for
# this test includes an error.
@db_vcr.use_cassette()
def test_monitor_import_statuses_returns_errors():
    cli = Client(**AUTH)
    import_ids = global_stash['import_ids']
    errors = cli.monitor_import_statuses(import_ids)
    assert errors == {47: ["Row 2: Response body for 'http://example.com' did "
                           "not match expected hash (hash_placeholder)"]}


# DEPRECATED
@db_vcr.use_cassette()
def test_list_changes():
    cli = Client(**AUTH)
    # smoke test
    cli.list_changes(PAGE_ID)


@db_vcr.use_cassette()
def test_get_changes():
    cli = Client(**AUTH)
    # smoke test
    changes = cli.get_changes(PAGE_ID)
    first = next(changes)
    assert 'uuid_from' in first
    assert 'uuid_to' in first


@db_vcr.use_cassette()
def test_get_change():
    cli = Client(**AUTH)
    # smoke test
    cli.get_change(page_id=PAGE_ID,
                   to_version_id=TO_VERSION_ID)


# DEPRECATED
@db_vcr.use_cassette()
def test_list_annotations():
    cli = Client(**AUTH)
    # smoke test
    cli.list_annotations(page_id=PAGE_ID,
                         to_version_id=TO_VERSION_ID)


@db_vcr.use_cassette()
def test_get_annotations():
    cli = Client(**AUTH)
    # smoke test
    annotations = cli.get_annotations(page_id=PAGE_ID,
                                      to_version_id=TO_VERSION_ID)
    first = next(annotations)
    assert 'uuid' in first


@db_vcr.use_cassette()
def test_add_annotation():
    cli = Client(**AUTH)
    # smoke test
    annotation = {'foo': 'bar'}
    result = cli.add_annotation(annotation=annotation,
                                page_id=PAGE_ID,
                                to_version_id=TO_VERSION_ID)
    annotation_id = result['data']['uuid']
    global_stash['annotation_id'] = annotation_id


@db_vcr.use_cassette()
def test_get_annotation():
    cli = Client(**AUTH)
    annotation_id = global_stash['annotation_id']
    result = cli.get_annotation(annotation_id=annotation_id,
                                page_id=PAGE_ID,
                                to_version_id=TO_VERSION_ID)
    fetched_annotation = result['data']['annotation']
    annotation = {'foo': 'bar'}
    assert fetched_annotation == annotation


@db_vcr.use_cassette()
def test_get_user_session():
    cli = Client(**AUTH)
    session = cli.get_user_session()
    assert session['user']['email'] == AUTH['email']


@db_vcr.use_cassette()
def test_validate_credentials():
    cli = Client(**AUTH)
    cli.validate_credentials()


@db_vcr.use_cassette()
def test_validate_credentials_should_raise():
    bad_auth = AUTH.copy()
    bad_auth['password'] = 'BAD_PASSWORD'
    cli = Client(**bad_auth)
    with pytest.raises(UnauthorizedCredentials):
        cli.validate_credentials()


def test_retry_defaults():
    """
    This test is pretty minimal; it only checks that a correctly configured
    Retry object is making it into requests. The retries themselves happen down
    in urllib3, which is below the level at which requests-mock functions, and
    hand-coding a VCR cassette is not a great idea. So it's tough to get a
    better test.
    """
    client = Client(**AUTH)
    adapter = client._session.adapters['https://']
    assert DEFAULT_RETRIES == adapter.max_retries.total
    assert DEFAULT_BACKOFF == adapter.max_retries.backoff_factor


def test_retries_tuple():
    """
    This test is pretty minimal; it only checks that a correctly configured
    Retry object is making it into requests. The retries themselves happen down
    in urllib3, which is below the level at which requests-mock functions, and
    hand-coding a VCR cassette is not a great idea. So it's tough to get a
    better test.
    """
    hard_working_client = Client(**AUTH, retries=(8, 5))
    adapter = hard_working_client._session.adapters['https://']
    assert 8 == adapter.max_retries.total
    assert 5 == adapter.max_retries.backoff_factor


def test_retries_object():
    """
    This test is pretty minimal; it only checks that a correctly configured
    Retry object is making it into requests. The retries themselves happen down
    in urllib3, which is below the level at which requests-mock functions, and
    hand-coding a VCR cassette is not a great idea. So it's tough to get a
    better test.
    """
    fancy_retries = urllib3.util.Retry()
    fancy_client = Client(**AUTH, retries=fancy_retries)
    adapter = fancy_client._session.adapters['https://']
    assert fancy_retries == adapter.max_retries


@patch('web_monitoring.db.requests.Session.request')
def test_client_with_default_timeout(mock_request):
    cli = Client(**AUTH)
    cli.get_user_session()
    mock_request.assert_called_with(method='GET',
                                    url=f'{AUTH["url"]}/users/session',
                                    timeout=DEFAULT_TIMEOUT)


@patch('web_monitoring.db.requests.Session.request')
def test_client_with_custom_timeout(mock_request):
    cli = Client(**AUTH, timeout=7.5)
    cli.get_user_session()
    mock_request.assert_called_with(method='GET',
                                    url=f'{AUTH["url"]}/users/session',
                                    timeout=7.5)


@patch('web_monitoring.db.requests.Session.request')
def test_client_with_no_timeout(mock_request):
    cli = Client(**AUTH, timeout=0)
    cli.get_user_session()
    mock_request.assert_called_with(method='GET',
                                    url=f'{AUTH["url"]}/users/session',
                                    timeout=None)
