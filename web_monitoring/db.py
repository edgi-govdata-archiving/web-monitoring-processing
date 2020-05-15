# Functions for interacting with web-monitoring-db
from collections.abc import Sequence
import dateutil.tz
from dateutil.parser import parse as parse_timestamp
import json
import os
import requests
import requests.exceptions
import time
import toolz
import warnings


DEFAULT_URL = 'https://api.monitoring.envirodatagov.org'
DEFAULT_TIMEOUT = 30.5
GET = 'GET'
POST = 'POST'


def _tzaware_isoformat(dt):
    """Express a datetime object in timezone-aware ISO format."""
    if dt.tzinfo is None:
        # This is naive. Assume they mean this time in the local timezone.
        dt = dt.replace(tzinfo=dateutil.tz.gettz())
    return dt.isoformat()


class WebMonitoringDbError(Exception):
    ...


class UnauthorizedCredentials(Exception):
    ...


def _process_errors(res):
    # If the app gives us errors, raise a custom exception with those.
    # If not, fall back on requests, which will raise an HTTPError.
    if res.ok:
        return
    try:
        errors = res.json()['errors']
    except Exception:
        if res.status_code == 401:
            raise UnauthorizedCredentials('Unauthorized credentials for Web Wonitoring DB')
        else:
            res.raise_for_status()
    else:
        raise WebMonitoringDbError(', '.join(map(repr, errors)))


def _time_range_string(start_date, end_date):
    """
    Parameters
    ----------
    start_date : datetime or None
    end_date : datetime or None

    Returns
    -------
    capture_time_query : None or string
        If None, do not query ``capture_time``.
    """
    if start_date is None and end_date is None:
        return None
    if start_date is not None:
        start_str = _tzaware_isoformat(start_date)
    else:
        start_str = ''
    if end_date is not None:
        end_str = _tzaware_isoformat(end_date)
    else:
        end_str = ''
    return f'{start_str}..{end_str}'


def _build_version(*, page_id, uuid, capture_time, uri, hash, source_type,
                   title, source_metadata=None, media_type=None,
                   media_type_parameters=None):
    """
    Build a Version dict from parameters, performing some validation.
    """
    if not isinstance(capture_time, str):
        capture_time = _tzaware_isoformat(capture_time)
    if source_metadata is None:
        source_metadata = {}
    version = {'page_id': page_id,
               'uuid': uuid,
               'capture_time': capture_time,
               'uri': str(uri),
               'hash': str(hash),
               'source_type': str(source_type),
               'title': str(title),
               'source_metadata': source_metadata,
               'media_type': media_type,
               'media_type_parameters': media_type_parameters}
    return version


def _build_importable_version(*, page_url, uuid=None, capture_time, uri,
                              version_hash, source_type, title,
                              page_maintainers=None, page_tags=None,
                              source_metadata=None, status=None,
                              media_type=None, media_type_parameters=None):
    """
    Build a Version dict from parameters, performing some validation.

    This is different than _build_version because it needs ``page_url`` instead
    of ``page_id`` of an existing Page.
    """
    if not isinstance(capture_time, str):
        capture_time = _tzaware_isoformat(capture_time)
    if source_metadata is None:
        source_metadata = {}
    version = {'page_url': page_url,
               'uuid': uuid,
               'capture_time': capture_time,
               'uri': str(uri),
               'hash': str(version_hash),
               'source_type': str(source_type),
               'title': str(title),
               'source_metadata': source_metadata,
               'status': str(status),
               'page_maintainers': page_maintainers,
               'page_tags': page_tags,
               'media_type': media_type,
               'media_type_parameters': media_type_parameters}
    return version


def _validate_timeout(timeout, default=None):
    if timeout is None:
        return default
    elif timeout is not None and timeout < 0:
        raise ValueError(f'Timeout must be non-negative. "{timeout}" was provided.')
    elif timeout == 0:
        return None
    else:
        return timeout


class MissingCredentials(RuntimeError):
    ...


class Client:
    """
    Communicate with web-monitoring-db via its REST API.

    This object encapsulates authentication information and provides
    methods corresponding to the REST API.

    The Client can also be configured via environment variables using the
    class method :meth:`Client.from_env`.

    Parameters
    ----------
    email : string
    password : string
    url : string, optional
        Default is ``https://api.monitoring.envirodatagov.org``.
    timeout: float, optional
        A connection timeout in seconds to be used for all requests. `0` indicates 
        no timeout should be used. Individual method calls may override this value
        if `timeout` can be provided as an argument. The default value is 30.5 seconds.
    """
    def __init__(self, email, password, url=DEFAULT_URL, timeout=DEFAULT_TIMEOUT):
        self._api_url = f'{url}/api/v0'
        self._base_url = url
        self._session = requests.Session()
        self._session.auth = (email, password)
        self._session.headers.update({'accept': 'application/json'})

        self._timeout = _validate_timeout(timeout)

    @classmethod
    def from_env(cls, **kwargs):
        """
        Instantiate a :class:`Client` by obtaining its authentication info from
        these environment variables:

            * ``WEB_MONITORING_DB_URL`` (optional -- defaults to
              ``https://api.monitoring.envirodatagov.org``)
            * ``WEB_MONITORING_DB_EMAIL``
            * ``WEB_MONITORING_DB_PASSWORD``
        """
        try:
            url = os.environ.get('WEB_MONITORING_DB_URL', DEFAULT_URL)
            email = os.environ['WEB_MONITORING_DB_EMAIL']
            password = os.environ['WEB_MONITORING_DB_PASSWORD']
        except KeyError:
            raise MissingCredentials("""
Before using this method, database credentials must be set via environmental
variables:

   WEB_MONITORING_DB_URL (optional)
   WEB_MONITORING_DB_EMAIL
   WEB_MONITORING_DB_PASSWORD

Alternatively, you can instaniate Client(user, password) directly.""")
        return cls(email=email, password=password, url=url, **kwargs)

    def request(self, method, url, data=None, timeout=None, **kwargs):
        if not url.startswith('http://') and not url.startswith('https://'):
            url = f'{self._api_url}{url}'

        timeout = _validate_timeout(timeout, default=self._timeout)

        if data is not None:
            headers = kwargs.setdefault('headers', {})
            if isinstance(data, Sequence):
                headers.update({'Content-Type': 'application/x-json-stream'})
                kwargs['data'] = '\n'.join(map(json.dumps, data))
            else:
                headers.update({'Content-Type': 'application/json'})
                kwargs['data'] = json.dumps(data)
        response = self._session.request(method=method, 
                                         url=url,
                                         timeout=timeout,
                                         **kwargs)
        _process_errors(response)
        return response

    def request_json(self, method, url, data=None, timeout=None, **kwargs):
        response = self.request(method, url, data, timeout, **kwargs)
        return response.json()

    ### PAGES ###

    def list_pages(self, *, chunk=None, chunk_size=None, sort=None,
                   tags=None, maintainers=None, url=None, title=None,
                   include_versions=None, include_earliest=None,
                   include_latest=None, source_type=None, hash=None,
                   start_date=None, end_date=None, active=None,
                   include_total=False):
        """
        List all Pages, optionally filtered by search criteria.

        Parameters
        ----------
        chunk : integer, optional
            pagination parameter
        chunk_size : integer, optional
            number of items per chunk
        sort : list of string, optional
            fields to sort by in `{field}:{order}` format, e.g. `title:asc`
        tags : list of string, optional
        maintainers : list of string, optional
        url : string, optional
        title : string, optional
        include_versions : boolean, optional
        include_earliest : boolean, optional
        include_latest : boolean, optional
        source_type : string, optional
            such as 'versionista' or 'internet_archive'
        hash : string, optional
            SHA256 hash of Version content
        start_date : datetime, optional
        end_date : datetime, optional
        active : boolean, optional
        include_total : boolean, optional
            Whether to include a `meta.total_results` field in the response.
            If not set, `links.last` will usually be empty unless you are on
            the last chunk. Setting this option runs a pretty expensive query,
            so use it sparingly. (Default: False)

        Returns
        -------
        response : dict
        """
        params = {'chunk': chunk,
                  'chunk_size': chunk_size,
                  'sort': sort and ','.join(sort) or None,
                  'tags[]': tags,
                  'maintainers[]': maintainers,
                  'url': url,
                  'title': title,
                  'include_versions': include_versions,
                  'include_earliest': include_earliest,
                  'include_latest': include_latest,
                  'source_type': source_type,
                  'hash': hash,
                  'capture_time': _time_range_string(start_date, end_date),
                  'active': active,
                  'include_total': include_total or None}
        url = '/pages'
        result = self.request_json(GET, url, params=params)
        data = result['data']
        # In place, replace datetime strings with datetime objects.
        for page in data:
            page['created_at'] = parse_timestamp(page['created_at'])
            page['updated_at'] = parse_timestamp(page['updated_at'])
            if 'earliest' in page:
                page['earliest']['capture_time'] = parse_timestamp(
                    page['earliest']['capture_time'])
                page['earliest']['created_at'] = parse_timestamp(
                    page['earliest']['created_at'])
                page['earliest']['updated_at'] = parse_timestamp(
                    page['earliest']['updated_at'])
            if 'latest' in page:
                page['latest']['capture_time'] = parse_timestamp(
                    page['latest']['capture_time'])
                page['latest']['created_at'] = parse_timestamp(
                    page['latest']['created_at'])
                page['latest']['updated_at'] = parse_timestamp(
                    page['latest']['updated_at'])
            if 'versions' in page:
                for v in page['versions']:
                    v['created_at'] = parse_timestamp(v['created_at'])
                    v['updated_at'] = parse_timestamp(v['updated_at'])
                    v['capture_time'] = parse_timestamp(v['capture_time'])
        return result

    def get_page(self, page_id):
        """
        Lookup a specific Page by ID.

        Parameters
        ----------
        page_id : string

        Returns
        -------
        response : dict
        """
        url = f'/pages/{page_id}'
        result = self.request_json(GET, url)
        # In place, replace datetime strings with datetime objects.
        data = result['data']
        data['created_at'] = parse_timestamp(data['created_at'])
        data['updated_at'] = parse_timestamp(data['updated_at'])
        for v in data['versions']:
            v['created_at'] = parse_timestamp(v['created_at'])
            v['updated_at'] = parse_timestamp(v['updated_at'])
            v['capture_time'] = parse_timestamp(v['capture_time'])
        return result


    ### VERSIONS ###

    def list_versions(self, *, page_id=None, chunk=None, chunk_size=None,
                      sort=None, start_date=None, end_date=None,
                      source_type=None, hash=None,
                      source_metadata=None, different=None,
                      include_change_from_previous=None,
                      include_change_from_earliest=None, include_total=False):
        """
        List Versions, optionally filtered by serach criteria, including Page.

        Parameters
        ----------
        page_id : string, optional
            restricts serach to Versions of a specific Page
        chunk : integer, optional
            pagination parameter
        chunk_size : integer, optional
            number of items per chunk
        sort : list of string, optional
            fields to sort by in `{field}:{order}` format,
            e.g. `capture_time:asc`
        start_date : datetime, optional
        end_date : datetime, optional
        source_type : string, optional
            such as 'versionista' or 'internetarchive'
        hash : string, optional
            SHA256 hash of Version content
        source_metadata : dict, optional
            Examples:

            * ``{'version_id': 12345678}``
            * ``{'account': 'versionista1', 'has_content': True}``
        different : boolean, optional
            If False, include versions that aren't actually different from the
            previous version of the same page in the response.
        include_change_from_previous : boolean, optional
            If True, include a `change_from_previous` field in each version
            that represents a change object between it and the previous version
            of the same page.
        include_change_from_earliest : boolean, optional
            If True, include a `change_from_earliest` field in each version
            that represents a change object between it and the earliest version
            of the same page.
        include_total : boolean, optional
            Whether to include a `meta.total_results` field in the response.
            If not set, `links.last` will usually be empty unless you are on
            the last chunk. Setting this option runs a pretty expensive query,
            so use it sparingly. (Default: False)

        Returns
        -------
        response : dict
        """
        params = {'chunk': chunk,
                  'chunk_size': chunk_size,
                  'sort': sort and ','.join(sort) or None,
                  'capture_time': _time_range_string(start_date, end_date),
                  'source_type': source_type,
                  'hash': hash,
                  'different': different,
                  'include_change_from_previous': include_change_from_previous,
                  'include_change_from_earliest': include_change_from_earliest,
                  'include_total': include_total or None}
        if source_metadata is not None:
            for k, v in source_metadata.items():
                params[f'source_metadata[{k}]'] = v
        if page_id is None:
            url = '/versions'
        else:
            url = f'/pages/{page_id}/versions'
        result = self.request_json(GET, url, params=params)
        # In place, replace datetime strings with datetime objects.
        for v in result['data']:
            v['created_at'] = parse_timestamp(v['created_at'])
            v['updated_at'] = parse_timestamp(v['updated_at'])
            v['capture_time'] = parse_timestamp(v['capture_time'])
        return result

    def get_version(self, version_id, include_change_from_previous=None,
                    include_change_from_earliest=None):
        """
        Lookup a specific Version by ID.

        Parameters
        ----------
        version_id : string
        include_change_from_previous : boolean, optional
            If True, include a `change_from_previous` field in that represents
            a change object between this and the previous version of the same
            page.
        include_change_from_earliest : boolean, optional
            If True, include a `change_from_earliest` field in that represents
            a change object between this and the earliest version of the same
            page.

        Returns
        -------
        response : dict
        """
        url = f'/versions/{version_id}'
        params = {'include_change_from_previous': include_change_from_previous,
                  'include_change_from_earliest': include_change_from_earliest}
        result = self.request_json(GET, url, params=params)
        data = result['data']
        data['capture_time'] = parse_timestamp(data['capture_time'])
        data['updated_at'] = parse_timestamp(data['updated_at'])
        data['created_at'] = parse_timestamp(data['created_at'])
        return result

    def add_version(self, *, page_id, capture_time, uri, hash,
                    source_type, title, uuid=None, source_metadata=None):
        """
        Submit one new Version.

        See :func:`add_versions` for a more efficient bulk importer.

        Parameters
        ----------
        page_id : string
            Page to which the Version is associated
        uri : string
            URI of content (such as an S3 bucket or InternetArchive URL)
        hash : string
            SHA256 hash of Version content
        source_type : string
            such as 'versionista' or 'internetarchive'
        title : string
            content of ``<title>`` tag
        uuid : string, optional
            A new, unique Version ID (UUID4). If not specified, the server
            will generate one.
        source_metadata : dict, optional
            free-form metadata blob provided by source

        Returns
        -------
        response : dict
        """
        # Do some type casting here as gentle error-checking.
        version = _build_version(
            page_id=page_id,
            uuid=uuid,
            capture_time=capture_time,
            uri=uri,
            hash=hash,
            source_type=source_type,
            title=title,
            source_metadata=source_metadata)
        url = f'/pages/{page_id}/versions'
        return self.request_json(POST, url, data=version)

    def add_versions(self, versions, *, update='skip', create_pages=None,
                     skip_unchanged_versions=None, batch_size=1000):
        """
        Submit versions in bulk for importing into web-monitoring-db.

        Chunk the versions into batches of at most the given size.

        Parameters
        ----------
        versions : iterable
            Iterable of dicts from :func:`format_version`
        update : {'skip', 'replace', 'merge'}, optional
            Specifies how versions that are already in the database (i.e.
            versions with the same ``capture_time`` and ``source_type``) should
            be handled:

                * ``'skip'`` (default) -- Don’t import the version or modify
                  the existing database entry.
                * ``'replace'`` -- Replace the existing database entry with the
                  imported one
                * ``'merge'`` -- Similar to `replace`, but merges the values in
                  ``source_metadata``

        create_pages : bool, optional
            If True, create new pages for any URLs in the import set that don't
            already exist.
        skip_unchanged_versions : bool, optional
            If true, don't import versions of a page that have the same hash as
            the version captured immediately before them.
        batch_size : integer, optional
            Default batch size is 1000 Versions.

        Returns
        -------
        import_ids : tuple
        """
        url = '/imports'
        # POST to the server in chunks. Stash the import id from each response.
        import_ids = []
        for batch in toolz.partition_all(batch_size, versions):
            # versions might be a generator. This comprehension will pull on it
            validated_versions = [_build_importable_version(**v)
                                  for v in batch]

            params = {'update': update, 'create_pages': create_pages,
                      'skip_unchanged_versions': skip_unchanged_versions}
            params = {k: v if isinstance(v, str) else str(v).lower()
                      for k, v in params.items() if v is not None}
            result = self.request_json(POST,
                                       url,
                                       data=validated_versions,
                                       params=params)
            import_id = result['data']['id']
            import_ids.append(import_id)
        return tuple(import_ids)

    # TODO: we probably need to change the return value here to support info
    # about imports that didn't finish if we stopped early. May also want an
    # optional timeout given that we get stuck sometimes if the DB drops an
    # import off its queue (which is a problem that also needs solving in DB).
    def monitor_import_statuses(self, import_ids, stop=None):
        """
        Poll status of Version import jobs until all complete.

        Use Ctrl+C to exit early. A dict mapping the import IDs to any errors
        from those imports (so far) will be returned.

        Parameters
        ----------
        import_ids : iterable of (str or int)
        stop : threading.Event, optional
            A threading.Event to monitor in order to determine whether to stop
            monitoring before all imports are complete.

        Returns
        -------
        errors : dict of {str or int : list}
        """
        errors = {}
        import_ids = list(import_ids)  # to ensure mutable collection
        try:
            while import_ids and (stop is None or not stop.is_set()):
                for import_id in tuple(import_ids):
                    # We are mainly interested in processing errors. We don't
                    # expect HTTPErrors, so we'll just warn and hope that
                    # everything works in the second pass.
                    try:
                        result = self.get_import_status(import_id)
                    except requests.exceptions.HTTPError as exc:
                        warnings.warn("Ignoring Exception: {}".format(exc))
                        continue
                    data = result['data']
                    if data['status'] == 'complete':
                        import_ids.remove(import_id)
                        job_errors = data['processing_errors']
                        if job_errors:
                            errors[import_id] = job_errors
                time.sleep(1)
        except KeyboardInterrupt:
            ...
        return errors

    def get_import_status(self, import_id):
        """
        Check on the status of a batch Version import job.

        Parameters
        ----------
        import_id : integer

        Returns
        -------
        response : dict
        """
        url = f'/imports/{import_id}'
        return self.request_json(GET, url)

    ### CHANGES AND ANNOTATIONS ###

    def list_changes(self, page_id, include_total=False):
        """
        List Changes between two Versions on a Page.

        Parameters
        ----------
        page_id : string
        include_total : boolean, optional
            Whether to include a `meta.total_results` field in the response.
            If not set, `links.last` will usually be empty unless you are on
            the last chunk. Setting this option runs a pretty expensive query,
            so use it sparingly. (Default: False)

        Returns
        -------
        response : dict
        """
        url = f'/pages/{page_id}/changes/'
        result = self.request_json(
            GET, url, params={'include_total': include_total or None})
        # In place, replace datetime strings with datetime objects.
        for change in result['data']:
            change['created_at'] = parse_timestamp(change['created_at'])
            change['updated_at'] = parse_timestamp(change['updated_at'])
        return result

    def get_change(self, *, page_id, to_version_id, from_version_id=''):
        """
        Get a Changes between two Versions.

        Parameters
        ----------
        page_id : string
        to_version_id : string
        from_version_id : string, optional
            If from_version_id is not given, it will be treated as version
            immediately prior to ``to_version``.

        Returns
        -------
        response : dict
        """
        url = (f'/pages/{page_id}/changes/'
               f'{from_version_id}..{to_version_id}')
        result = self.request_json(GET, url)
        # In place, replace datetime strings with datetime objects.
        data = result['data']
        # For changes which were created just-in-time to fulfill this API request,
        # created/updated_at will be None, and that's OK.
        if data['created_at'] and data['updated_at']:
            data['created_at'] = parse_timestamp(data['created_at'])
            data['updated_at'] = parse_timestamp(data['updated_at'])
        return result

    def list_annotations(self, *, page_id, to_version_id, from_version_id='',
                         include_total=False):
        """
        List Annotations for a Change between two Versions.

        Parameters
        ----------
        page_id : string
        to_version_id : string
        from_version_id : string, optional
            If from_version_id is not given, it will be treated as version
            immediately prior to ``to_version``.
        include_total : boolean, optional
            Whether to include a `meta.total_results` field in the response.
            If not set, `links.last` will usually be empty unless you are on
            the last chunk. Setting this option runs a pretty expensive query,
            so use it sparingly. (Default: False)

        Returns
        -------
        response : dict
        """
        url = (f'/pages/{page_id}/changes/'
               f'{from_version_id}..{to_version_id}/annotations')
        result = self.request_json(
            GET, url, params={'include_total': include_total or None})
        # In place, replace datetime strings with datetime objects.
        for a in result['data']:
            a['created_at'] = parse_timestamp(a['created_at'])
            a['updated_at'] = parse_timestamp(a['updated_at'])
        return result

    def add_annotation(self, *, annotation, page_id, to_version_id,
                       from_version_id=''):
        """
        Submit updated annotations for a change between versions.

        Parameters
        ----------
        annotation : dict
        page_id : string
        to_version_id : string
        from_version_id : string, optional
            If from_version_id is not given, it will be treated as version
            immediately prior to ``to_version``.

        Returns
        -------
        response : dict
        """
        url = (f'/pages/{page_id}/changes/'
               f'{from_version_id}..{to_version_id}/annotations')
        return self.request_json(POST, url, data=annotation)

    def get_annotation(self, *, annotation_id, page_id, to_version_id,
                       from_version_id=''):
        """
        Get a specific Annontation.

        Parameters
        ----------
        annotation_id : string
        page_id : string
        to_version_id : string
        from_version_id : string, optional
            If from_version_id is not given, it will be treated as version
            immediately prior to ``to_version``.

        Returns
        -------
        response : dict
        """
        url = (f'/pages/{page_id}/changes/'
               f'{from_version_id}..{to_version_id}/annotations/'
               f'{annotation_id}')
        result = self.request_json(GET, url)
        # In place, replace datetime strings with datetime objects.
        data = result['data']
        data['created_at'] = parse_timestamp(data['created_at'])
        data['updated_at'] = parse_timestamp(data['updated_at'])
        return result

    ### USERS ###

    def get_user_session(self):
        """
        Get the current user session.

        Returns
        -------
        response : dict
        """
        user_session_url = f'{self._base_url}/users/session'
        return self.request_json(GET, user_session_url)

    ### CONVENIENCE METHODS ###

    def get_version_content(self, version_id):
        """
        Download the saved content from a given Version.

        Parameters
        ----------
        version_id : string

        Returns
        -------
        content : bytes
        """
        db_result = self.get_version(version_id)
        content_uri = db_result['data']['uri']
        # override the session-level "accept: json" header
        response = self.request(GET, content_uri, headers={'accept': None})
        if response.headers.get('Content-Type', '').startswith('text/'):
            return response.text
        else:
            return response.content

    def get_version_by_versionista_id(self, versionista_id):
        """
        Look up a Version by its Verisonista-issued ID.

        This is a convenience method for dealing with Versions ingested from
        Versionista.

        Parameters
        ----------
        versionista_id : string

        Returns
        -------
        response : dict
        """
        result = self.list_versions(
            source_type='versionista',
            source_metadata={'version_id': versionista_id})
        # There should not be more than one match.
        data = result['data']
        if len(data) == 0:
            raise ValueError("No match found for versionista_id {}"
                             "".format(versionista_id))
        elif len(data) > 1:
            raise Exception("Multiple Versions match the versionista_id {}."
                            "Their web-monitoring-db version_ids are: {}"
                            "".format(versionista_id,
                                      [v['uuid'] for v in data]))
        # Make result look like the result of `get_version` rather than the
        # result of `list_versions`.
        return {'data': result['data'][0]}

    def validate_credentials(self):
        """
        Validate that the DB Client is authorized for the provided host.
        This function raises an exception if the credentials are invalid, so
        it's intended to be used like an assert statement.

        Raises
        ------
        UnauthorizedCredentials
            If the credentials are not authorized for the provided host.
        """
        self.get_user_session()
