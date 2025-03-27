"""
Command-Line Tools for loading data from the Wayback Machine and importing it
into web-monitoring-db

See the `scripts/` directory for the associated executable(s). Most of the
logic is implemented in this module to make it easier to test or reuse.

There is a lot of asynchronous, thread-based logic here to make sure large
import jobs can be performed efficiently, making as many parallel network
requests as Wayback and your local machine will comfortably support. The
general data flow looks something like:

   (start here)         (or here)
 ┌──────────────┐   ┌──────────────┐
 │ Create list  │   │ Load list of │
 │ of arbitrary │   │  known URLs  │
 │    URLs      │   │   from API   │
 └──────────────┘   └──────────────┘
        ├───────────────────┘
        │
 ┌ ─ ─ ─┼─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (in parallel) ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
 ┊      │                                                                      ┊
 ┊ ┌──────────┐  ┌─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐  ┌─────────┐ ┊
 ┊ │ Load CDX │  ┊ ┌────────────┐ ┌────────────┐ ┌────────────┐ ┊  │Summarize│ ┊
 ┊ │ records  │  ┊ │Load memento│ │Load memento│ │Load memento│ ┊  │ results │ ┊
 ┊ │ for URLs │  ┊ └────────────┘ └────────────┘ └────────────┘ ┊  │   and   │ ┊
 ┊ │          │  ┊    ├─────────────────┴──────────────┘        ┊  │  errors │ ┊
 ┊ └──────────┘  └─ ─ ┼ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  └─────────┘ ┊
 ┊      ↓             ↑                                  ↓           ↑   │     ┊
 ┊      └── (queue) ──┘                                  └─ (queue) ─┘   │     ┊
 ┊                                                                       │     ┊
 ┊                                              ┌────────┐      ┌────────────┐ ┊
 ┊                                              │ Import │←─────│   Filter   │ ┊
 ┊                                              │ to DB  │      │ out errors │ ┊
 ┊                                              └────────┘      └────────────┘ ┊
 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┼─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
                                                    │
                                                  Done!

Each box represents a thread. Instances of `FiniteQueue` are used to move data
and results between them.
"""

from cloudpathlib import CloudPath
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from web_monitoring.utils import cli_datetime, detect_encoding
import dateutil.parser
from docopt import docopt
from itertools import islice
import json
import logging
import math
import os
from os.path import splitext
from pathlib import Path
import re
import requests
import sentry_sdk
import threading
import time
from tqdm import tqdm
from urllib.parse import urlparse
from web_monitoring import db
import wayback
from wayback.exceptions import (WaybackException, WaybackRetryError,
                                MementoPlaybackError, NoMementoError,
                                BlockedByRobotsError)
from web_monitoring.media import (HTML_MEDIA_TYPES, PDF_MEDIA_TYPES,
                                  find_media_type)
from web_monitoring import utils, __version__


logger = logging.getLogger(__name__)

# User agent for requests to Wayback
USER_AGENT = f'edgi.web_monitoring.WaybackClient/{__version__}'

# Number of memento requests to make at once. Can be overridden via CLI args.
PARALLEL_REQUESTS = 10

# Matches the host segment of a URL.
HOST_EXPRESSION = re.compile(r'^[^:]+://([^/]+)')
# Matches URLs for "index" pages that are likely to be the same as a URL ending
# with a slash. e.g. matches the `index.html` in `https://epa.gov/index.html`.
# Used to group URLs representing the same logical page.
INDEX_PAGE_EXPRESSION = re.compile(r'index(\.\w+)?$')
# MIME types that we always consider to be subresources and never "pages".
SUBRESOURCE_MIME_TYPES = (
    'text/css',
    'text/javascript',
    'application/javascript',
    'image/jpeg',
    'image/webp',
    'image/png',
    'image/gif',
    'image/bmp',
    'image/tiff',
    'image/x-icon',
)
# Extensions that we always consider to be subresources and never "pages".
SUBRESOURCE_EXTENSIONS = (
    '.css',
    '.js',
    '.es',
    '.es6',
    '.jsm',
    '.jpg',
    '.jpeg',
    '.webp',
    '.png',
    '.gif',
    '.bmp',
    '.tif',
    '.ico',
)
# Never query CDX for *all* snapshots at any of these domains (instead, always
# query for each specific URL we want). This is usually because we assume these
# domains are HUGE and completely impractical to query all pages on.
NEVER_QUERY_DOMAINS = (
    'instagram.com',
    'youtube.com',
    'amazon.com'
)
# Query an entire domain for snapshots if we are interested in more than this
# many URLs in the domain (NEVER_QUERY_DOMAINS above overrides this).
# NOTE: this is intentionally set high enough that we are unlikely to ever
# reach this threshold -- it turns out the CDX API doesn't always return all
# pages when using domain/prefix queries (some indexes are excluded from those
# queries, but it also looks like there are some bugs preventing other mementos
# from being included), so until that gets resolved (maybe never?), this makes
# sure we query for ever page individually.
MAX_QUERY_URLS_PER_DOMAIN = 30_000

try:
    WAYBACK_RATE_LIMIT = int(os.getenv('WAYBACK_RATE_LIMIT'))
except Exception:
    WAYBACK_RATE_LIMIT = 10


# These functions lump together library code into monolithic operations for the
# CLI. They also print. To access this functionality programmatically, it is
# better to use the underlying library code.

def _add_and_monitor(versions, create_pages=True, skip_unchanged_versions=True, stop_event=None, db_client=None):
    cli = db_client or db.Client.from_env()  # will raise if env vars not set
    import_ids = cli.add_versions(versions, create_pages=create_pages,
                                  skip_unchanged_versions=skip_unchanged_versions)
    if len(import_ids) == 0:
        return

    print('Import job IDs: {}'.format(import_ids))
    print('Polling web-monitoring-db until import jobs are finished...')
    errors = cli.monitor_import_statuses(import_ids, stop_event)
    total = sum(len(job_errors) for job_errors in errors.values())
    if total > 0:
        print('Import job errors:')
        for job_id, job_errors in errors.items():
            print(f'  {job_id}: {len(job_errors):>3} errors {job_errors}')
        print(f'  Total: {total:>3} errors')


def _log_adds(versions):
    for version in versions:
        print(json.dumps(version))


class ExistingVersionError(Exception):
    """
    Indicates that a CDX record represented a version that's already been
    imported and so the memento was skipped.
    """
    ...


# HACK: Ensure WaybackSession retries all ConnectionErrors, since we are
# clearly being too narrow about them right now.
wayback.WaybackSession.retryable_errors = wayback.WaybackSession.handleable_errors


class RateLimitedAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, *args, requests_per_second=0, **kwargs):
        self._rate_limit = utils.RateLimit(requests_per_second)
        return super().__init__(*args, **kwargs)

    def send(self, *args, **kwargs):
        with self._rate_limit:
            return super().send(*args, **kwargs)


class CustomAdapterSession(wayback.WaybackSession):
    """
    CustomAdapterSession is a WaybackSession with a user-supplied HTTP adapter.
    (HTTP adapters in requests are the main interface between requests's
    high-level concepts and urllib3's ConnectionManager, which pools sets of
    connections together).

    We use this here to provide a single adapter for use across a number of
    parallel sessions in order to more safely and reliably manage the number
    of connections that get opened to Wayback. This makes for a HUGE
    improvement in performance and error rates. (NOTE: requests is not thread-
    safe, which is why we don't just share a single WaybackSession or
    WaybackClient across threads. Even sharing the HTTPAdapter instance is
    not really a good idea, but a quick read of the source makes this seem ok.)
    """
    def __init__(self, *args, adapter=None, **kwargs):
        super().__init__(*args, **kwargs)
        # Close existing adapters (requests.Session automatically creates new
        # adapters as part of __init__, so the only way to override is to drop
        # the built-in ones first).
        for built_in_adapter in self.adapters.values():
            built_in_adapter.close()
        self.mount('https://', adapter or requests.adapters.HTTPAdapter())
        self.mount('http://', adapter or requests.adapters.HTTPAdapter())

    def reset(self, *args, **kwargs):
        pass


class RequestStatistics:
    """
    Track basic timing statistics around HTTP requests. Creates a histogram
    with 10-second buckets counting the number of requests, and tracks the
    longest N requests for later reporting.

    Parameters
    ----------
    leaderboard_size : int
        How many of the longest requests to track details for, as opposed to
        just counting in the histogram (e.g. the 10 longest).
        (default: 10)
    leaderboard_min : float
        Minimum time (in seconds) for a request to be included on the
        leaderboard. (default: 0)
    """
    def __init__(self, leaderboard_size=10, leaderboard_min=0):
        self._lock = threading.Lock()
        self.histogram = defaultdict(int)
        self.leaderboard_size = leaderboard_size
        self.leaderboard = []
        self.leaderboard_min = leaderboard_min

    def record_time(self, url, time):
        bucket = math.floor(time / 10)
        with self._lock:
            self.histogram[bucket] += 1
            if time > self.leaderboard_min:
                self.leaderboard.append((time, url))
                size = len(self.leaderboard)
                if size > self.leaderboard_size:
                    self.leaderboard = sorted(self.leaderboard,
                                              key=lambda x: x[0])[1:]
                    self.leaderboard_min = self.leaderboard[0][0]


# TODO: this probably shouldn't be global/static, but it's expedient for now.
# Ideally this is incorporated into our shared HTTP adapter/session.
MEMENTO_STATISTICS = RequestStatistics(leaderboard_size=10, leaderboard_min=10)


class WaybackRecordsWorker(threading.Thread):
    """
    WaybackRecordsWorker is a thread that takes CDX records from a queue and
    loads the corresponding mementos from Wayback. It then transforms the
    mementos into Web Monitoring import records and emits them on another
    queue. If a `failure_queue` is provided, records that fail to load in a way
    that might be worth retrying are emitted on that queue.
    """

    def __init__(self, records, results_queue, maintainers, tags, cancel,
                 failure_queue=None, session_options=None, adapter=None,
                 unplaybackable=None, version_cache=None, archive_storage=None):
        super().__init__()
        self.results_queue = results_queue
        self.failure_queue = failure_queue
        self.cancel = cancel
        self.records = records
        self.maintainers = maintainers
        self.tags = tags
        self.unplaybackable = unplaybackable
        self.version_cache = version_cache or set()
        self.adapter = adapter
        session_options = session_options or dict(retries=3, backoff=2,
                                                  timeout=45)
        session = CustomAdapterSession(adapter=adapter,
                                       user_agent=USER_AGENT,
                                       **session_options)
        self.wayback = wayback.WaybackClient(session=session)
        self.archive_storage = archive_storage

    def is_active(self):
        return not self.cancel.is_set()

    def run(self):
        """
        Work through the queue of CDX records to load them from Wayback,
        transform them to Web Monitoring DB import entries, and queue them for
        importing.
        """
        while self.is_active():
            try:
                record = next(self.records)
            except StopIteration:
                break

            self.handle_record(record)

        # Only close the client if it's using an adapter we created, instead of
        # one some other piece of code owns.
        if not self.adapter:
            self.wayback.close()

    def handle_record(self, record):
        """
        Handle a single CDX record.
        """
        # Check whether we already have this memento and bail out.
        if _version_cache_key(record.timestamp, record.url) in self.version_cache:
            self.results_queue.put([record, None, ExistingVersionError(f'Skipped {record.raw_url}')])
            return
        # Check for whether we already know this can't be played and bail out.
        if self.unplaybackable is not None and record.raw_url in self.unplaybackable:
            self.results_queue.put([record, None, MementoPlaybackError(f'Skipped {record.raw_url}')])
            return

        start_time = time.time()
        try:
            version = self.process_record(record)
            self.results_queue.put([record, version, None])
        except MementoPlaybackError as error:
            if self.unplaybackable is not None:
                self.unplaybackable[record.raw_url] = datetime.utcnow()
            self.results_queue.put([record, None, error])
        except Exception as error:
            self.results_queue.put([record, None, error])
        finally:
            MEMENTO_STATISTICS.record_time(record.raw_url, time.time() - start_time)

    def process_record(self, record):
        """
        Load the actual Wayback memento for a CDX record and transform it to
        a Web Monitoring DB import record.
        """
        memento = self.wayback.get_memento(record, exact_redirects=False)
        with memento:
            version = self.format_memento(memento, record, self.maintainers,
                                          self.tags)
            if self.archive_storage and version['body_hash']:
                url = self.archive_storage.store(
                    memento.content,
                    hash=version['body_hash'],
                    content_type=version['media_type']
                )
                version['body_url'] = url

            return version

    def format_memento(self, memento, cdx_record, maintainers, tags):
        """
        Format a Wayback Memento response as a dict with import-ready info.
        """
        iso_date = cdx_record.timestamp.isoformat()
        # Use compact representation for UTC
        if iso_date.endswith('+00:00'):
            no_tz_date = iso_date.split("+", 1)[0]
            iso_date = f'{no_tz_date}Z'

        metadata = {
            'view_url': cdx_record.view_url
        }

        # If there were redirects, list every URL in the chain of requests.
        if memento.url != cdx_record.url:
            metadata['redirected_url'] = memento.url
            metadata['redirects'] = [
                *map(lambda item: item.url, memento.history),
                memento.url
            ]

        media_type, _ = find_media_type(memento.headers, memento.content,
                                        url=cdx_record.url)

        title = ''
        if media_type in HTML_MEDIA_TYPES:
            encoding = detect_encoding(memento.content, memento.headers)
            title = utils.extract_html_title(memento.content, encoding)
        # FIXME: remove the sniffing here; I think this is an anachronism from
        # before we had better sniffing support when setting `media_type`.
        elif media_type in PDF_MEDIA_TYPES or memento.content.startswith(b'%PDF-'):
            title = utils.extract_pdf_title(memento.content) or title

        return dict(
            # Page-level info
            url=cdx_record.url,
            page_maintainers=maintainers,
            page_tags=tags,
            title=title,

            # Version/memento-level info
            capture_time=iso_date,
            body_url=cdx_record.raw_url,
            media_type=media_type or None,
            content_length=len(memento.content),
            body_hash=utils.hash_content(memento.content),
            source_type='internet_archive',
            source_metadata=metadata,
            status=memento.status_code,
            headers=dict(memento.headers),
        )

    @classmethod
    def parallel(cls, count, records, results_queue, *args, **kwargs):
        """
        Run several `WaybackRecordsWorker` instances in parallel. When this
        returns, the workers will have finished running.

        Parameters
        ----------
        count: int
            Number of instances to run in parallel.
        records: web_monitoring.utils.FiniteQueue
            Queue of CDX records to load mementos for.
        results_queue: web_monitoring.utils.FiniteQueue
            Queue to place resulting import records onto.
        *args
            Arguments to pass to each instance.
        **kwargs
            Keyword arguments to pass to each instance.

        Returns
        -------
        list of WaybackRecordsWorker
        """
        # Use a shared adapter across workers to help manage HTTP connections
        # to Wayback. We've had real problems with overdoing on connections
        # across threads before, and using a single adapter with a limited
        # number of connections (you have set both `pool_maxsize` *and*
        # `pool_block` to actually have a limit) lets us do this fairly well.
        #
        # NOTE: Requests is not thread-safe (urllib3 is), so this is not
        # perfect. However, the surface area of HTTPAdapter is fairly small
        # relative to the rest of requests, and a quick review of the code
        # looks like this should be ok. We haven't seen issues yet.
        adapter = RateLimitedAdapter(requests_per_second=WAYBACK_RATE_LIMIT,
                                     pool_maxsize=count,
                                     pool_block=True)
        kwargs.setdefault('adapter', adapter)
        workers = []
        for i in range(count):
            worker = cls(records, results_queue, *args, **kwargs)
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()

        results_queue.end()
        adapter.close()
        return workers


def _filter_and_summarize_mementos(memento_info, summary):
    summary.update({'total': 0, 'success': 0, 'already_known': 0,
                    'playback': 0, 'missing': 0, 'unknown': 0})
    for cdx, memento, error in memento_info:
        summary['total'] += 1
        if memento:
            summary['success'] += 1
            yield memento
        elif isinstance(error, ExistingVersionError):
            summary['already_known'] += 1
        elif isinstance(error, NoMementoError):
            logger.info(f'  Missing memento: {cdx.raw_url}')
            summary['missing'] += 1
        elif isinstance(error, MementoPlaybackError):
            summary['playback'] += 1
            # Playback errors are not unusual or exceptional for us, so log
            # only at debug level. The Wayback Machine marks some mementos as
            # unplaybackable when there are many of them in a short timeframe
            # in order to increase cache efficiency (the assumption they make
            # here is that the mementos are likely the same). Since we are
            # looking at highly monitored, public URLs, we hit this case a lot.
            logger.debug(f'  {error}')
        elif isinstance(error, WaybackRetryError):
            logger.info(f'  {error}; URL: {cdx.raw_url}')
            summary['unknown'] += 1
        elif isinstance(error, Exception):
            # FIXME: getting read timed out connection errors here...
            # requests.exceptions.ConnectionError: HTTPConnectionPool(host='web.archive.org', port=80): Read timed out.
            # TODO: don't count or log (well, maybe DEBUG log) if failure_queue
            # is present and we are ultimately going to retry.
            logger.exception(f'  {error!r}; URL: {cdx.raw_url}')
            summary['unknown'] += 1
        else:
            logger.error(f'Expected mementos and errors, but got {type(error)} for {cdx.raw_url}: {error}')
            summary['unknown'] += 1

    # Add percentage calculations to summary
    if summary['total']:
        summary.update({f'{k}_pct': 100 * v / summary['total']
                        for k, v in summary.items()
                        if k != 'total' and not k.endswith('_pct')})
    else:
        summary.update({f'{k}_pct': 0.0
                        for k, v in summary.items()
                        if k != 'total' and not k.endswith('_pct')})


def _version_cache_key(time, url):
    utc_time = time.astimezone(timezone.utc)
    return f'{utc_time.strftime("%Y%m%d%H%M%S")}|{url}'


def _load_known_versions(client, start_date, end_date):
    print('Pre-checking known versions...', flush=True)

    versions = client.get_versions(start_date=start_date,
                                   end_date=end_date,
                                   different=False,  # Get *every* record
                                   sort=['capture_time:desc'],
                                   chunk_size=1000)
    # Limit to latest 500,000 results for sanity/time/memory
    limited_versions = islice(versions, 500_000)
    cache = set(_version_cache_key(v["capture_time"], v.get("url", v.get("capture_url")))
                for v in limited_versions)
    logger.debug(f'  Found {len(cache)} known versions')
    return cache


def import_ia_db_urls(*, from_date=None, to_date=None, maintainers=None,
                      tags=None, skip_unchanged='resolved-response',
                      url_pattern=None, worker_count=0,
                      unplaybackable_path=None, dry_run=False,
                      precheck_versions=False, archive_storage=None):
    client = db.Client.from_env()
    logger.info('Loading known pages from web-monitoring-db instance...')
    urls, version_filter = _get_db_page_url_info(client, url_pattern)

    # Wayback search treats URLs as SURT, so dedupe obvious repeats first.
    www_subdomain = re.compile(r'^https?://www\d*\.')
    urls = set((www_subdomain.sub('http://', url) for url in urls))

    logger.info(f'Found {len(urls)} CDX-queryable URLs')
    logger.debug('\n  '.join(urls))

    version_cache = None
    if precheck_versions:
        version_cache = _load_known_versions(client,
                                             start_date=from_date,
                                             end_date=to_date)

    return import_ia_urls(
        urls=urls,
        from_date=from_date,
        to_date=to_date,
        maintainers=maintainers,
        tags=tags,
        skip_unchanged=skip_unchanged,
        version_filter=version_filter,
        worker_count=worker_count,
        create_pages=False,
        unplaybackable_path=unplaybackable_path,
        db_client=client,
        dry_run=dry_run,
        version_cache=version_cache,
        archive_storage=archive_storage)


# TODO: this function probably be split apart so `dry_run` doesn't need to
# exist as an argument.
def import_ia_urls(urls, *, from_date=None, to_date=None,
                   maintainers=None, tags=None,
                   skip_unchanged='resolved-response',
                   version_filter=None, worker_count=0,
                   create_pages=True, unplaybackable_path=None,
                   db_client=None, dry_run=False, version_cache=None,
                   archive_storage=None):
    for url in urls:
        if not _is_valid(url):
            raise ValueError(f'Invalid URL: "{url}"')

    worker_count = worker_count if worker_count > 0 else PARALLEL_REQUESTS
    unplaybackable = load_unplaybackable_mementos(unplaybackable_path)

    with utils.QuitSignal() as stop_event:
        cdx_records = utils.FiniteQueue()
        cdx_thread = threading.Thread(target=lambda: utils.iterate_into_queue(
            cdx_records,
            _list_ia_versions_for_urls(
                urls,
                from_date,
                to_date,
                version_filter,
                # Use a custom session to make sure CDX calls are extra robust.
                client=wayback.WaybackClient(wayback.WaybackSession(user_agent=USER_AGENT,
                                                                    retries=4,
                                                                    backoff=4,
                                                                    search_calls_per_second=WAYBACK_RATE_LIMIT)),
                stop=stop_event)))
        cdx_thread.start()

        versions_queue = utils.FiniteQueue()
        memento_thread = threading.Thread(target=lambda: WaybackRecordsWorker.parallel(
            worker_count,
            cdx_records,
            versions_queue,
            maintainers,
            tags,
            stop_event,
            unplaybackable=unplaybackable,
            version_cache=version_cache,
            archive_storage=archive_storage))
        memento_thread.start()

        # Show a progress meter
        # TODO: figure out whether we can update the expected total on the
        # meter once we have finished all the CDX searches.
        memento_data_queue = utils.FiniteQueue()
        progress_thread = threading.Thread(target=lambda: utils.iterate_into_queue(
            memento_data_queue,
            tqdm(versions_queue,
                 desc='Processing',
                 unit=' CDX Records',
                 disable=None)))
        progress_thread.start()

        # Filter out errors and summarize
        summary = {}
        importable_queue = utils.FiniteQueue()
        filter_importable_thread = threading.Thread(target=lambda: utils.iterate_into_queue(
            importable_queue,
            _filter_and_summarize_mementos(memento_data_queue, summary)))
        filter_importable_thread.start()

        uploadable_versions = importable_queue
        if skip_unchanged == 'resolved-response':
            uploadable_versions = _filter_unchanged_versions(importable_queue)
        if dry_run:
            uploader = threading.Thread(target=lambda: _log_adds(uploadable_versions))
        else:
            uploader = threading.Thread(target=lambda: _add_and_monitor(uploadable_versions, create_pages, False, stop_event))
        uploader.start()

        cdx_thread.join()
        memento_thread.join()
        progress_thread.join()
        filter_importable_thread.join()

        print('\nLoaded {total} CDX records:\n'
              '  {success:6} successes ({success_pct:.2f}%)\n'
              '  {already_known:6} skipped - already in DB ({already_known_pct:.2f}%)\n'
              '  {playback:6} could not be played back ({playback_pct:.2f}%)\n'
              '  {missing:6} had no actual memento ({missing_pct:.2f}%)\n'
              '  {unknown:6} unknown errors ({unknown_pct:.2f}%)'.format(
                **summary))

        # Print slow request statistics
        if len(MEMENTO_STATISTICS.histogram):
            print('\nMemento load timings:')
            top_bucket = max(MEMENTO_STATISTICS.histogram.keys())
            for bucket in range(top_bucket + 1):
                value = MEMENTO_STATISTICS.histogram[bucket]
                print(f'  {bucket * 10}-{bucket * 10 + 10} s: {value}')
            print('Slowest mementos:')
            if len(MEMENTO_STATISTICS.leaderboard) > 0:
                for timing in MEMENTO_STATISTICS.leaderboard:
                    print(f'  {timing[0]:.1f}: {timing[1]}')
            else:
                print('  ---')
            print('')

        uploader.join()

        if not dry_run:
            save_unplaybackable_mementos(unplaybackable_path, unplaybackable)

        if summary['success'] == 0:
            print('------------------------------')
            print('No new versions were imported!')


def _filter_unchanged_versions(versions):
    """
    Take an iteratable of importable version dicts and yield only versions that
    differ from the previous version of the same page.
    """
    last_hashes = {}
    for version in versions:
        if last_hashes.get(version['url']) != version['body_hash']:
            last_hashes[version['url']] = version['body_hash']
            yield version


def _list_ia_versions_for_urls(url_patterns, from_date, to_date,
                               version_filter=None, client=None, stop=None):
    version_filter = version_filter or _is_page
    skipped = 0
    total = 0

    with client or wayback.WaybackClient(wayback.WaybackSession(user_agent=USER_AGENT)) as client:
        for url in url_patterns:
            should_retry = True
            if stop and stop.is_set():
                break

            ia_versions = client.search(url, from_date=from_date,
                                        to_date=to_date, limit=1000)
            try:
                for version in ia_versions:
                    if stop and stop.is_set():
                        break
                    if version_filter(version):
                        total += 1
                        yield version
                    else:
                        skipped += 1
                        logger.debug('Skipping URL "%s"', version.url)
            except BlockedByRobotsError as error:
                logger.warn(f'CDX search error: {error!r}')
            except WaybackException as error:
                logger.error(f'Error getting CDX data for {url}: {error!r}')
            except Exception as error:
                # On connection failures, reset the session and try again. If
                # we don't do this, the connection pool for this thread is
                # pretty much dead. It's not clear to me whether there is a
                # problem in urllib3 or Wayback's servers that requires this.
                # This unfortunately requires string checking because the error
                # can get wrapped up into multiple kinds of higher-level
                # errors :(
                # TODO: unify this with similar code in WaybackRecordsWorker or
                # push it down into the `wayback` package.
                if should_retry and ('failed to establish a new connection' in str(error).lower()):
                    logger.warn('Resetting Wayback Session for CDX search.')
                    client.session.reset()
                    should_retry = False
                else:
                    # Need to handle the exception here to let iteration
                    # continue and allow other threads that might be running to
                    # be joined.
                    logger.exception(f'Error processing versions of {url}')

    logger.info('Found %s matching CDX records', total)
    logger.info('Skipped %s CDX records that did not match filters', skipped)


def load_unplaybackable_mementos(path):
    unplaybackable = {}
    if path:
        try:
            with path.open() as file:
                unplaybackable = json.load(file)
        except FileNotFoundError:
            pass
    return unplaybackable


def save_unplaybackable_mementos(path, mementos, expiration=7 * 24 * 60 * 60):
    if path is None:
        return

    print('Saving list of non-playbackable URLs...')

    threshold = datetime.utcnow() - timedelta(seconds=expiration)
    urls = list(mementos.keys())
    for url in urls:
        date = mementos[url]
        needs_format = False
        if isinstance(date, str):
            date = dateutil.parser.parse(date, ignoretz=True)
        else:
            needs_format = True

        if date < threshold:
            del mementos[url]
        elif needs_format:
            mementos[url] = date.isoformat(timespec='seconds') + 'Z'

    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as file:
        json.dump(mementos, file)


def _can_query_domain(domain):
    if domain in NEVER_QUERY_DOMAINS:
        return False

    return next((False for item in NEVER_QUERY_DOMAINS
                if domain.endswith(f'.{item}')), True)


def _get_db_page_url_info(client, url_pattern=None):
    # If these sets get too big, we can switch to a bloom filter. It's fine if
    # we have some false positives. Any noise reduction is worthwhile.
    url_keys = set()
    domains = defaultdict(lambda: {'query_domain': False, 'urls': []})

    domains_without_url_keys = set()
    pages = client.get_pages(url=url_pattern, active=True,
                             sort=['created_at:asc'], chunk_size=1000)
    for page in pages:
        domain = HOST_EXPRESSION.match(page['url']).group(1)
        data = domains[domain]
        if not data['query_domain']:
            if len(data['urls']) >= MAX_QUERY_URLS_PER_DOMAIN and _can_query_domain(domain):
                data['query_domain'] = True
            else:
                data['urls'].append(page['url'])

        if domain in domains_without_url_keys:
            continue

        url_key = page['url_key']
        if url_key:
            url_keys.add(_rough_url_key(url_key))
        else:
            domains_without_url_keys.add(domain)
            logger.warn('Found DB page with no url_key; *all* pages in '
                        f'"{domain}" will be imported')

    def filterer(version, domain=None):
        domain = domain or HOST_EXPRESSION.match(version.url).group(1)
        if domain in domains_without_url_keys:
            return _is_page(version)
        else:
            return _rough_url_key(version.key) in url_keys

    url_list = []
    for domain, data in domains.items():
        if data['query_domain']:
            url_list.append(f'http://{domain}/*')
        else:
            url_list.extend(data['urls'])

    return url_list, filterer


def _rough_url_key(url_key):
    """
    Create an ultra-loose version of a SURT key that should match regardless of
    most SURT settings. (This allows lots of false positives.)
    """
    rough_key = url_key.lower()
    rough_key = rough_key.split('?', 1)[0]
    rough_key = rough_key.split('#', 1)[0]
    rough_key = INDEX_PAGE_EXPRESSION.sub('', rough_key)
    if rough_key.endswith('/'):
        rough_key = rough_key[:-1]
    return rough_key


def _is_page(version):
    """
    Determine if a version might be a page we want to track. This is used to do
    some really simplistic filtering on noisy Internet Archive results if we
    aren't filtering down to a explicit list of URLs.
    """
    return (version.mime_type not in SUBRESOURCE_MIME_TYPES and
            splitext(urlparse(version.url).path)[1] not in SUBRESOURCE_EXTENSIONS)


def _parse_path(path_string):
    if path_string is None:
        return None

    parsed = urlparse(path_string)
    if parsed.scheme == '':
        return Path(path_string)
    elif parsed.scheme == 'file':
        return Path(path_string[7:])
    if parsed.scheme == 's3':
        return CloudPath(path_string)
    else:
        raise ValueError(f'"{parsed.scheme}://" paths are not supported')


def _is_valid(url):
    """
    Validate that all URLs are formatted correctly. This function assumes that
    a URL is valid if it has a valid addressing scheme and network location.
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


def validate_db_credentials():
    """
    Validate Web Monitoring DB credentials by creating a temporary client and
    attempting to get the current user session.

    Raises
    ------
    UnauthorizedCredentials
        If the credentials are not authorized for the provided host.
    """
    test_client = db.Client.from_env()
    try:
        test_client.validate_credentials()
    except db.UnauthorizedCredentials:
        raise db.UnauthorizedCredentials(f"""
            Unauthorized credentials for {test_client._base_url}.
            Check the following environment variables:

                WEB_MONITORING_DB_URL
                WEB_MONITORING_DB_EMAIL {os.environ.get('WEB_MONITORING_DB_EMAIL')}
                WEB_MONITORING_DB_PASSWORD
                """)


def main():
    doc = f"""Command Line Interface to the web_monitoring Python package

Usage:
wm import ia <url> [--from <from_date>] [--to <to_date>] [--tag <tag>...] [--maintainer <maintainer>...] [options]
wm import ia-known-pages [--from <from_date>] [--to <to_date>] [--pattern <url_pattern>] [--tag <tag>...] [--maintainer <maintainer>...] [options]

Options:
-h --help                     Show this screen.
--version                     Show version.
--maintainer <maintainer>     Name of entity that maintains the imported pages.
                              Repeat to add multiple maintainers.
--tag <tag>                   Tags to apply to pages. Repeat for multiple tags.
--skip-unchanged <skip_type>  Skip consecutive captures of the same content.
                              Can be:
                                `none` (no skipping) or
                                `resolved-response` (if the final response
                                    after redirects is unchanged)
                              [default: resolved-response]
--pattern <url_pattern>       A pattern to match when retrieving URLs from a
                              web-monitoring-db instance.
--parallel <parallel_count>   Number of parallel network requests to support.
                              [default: {PARALLEL_REQUESTS}]
--unplaybackable <play_path>  A file in which to list memento URLs that can not
                              be played back. When importing is complete, a
                              list of unplaybackable mementos will be written
                              to this file. If it exists before importing,
                              memento URLs listed in it will be skipped.
--precheck                    Check the list of versions in web-monitoring-db
                              and avoid re-importing duplicates.
--dry-run                     Don't upload data to web-monitoring-db.
--archive-s3 <bucket>         Pre-upload response bodies to this S3 bucket
                              before sending import data to web-monitoring-db.
"""
    sentry_sdk.init()
    arguments = docopt(doc, version='0.0.1')
    if arguments['import']:
        dry_run = arguments.get('--dry-run') or False

        skip_unchanged = arguments['--skip-unchanged']
        if skip_unchanged not in ('none', 'response', 'resolved-response'):
            print('--skip-unchanged must be one of `none`, `response`, '
                  'or `resolved-response`')
            return

        unplaybackable_path = _parse_path(arguments.get('--unplaybackable'))
        if not dry_run:
            validate_db_credentials()

        archive_storage = None
        archive_bucket = arguments.get('--archive-s3')
        if archive_bucket and not dry_run:
            archive_storage = utils.S3HashStore(archive_bucket, extra_args={
                'ACL': 'public-read',
                # Ideally, we'd gzip stuff, but the DB needs to learn to
                # correctly read gzipped items first.
                # 'ContentEncoding': 'gzip'
            }, dry_run=dry_run)

        start_time = datetime.now(tz=timezone.utc)
        if arguments['ia']:
            import_ia_urls(
                urls=[arguments['<url>']],
                maintainers=arguments.get('--maintainer'),
                tags=arguments.get('--tag'),
                from_date=cli_datetime(arguments['<from_date>']),
                to_date=cli_datetime(arguments['<to_date>']) if arguments['<to_date>'] else None,
                skip_unchanged=skip_unchanged,
                unplaybackable_path=unplaybackable_path,
                dry_run=dry_run,
                archive_storage=archive_storage)
        elif arguments['ia-known-pages']:
            import_ia_db_urls(
                from_date=cli_datetime(arguments['<from_date>']),
                to_date=cli_datetime(arguments['<to_date>']) if arguments['<to_date>'] else None,
                maintainers=arguments.get('--maintainer'),
                tags=arguments.get('--tag'),
                skip_unchanged=skip_unchanged,
                url_pattern=arguments.get('--pattern'),
                worker_count=int(arguments.get('--parallel')),
                unplaybackable_path=unplaybackable_path,
                dry_run=dry_run,
                precheck_versions=arguments.get('--precheck'),
                archive_storage=archive_storage)

        end_time = datetime.now(tz=timezone.utc)
        print(f'Completed at {end_time.isoformat()}')
        print(f'Duration: {end_time - start_time}')


if __name__ == '__main__':
    main()
