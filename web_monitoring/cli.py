# Command Line Interface
# See scripts/ directory for associated executable(s). All of the interesting
# functionality is implemented in this module to make it easier to test.
from collections import defaultdict
from datetime import datetime, timedelta
from docopt import docopt
import json
import logging
from os.path import splitext
import pandas
from pathlib import Path
import re
import requests
import toolz
from tqdm import tqdm
from urllib.parse import urlparse
from web_monitoring import db
from web_monitoring import internetarchive as ia
from web_monitoring import utils

import queue
import asyncio
import concurrent


logger = logging.getLogger(__name__)

PARALLEL_REQUESTS = 10

HOST_EXPRESSION = re.compile(r'^[^:]+://([^/]+)')
INDEX_PAGE_EXPRESSION = re.compile(r'index(\.\w+)?$')
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
# query for each specific URL we want).
NEVER_QUERY_DOMAINS = (
    'instagram.com',
    'youtube.com',
    'amazon.com'
)
# Query an entire domain for snapshots if we are interested in more than this
# many URLs in the domain (NEVER_QUERY_DOMAINS above overrides this).
MAX_QUERY_URLS_PER_DOMAIN = 20


# These functions lump together library code into monolithic operations for the
# CLI. They also print. To access this functionality programmatically, it is
# better to use the underlying library code.


def _add_and_monitor(versions, create_pages=True, skip_unchanged_versions=True):
    cli = db.Client.from_env()  # will raise if env vars not set
    # Wrap verions in a progress bar.
    # TODO: create this on the main thread so we can update totals when we
    # discover them in CDX, but update progress here as we import.
    versions = tqdm(versions, desc='importing', unit=' versions')
    import_ids = cli.add_versions(versions, create_pages=create_pages,
                                  skip_unchanged_versions=skip_unchanged_versions)
    print('Import jobs IDs: {}'.format(import_ids))
    print('Polling web-monitoring-db until import jobs are finished...')
    errors = cli.monitor_import_statuses(import_ids)
    if errors:
        print("Errors: {}".format(errors))


def _log_adds(versions):
    versions = tqdm(versions, desc='importing', unit=' versions')
    for version in versions:
        print('')  # Line break from tqdm output
        print(json.dumps(version))


def load_wayback_records_worker(records, results_queue, maintainers, tags, failure_queue=None, session_options=None, unplaybackable=None):
    summary = worker_summary()
    session_options = session_options or dict(retries=3, backoff=2, timeout=(30.5, 2))
    session = ia.WaybackSession(**session_options)
    start_date = datetime.utcnow()

    with ia.WaybackClient(session=session) as wayback:
        while True:
            try:
                record = next(records)
                summary['total'] += 1
            except StopIteration:
                break

            if unplaybackable is not None and record.raw_url in unplaybackable:
                summary['playback'] += 1
                continue

            try:
                version = wayback.timestamped_uri_to_version(record.date,
                                                             record.raw_url,
                                                             url=record.url,
                                                             maintainers=maintainers,
                                                             tags=tags,
                                                             view_url=record.view_url)
                results_queue.put(version)
                summary['success'] += 1
            except ia.MementoPlaybackError as error:
                summary['playback'] += 1
                if unplaybackable is not None:
                    unplaybackable[record.raw_url] = start_date
                logger.info(f'  {error}')
            except requests.exceptions.HTTPError as error:
                if error.response.status_code == 404:
                    logger.info(f'  Missing memento: {record.raw_url}')
                    summary['missing'] += 1
                else:
                    logger.info(f'  (HTTPError) {error}')
                    summary['unknown'] += 1
                    if failure_queue:
                        failure_queue.put(record)
            except ia.WaybackRetryError as error:
                summary['unknown'] += 1
                logger.info(f'  {error}; URL: {record.raw_url}')
                if failure_queue:
                    failure_queue.put(record)
            except Exception as error:
                summary['unknown'] += 1
                logger.info(f'  ({type(error)}) {error}; URL: {record.raw_url}')
                if failure_queue:
                    failure_queue.put(record)

    return summary


async def import_ia_db_urls(*, from_date=None, to_date=None, maintainers=None,
                            tags=None, skip_unchanged='resolved-response',
                            url_pattern=None, worker_count=0,
                            unplaybackable_path=None, dry_run=False):
    client = db.Client.from_env()
    logger.info('Loading known pages from web-monitoring-db instance...')
    urls, version_filter = _get_db_page_url_info(client, url_pattern)

    # Wayback search treats URLs as SURT, so dedupe obvious repeats first.
    www_subdomain = re.compile(r'^https?://www\d*\.')
    urls = set((www_subdomain.sub('http://', url) for url in urls))

    _print_domain_list(urls)

    return await import_ia_urls(
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
        dry_run=dry_run)


def worker_summary():
    return {'total': 0, 'success': 0, 'playback': 0, 'missing': 0,
            'unknown': 0}


def merge_worker_summaries(summaries):
    merged = worker_summary()
    for summary in summaries:
        for key in merged.keys():
            merged[key] += summary[key]

    # Add percentage calculations
    if merged['total']:
        merged.update({f'{k}_pct': 100 * v / merged['total']
                       for k, v in merged.items()
                       if k != 'total' and not k.endswith('_pct')})
    else:
        merged.update({f'{k}_pct': 0.0
                       for k, v in merged.items()
                       if k != 'total' and not k.endswith('_pct')})

    return merged


# TODO: this function probably be split apart so `dry_run` doesn't need to
# exist as an argument.
async def import_ia_urls(urls, *, from_date=None, to_date=None,
                         maintainers=None, tags=None,
                         skip_unchanged='resolved-response',
                         version_filter=None, worker_count=0,
                         create_pages=True, unplaybackable_path=None,
                         dry_run=False):
    skip_responses = skip_unchanged == 'response'
    worker_count = worker_count if worker_count > 0 else PARALLEL_REQUESTS
    unplaybackable = load_unplaybackable_mementos(unplaybackable_path)

    # Use a custom session to make sure CDX calls are extra robust.
    session = ia.WaybackSession(retries=10, backoff=4)

    memento_options_intermediate = dict(retries=3, backoff=4, timeout=(30.5, 2))
    memento_options_final = dict(retries=7, backoff=4, timeout=60.5)
    final_retry_queue = queue.Queue()

    with ia.WaybackClient(session) as wayback:
        # wayback_records = utils.ThreadSafeIterator(
        #     _list_ia_versions_for_urls(
        #         urls,
        #         from_date,
        #         to_date,
        #         skip_responses,
        #         version_filter,
        #         client=wayback))

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count + 1)
        loop = asyncio.get_event_loop()
        versions_queue = queue.Queue()
        versions = utils.queue_iterator(versions_queue)
        if skip_unchanged == 'resolved-response':
            versions = _filter_unchanged_versions(versions)

        if dry_run:
            uploader = loop.run_in_executor(executor, _log_adds, versions)
        else:
            uploader = loop.run_in_executor(executor, _add_and_monitor, versions, create_pages)

        # summary = worker_summary()
        summary = merge_worker_summaries([worker_summary()])
        all_records = _list_ia_versions_for_urls(
            urls,
            from_date,
            to_date,
            skip_responses,
            version_filter,
            client=wayback)
        for wayback_records in toolz.partition_all(2000, all_records):
            wayback_records = utils.ThreadSafeIterator(wayback_records)

            # versions_queue = queue.Queue()
            retry_queue = queue.Queue()
            # Add an extra thread for the DB uploader so it can collect results in
            # parallel if there are more than 1000
            # executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count + 1)
            # loop = asyncio.get_event_loop()
            workers = [loop.run_in_executor(executor, load_wayback_records_worker, wayback_records, versions_queue, maintainers, tags, retry_queue, None, unplaybackable)
                       for i in range(worker_count)]

            # versions = utils.queue_iterator(versions_queue)
            # if skip_unchanged == 'resolved-response':
            #     versions = _filter_unchanged_versions(versions)
            # uploader = loop.run_in_executor(executor, _add_and_monitor, versions, create_pages)

            results = await asyncio.gather(*workers)
            # summary = merge_worker_summaries(results)
            summary = merge_worker_summaries((summary, *results))

            # If there are failures to retry, re-spawn the workers to run them
            # with more retries and higher timeouts.
            if not retry_queue.empty():
                print(f'\nRetrying about {retry_queue.qsize()} failed records...')
                retry_queue.put(None)
                retries = utils.ThreadSafeIterator(utils.queue_iterator(retry_queue))
                workers = [loop.run_in_executor(executor, load_wayback_records_worker, retries, versions_queue, maintainers, tags, final_retry_queue, memento_options_intermediate, unplaybackable)
                           for i in range(worker_count)]

                # Update summary info
                results = await asyncio.gather(*workers)
                retry_summary = merge_worker_summaries(results)
                summary['success'] += retry_summary['success']
                summary['success_pct'] = summary['success'] / summary['total']
                summary['unknown'] -= retry_summary['success']
                summary['unknown_pct'] = summary['unknown'] / summary['total']

        # If there are failures to retry, re-spawn the workers to run them
        # with more retries and higher timeouts.
        if not final_retry_queue.empty():
            print(f'\nRetrying about {retry_queue.qsize()} failed records...')
            final_retry_queue.put(None)
            retries = utils.ThreadSafeIterator(utils.queue_iterator(final_retry_queue))
            workers = [loop.run_in_executor(executor, load_wayback_records_worker, retries, versions_queue, maintainers, tags, None, memento_options_final, unplaybackable)
                       for i in range(worker_count)]

            # Update summary info
            results = await asyncio.gather(*workers)
            retry_summary = merge_worker_summaries(results)
            summary['success'] += retry_summary['success']
            summary['success_pct'] = summary['success'] / summary['total']
            summary['unknown'] -= retry_summary['success']
            summary['unknown_pct'] = summary['unknown'] / summary['total']

        print('\nLoaded {total} CDX records:\n'
              '  {success:6} successes ({success_pct:.2f}%),\n'
              '  {playback:6} could not be played back ({playback_pct:.2f}%),\n'
              '  {missing:6} had no actual memento ({missing_pct:.2f}%),\n'
              '  {unknown:6} unknown errors ({unknown_pct:.2f}%).'.format(
                **summary))

        # Signal that there will be nothing else on the queue so uploading can finish
        versions_queue.put(None)

        if not dry_run:
            print('Saving list of non-playbackable URLs...')
            save_unplaybackable_mementos(unplaybackable_path, unplaybackable)

        await uploader


def _filter_unchanged_versions(versions):
    """
    Take an iteratable of importable version dicts and yield only versions that
    differ from the previous version of the same page.
    """
    last_hashes = {}
    for version in versions:
        if last_hashes.get(version['page_url']) != version['version_hash']:
            last_hashes[version['page_url']] = version['version_hash']
            yield version


def _list_ia_versions_for_urls(url_patterns, from_date, to_date,
                               skip_repeats=True, version_filter=None,
                               client=None):
    version_filter = version_filter or _is_page
    skipped = 0

    with client or ia.WaybackClient() as client:
        for url in url_patterns:
            ia_versions = client.list_versions(url,
                                            from_date=from_date,
                                            to_date=to_date,
                                            skip_repeats=skip_repeats)
            try:
                for version in ia_versions:
                    if version_filter(version):
                        yield version
                    else:
                        skipped += 1
                        logger.debug('Skipping URL "%s"', version.url)
            except ia.BlockedByRobotsError as error:
                logger.warn(str(error))
            except ValueError as error:
                # TODO: there should probably be no exception in this case
                if 'does not have archived versions' not in str(error):
                    logger.warn(error)

    if skipped > 0:
        logger.info('Skipped %s URLs that did not match filters', skipped)


def load_unplaybackable_mementos(path):
    unplaybackable = {}
    if path:
        try:
            with open(path) as file:
                unplaybackable = json.load(file)
        except FileNotFoundError:
            pass
    return unplaybackable


def save_unplaybackable_mementos(path, mementos, expiration=7 * 24 * 60 * 60):
    if path is None:
        return

    threshold = datetime.utcnow() - timedelta(seconds=expiration)
    urls = list(mementos.keys())
    for url in urls:
        date = mementos[url]
        needs_format = False
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
        else:
            needs_format = True

        if date < threshold:
            del mementos[url]
        elif needs_format:
            mementos[url] = date.isoformat(timespec='seconds') + 'Z'

    file_path = Path(path)
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open('w') as file:
        json.dump(mementos, file)


def list_domains(url_pattern=None):
    client = db.Client.from_env()
    logger.info('Loading known pages from web-monitoring-db instance...')
    domains, version_filter = _get_db_page_url_info(client, url_pattern)
    _print_domain_list(domains)


def _print_domain_list(domains):
    text = '\n  '.join(domains)
    print(f'Found {len(domains)} matching domains:\n  {text}')


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
    for page in _list_all_db_pages(client, url_pattern):
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

    ###### DEBUG
    # print(f'Total domains: {len(domains)}')
    # if len(domains) > 2:
    #     # domains = set(['www.phmsa.dot.gov', 'www.noaa.inel.gov'] + list(domains)[0:2])
    #     domains = set(list(domains)[0:2])
    # # domains = domains - {'www.w3.org'}
    # # domains = {'mrcc.illinois.edu'}
    # # domains = {'www.doe.gov'}
    # domains = {'www.epa.gov'}
    ###### DEBUG

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


# TODO: this should probably be a method on db.Client, but db.Client could also
# do well to transform the `links` into callables, e.g:
#     more_pages = pages['links']['next']()
def _list_all_db_pages(client, url_pattern=None):
    chunk = 1
    while chunk > 0:
        pages = client.list_pages(sort=['created_at:asc'], chunk_size=1000,
                                  chunk=chunk, url=url_pattern)
        yield from pages['data']
        chunk = pages['links']['next'] and (chunk + 1) or -1


def _parse_date_argument(date_string):
    """Parse a CLI argument that should represent a date into a datetime"""
    if not date_string:
        return None

    try:
        hours = float(date_string)
        return datetime.utcnow() - timedelta(hours=hours)
    except ValueError:
        pass

    try:
        parsed = pandas.to_datetime(date_string)
        if not pandas.isnull(parsed):
            return parsed
    except ValueError:
        pass

    return None


def main():
    doc = f"""Command Line Interface to the web_monitoring Python package

Usage:
wm import ia <url> [--from <from_date>] [--to <to_date>] [--tag <tag>...] [--maintainer <maintainer>...] [options]
wm import ia-known-pages [--from <from_date>] [--to <to_date>] [--pattern <url_pattern>] [--tag <tag>...] [--maintainer <maintainer>...] [options]
wm db list-domains [--pattern <url_pattern>]

Options:
-h --help                     Show this screen.
--version                     Show version.
--maintainer <maintainer>     Name of entity that maintains the imported pages.
                              Repeat to add multiple maintainers.
--tag <tag>                   Tags to apply to pages. Repeat for multiple tags.
--skip-unchanged <skip_type>  Skip consecutive captures of the same content.
                              Can be:
                                `none` (no skipping),
                                `response` (if the response is unchanged), or
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
--dry-run                     Don't upload data to web-monitoring-db.
"""
    arguments = docopt(doc, version='0.0.1')
    command = None
    if arguments['import']:
        skip_unchanged = arguments['--skip-unchanged']
        if skip_unchanged not in ('none', 'response', 'resolved-response'):
            print('--skip-unchanged must be one of `none`, `response`, '
                  'or `resolved-response`')
            return

        if arguments['ia']:
            command = import_ia_urls(
                urls=[arguments['<url>']],
                maintainers=arguments.get('--maintainer'),
                tags=arguments.get('--tag'),
                from_date=_parse_date_argument(arguments['<from_date>']),
                to_date=_parse_date_argument(arguments['<to_date>']),
                skip_unchanged=skip_unchanged,
                unplaybackable_path=arguments.get('--unplaybackable'),
                dry_run=arguments.get('--dry-run'))
        elif arguments['ia-known-pages']:
            command = import_ia_db_urls(
                from_date=_parse_date_argument(arguments['<from_date>']),
                to_date=_parse_date_argument(arguments['<to_date>']),
                maintainers=arguments.get('--maintainer'),
                tags=arguments.get('--tag'),
                skip_unchanged=skip_unchanged,
                url_pattern=arguments.get('--pattern'),
                worker_count=int(arguments.get('--parallel')),
                unplaybackable_path=arguments.get('--unplaybackable'),
                dry_run=arguments.get('--dry-run'))

    elif arguments['db']:
        if arguments['list-domains']:
            list_domains(url_pattern=arguments.get('--pattern'))

    # Start a loop and execute commands that are async.
    if asyncio.iscoroutine(command):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(command)


if __name__ == '__main__':
    main()
