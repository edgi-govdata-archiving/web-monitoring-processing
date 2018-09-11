# Command Line Interface
# See scripts/ directory for associated executable(s). All of the interesting
# functionality is implemented in this module to make it easier to test.
from docopt import docopt
import logging
from os.path import splitext
import pandas
import re
import requests
from tqdm import tqdm
from urllib.parse import urlparse
from web_monitoring import db
from web_monitoring import internetarchive as ia


logger = logging.getLogger(__name__)

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


# These functions lump together library code into monolithic operations for the
# CLI. They also print. To access this functionality programmatically, it is
# better to use the underlying library code.


def _add_and_monitor(versions):
    cli = db.Client.from_env()  # will raise if env vars not set
    # Wrap verions in a progress bar.
    versions = tqdm(versions, desc='importing', unit=' versions')
    import_ids = cli.add_versions(versions)
    print('Import jobs IDs: {}'.format(import_ids))
    print('Polling web-monitoring-db until import jobs are finished...')
    errors = cli.monitor_import_statuses(import_ids)
    if errors:
        print("Errors: {}".format(errors))


def import_ia_db_urls(*, from_date=None, to_date=None, maintainers=None,
                      tags=None, skip_unchanged='resolved-response'):
    client = db.Client.from_env()
    logger.info('Loading known pages from web-monitoring-db instance...')
    domains, version_filter = _get_db_page_url_info(client)
    print('Importing {} Domains:\n  {}'.format(
        len(domains),
        '\n  '.join(domains)))

    return import_ia_urls(
        urls=[f'http://{domain}/*' for domain in domains],
        from_date=from_date,
        to_date=to_date,
        maintainers=maintainers,
        tags=tags,
        skip_unchanged=skip_unchanged,
        version_filter=version_filter)


def import_ia_urls(urls, *, from_date=None, to_date=None, maintainers=None,
                   tags=None, skip_unchanged='resolved-response',
                   version_filter=None):
    skip_responses = skip_unchanged == 'response'
    with ia.WaybackClient() as wayback:
        def timestamped_uri_to_versions(ia_versions):
            wayback_errors = {'playback': [], 'missing': [], 'unknown': []}
            for version in ia_versions:
                try:
                    yield wayback.timestamped_uri_to_version(version.date,
                                                             version.raw_url,
                                                             url=version.url,
                                                             maintainers=maintainers,
                                                             tags=tags,
                                                             view_url=version.view_url)
                except ia.MementoPlaybackError as error:
                    wayback_errors['playback'].append(error)
                    logger.info(f'  {error}')
                except requests.exceptions.HTTPError as error:
                    logger.info(f'  {error}')
                    if error.response.status_code == 404:
                        wayback_errors['missing'].append(error)
                    else:
                        wayback_errors['unknown'].append(error)
                except Exception as error:
                    wayback_errors['unknown'].append(error)
                    logger.info(f'  {error}')

            playback_errors = len(wayback_errors['playback'])
            missing_errors = len(wayback_errors['missing'])
            unknown_errors = len(wayback_errors['unknown'])
            if playback_errors + missing_errors + unknown_errors > 0:
                logger.warn(f'\nErrors: {playback_errors} could not be played back, '
                            f'{missing_errors} indexed snapshots were missing, '
                            f'{unknown_errors} unknown errors.')

        versions = timestamped_uri_to_versions(_list_ia_versions_for_urls(urls,
                                                                          from_date,
                                                                          to_date,
                                                                          skip_responses,
                                                                          version_filter,
                                                                          client=wayback))

        if skip_unchanged == 'resolved-response':
            versions = _filter_unchanged_versions(versions)

        _add_and_monitor(versions)


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
    client = client or ia.WaybackClient()
    skipped = 0

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
        except ValueError as error:
            logger.warn(error)

    if skipped > 0:
        logger.info('Skipped %s URLs that did not match filters', skipped)


def _get_db_page_url_info(client):
    url_keys = set()
    domains = set()
    use_url_keys = True
    for page in _list_all_db_pages(client):
        domains.add(HOST_EXPRESSION.match(page['url']).group(1))
        if use_url_keys is False:
            continue

        url_key = page['url_key']
        if url_key:
            url_keys.add(_rough_url_key(url_key))
        else:
            use_url_keys = False
            url_keys.clear()
            logger.warn('Found DB page with no url_key; *all* pages in '
                        'matching domains will be imported')

    filterer = None
    if use_url_keys:
        filterer = lambda version: _rough_url_key(version.key) in url_keys
    return domains, filterer


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
def _list_all_db_pages(client):
    chunk = 1
    while chunk > 0:
        pages = client.list_pages(sort=['created_at:asc'], chunk_size=1000,
                                  chunk=chunk)
        yield from pages['data']
        chunk = pages['links']['next'] and (chunk + 1) or -1


def _parse_date_argument(date_string):
    """Parse a CLI argument that should represent a date into a datetime"""
    if not date_string:
        return None

    try:
        parsed = pandas.to_datetime(date_string)
        if not pandas.isnull(parsed):
            return parsed
    except ValueError:
        pass

    return None


def main():
    doc = """Command Line Interface to the web_monitoring Python package

Usage:
wm import ia <url> [--from <from_date>] [--to <to_date>] [options]
wm import ia-known-pages [--from <from_date>] [--to <to_date>] [options]

Options:
-h --help                     Show this screen.
--version                     Show version.
--maintainers <maintainers>   Comma-separated list of entities that maintain
                              the imported pages.
--tags <tags>                 Comma-separated list of tags to apply to pages
--skip-unchanged <skip_type>  Skip consecutive captures of the same content.
                              Can be:
                                `none` (no skipping),
                                `response` (if the response is unchanged), or
                                `resolved-response` (if the final response
                                    after redirects is unchanged)
                              [default: resolved-response]
"""
    arguments = docopt(doc, version='0.0.1')
    if arguments['import']:
        skip_unchanged = arguments['--skip-unchanged']
        if skip_unchanged not in ('none', 'response', 'resolved-response'):
            print('--skip-unchanged must be one of `none`, `response`, '
                  'or `resolved-response`')
            return

        if arguments['ia']:
            import_ia_urls(
                url=[arguments['<url>']],
                maintainers=arguments.get('--maintainers'),
                tags=arguments.get('--tags'),
                from_date=_parse_date_argument(arguments['<from_date>']),
                to_date=_parse_date_argument(arguments['<to_date>']),
                skip_unchanged=skip_unchanged)
        elif arguments['ia-known-pages']:
            import_ia_db_urls(
                from_date=_parse_date_argument(arguments['<from_date>']),
                to_date=_parse_date_argument(arguments['<to_date>']),
                maintainers=arguments.get('--maintainers'),
                tags=arguments.get('--tags'),
                skip_unchanged=skip_unchanged)


if __name__ == '__main__':
    main()
