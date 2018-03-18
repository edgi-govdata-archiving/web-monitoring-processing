# Command Line Interface
# See scripts/ directory for associated executable(s). All of the interesting
# functionality is implemented in this module to make it easier to test.
from docopt import docopt
import logging
import pandas
import re
from tqdm import tqdm
from web_monitoring import db
from web_monitoring import internetarchive as ia


logger = logging.getLogger(__name__)

HOST_EXPRESSION = re.compile(r'^[^:]+://([^/]+)')


# These functions lump together library code into monolithic operations for the
# CLI. They also print. To access this functionality programmatically, it is
# better to use the underlying library code.


def _add_and_monitor(versions):
    cli = db.Client.from_env()  # will raise if env vars not set
    # Wrap verions in a progress bar.
    versions = tqdm(versions, desc='importing', unit=' versions')
    print('Submitting Versions to web-monitoring-db...')
    import_ids = cli.add_versions(versions)
    print('Import jobs IDs: {}'.format(import_ids))
    print('Polling web-monitoring-db until import jobs are finished...')
    errors = cli.monitor_import_statuses(import_ids)
    if errors:
        print("Errors: {}".format(errors))


def import_ia(url, *, from_date=None, to_date=None, maintainers=None,
              tags=None, skip_unchanged='resolved-response'):
    skip_responses = skip_unchanged == 'response'
    with ia.WaybackClient() as wayback:
        # Pulling on this generator does the work.
        versions = (wayback.timestamped_uri_to_version(version.date,
                                                       version.raw_url,
                                                       url=version.url,
                                                       maintainers=maintainers,
                                                       tags=tags,
                                                       view_url=version.view_url)
                    for version in wayback.list_versions(url,
                                                         from_date=from_date,
                                                         to_date=to_date,
                                                         skip_repeats=skip_responses))

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


def import_ia_db_urls(*, from_date=None, to_date=None, maintainers=None,
                      tags=None):
    client = db.Client.from_env()
    domains, url_keys = _get_db_page_url_info(client)
    print('Importing {} URLs from {} Domains:\n  {}'.format(
        len(url_keys),
        len(domains),
        '\n  '.join(domains)))

    versions = (ia.timestamped_uri_to_version(version.date, version.raw_url,
                                              url=version.url,
                                              maintainers=maintainers,
                                              tags=tags,
                                              view_url=version.view_url)
                for version in _list_ia_versions_for_domains(domains,
                                                             from_date,
                                                             to_date,
                                                             url_keys))
    _add_and_monitor(versions)


def _list_ia_versions_for_domains(domains, from_date, to_date,
                                  filter_keys=None):
    skipped = 0

    for domain in domains:
        ia_versions = ia.list_versions(f'http://{domain}/*',
                                       from_date=from_date,
                                       to_date=to_date)
        for version in ia_versions:
            # TODO: this is an imperfect shortcut -- if the key algorithm ever
            # changes on our side or IA's side (which seems like a given that
            # it will *sometime*), it won't work right. Maybe the DB needs
            # `add=existing_pages` or something?
            if version.key in filter_keys:
                yield version
            else:
                skipped += 1
                logger.debug('Skipping URL "%s"', version.url)

    if skipped > 0:
        logger.info('Skipped %s URLs that were unknown', skipped)


def _get_db_page_url_info(client):
    url_keys = set()
    domains = set()
    for page in _list_all_db_pages(client):
        url_keys.add(page['url_key'])
        domains.add(HOST_EXPRESSION.match(page['url']).group(1))

    return domains, url_keys


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
            import_ia(url=arguments['<url>'],
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
                tags=arguments.get('--tags'))


if __name__ == '__main__':
    main()
