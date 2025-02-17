#!/usr/bin/env python

# This script checks whether the Internet Archive's Wayback Machine has
# recent captures of the URLs we are tracking in the Web Monitoring Database.
# It works by taking a random sample of pages from the DB and using the CDX API
# to check that each has been captured at least once in the last few days.

from argparse import ArgumentParser
import random
import sentry_sdk
import sys
from wayback import WaybackClient
from .. import db
from ..utils import cli_datetime


# The current Sentry client truncates string values at 512 characters. It
# appears that monkey-patching this module global is only way to change it and
# that doing so is the intended method:
#   https://github.com/getsentry/sentry-python/blob/5f9f7c469af16a731948a482ea162c2348800999/sentry_sdk/utils.py#L662-L664
# That doesn't seem great, so I've asked about this on their forums:
#   https://forum.sentry.io/t/some-stack-traces-are-truncated/7309/4
sentry_sdk.utils.MAX_STRING_LENGTH = 2048


def sample_monitored_urls(sample_size):
    """
    Get a random sample of `sample_size` URLs that are tracked in a Web
    Monitoring DB instance.

    Returns
    -------
    list of string
    """
    client = db.Client.from_env()
    page = client.list_pages(chunk=1, chunk_size=1, active=True, include_total=True)
    url_count = page['meta']['total_results']
    return (get_page_url(client, index)
            for index in random.sample(range(url_count), sample_size))


def get_page_url(client, index):
    return client.list_pages(chunk=index, chunk_size=1, active=True)['data'][0]['url']


def wayback_has_captures(url, from_date=None):
    """
    Determine whether the Wayback Machine has any recent captures of a URL.

    Parameters
    ----------
    url : string

    Returns
    -------
    list of JSON
    """
    with WaybackClient() as wayback:
        versions = wayback.search(url, from_date=from_date, limit=1)
        try:
            next(versions)
        except StopIteration:
            return False
        else:
            return True


def output_results(statuses):
    """
    Output nicely formatted results.

    Parameters
    ----------
    statuses: sequence of tuple of (str, bool)
    """
    healthy_links = 0
    unhealthy_links = 0

    logs = []
    for (url, status) in statuses:
        if status:
            healthy_links += 1
            status_text = '✔︎    Found'
        else:
            unhealthy_links += 1
            status_text = '✘  Missing'

        message = f'{status_text}: {url}'
        print(message)
        logs.append(message)

    # At this point, everything is OK; we don't need breadcrumbs and other
    # extra noise to come with the message we are about to send.
    with sentry_sdk.configure_scope() as scope:
        scope.clear()

    if healthy_links + unhealthy_links == 0:
        print('Failed to sampled any pages!')
        sentry_sdk.capture_message('Failed to sampled any pages!')
    else:
        message = f'\nFound: {healthy_links} healthy links and {unhealthy_links} unhealthy links.'
        print(message)
        if unhealthy_links > 0:
            log_string = '\n'.join(logs)
            sentry_sdk.capture_message(f'{message}\n{log_string}')


def main():
    sentry_sdk.init()
    parser = ArgumentParser()
    parser.add_argument('--from', type=cli_datetime,
                        default=cli_datetime('3d'), dest='from_time',
                        help='Check for captures later than this time.')
    parser.add_argument('--sample', type=int, default=10,
                        help='Sample this many URLs for captures.')
    args = parser.parse_args()

    try:
        print(f'Sampling {args.sample} pages from Web Monitoring API...')
        links = sample_monitored_urls(args.sample)
        print('Checking for captures in Wayback Machine...')
        capture_statuses = ((url, wayback_has_captures(url, args.from_time))
                            for url in links)
        output_results(capture_statuses)
    except db.MissingCredentials as error:
        print(error, file=sys.stderr)
