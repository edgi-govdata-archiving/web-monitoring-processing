from argparse import ArgumentParser
from dataclasses import dataclass, field
import gzip
from itertools import islice
import json
import logging
from pathlib import Path
import re
import sys
import threading
from typing import Generator, Iterable
from cloudpathlib import S3Client, S3Path
import sentry_sdk
from tqdm.contrib.logging import tqdm_logging_redirect
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord, StatusAndHeadersParser, StatusAndHeaders
from .. import db
from .. import utils
from ..utils import detect_encoding, sniff_media_type


logger = logging.getLogger(__name__)


# FIXME: share with `cli.py`
# We do some parsing of HTML and PDF documents to extract the title before
# importing the document. These media types are used to determine whether and
# how to parse the document.
HTML_MEDIA_TYPES = frozenset((
    'application/html',
    'application/xhtml',
    'application/xhtml+xml',
    'application/xml',
    'application/xml+html',
    'application/xml+xhtml',
    'text/webviewhtml',
    'text/html',
    'text/x-server-parsed-html',
    'text/xhtml',
))
PDF_MEDIA_TYPES = frozenset((
    'application/pdf',
    'application/x-pdf',
))
# These media types are so meaningless that it's worth sniffing the content to
# see if we can determine an actual media type.
SNIFF_MEDIA_TYPES = frozenset((
    'application/octet-stream',
    'application/x-download',
    'binary/octet-stream',
))
# Identifies a bare media type (that is, one without parameters)
MEDIA_TYPE_EXPRESSION = re.compile(r'^\w+/\w[\w+_\-.]+$')


# FIXME: share with `cli.py`
class S3HashStore:
    """
    Store and track content-addressed data in S3.
    """

    def __init__(self, bucket: str, gzip: bool = False, extra_args: dict = {}, dry_run: bool = False) -> None:
        self.bucket = bucket
        self.extra_args = extra_args
        self.gzip = gzip
        if gzip:
            self.extra_args['ContentEncoding'] = 'gzip'
        self.seen_hashes = set()
        self.lock = threading.Lock()
        self.dry_run = dry_run

    def store(self, data: bytes, hash: str = '', content_type: str = '') -> str:
        if not hash:
            hash = utils.hash_content(data)

        if not content_type:
            content_type = 'application/octet-stream'

        archive = S3Path(f's3://{self.bucket}', client=S3Client(extra_args={
            **self.extra_args,
            'ContentType': content_type
        }))
        path = archive / hash

        upload = False
        with self.lock:
            if hash not in self.seen_hashes:
                self.seen_hashes.add(hash)
                upload = True

        if upload and not path.exists():
            logger.info(f'Uploading to S3 (hash={hash})')
            if self.gzip:
                data = gzip.compress(data)
            if not self.dry_run:
                path.write_bytes(data)

        return path.as_url()


def read_seeds_file(seeds_path: str) -> list[str]:
    with open(seeds_path, 'r') as file:
        try:
            first = json.loads(file.readline())
            if first['format'] != 'json-pages-1.0':
                raise ValueError(f'Incorrect format: {first["format"]}')
        except Exception:
            raise ValueError('Seeds file is not a Browsertrix "json-pages-1.0" file.')

        pages = (json.loads(line) for line in file if line != '')
        return [page['url'] for page in pages if page['seed']]


@dataclass
class RequestRecords:
    url: str
    records: list[ArcWarcRecord] = field(default_factory=list)
    request: ArcWarcRecord | None = None
    response: ArcWarcRecord | None = None
    response_body: bytes = b''
    last_index: int = 0
    warc_info: dict | None = None

    @property
    def redirect_target(self) -> str:
        if self.response:
            status = self.response.http_headers.get_statuscode()
            location = self.response.http_headers.get_header('location')
            if status.startswith('3') and location:
                return location

        return ''

    def add(self, record: ArcWarcRecord, index: int) -> None:
        self.records.append(record)
        self.last_index = index
        if record.rec_type == 'response':
            self.response = record
            self.response_body = record.content_stream().read()
        elif record.rec_type == 'request':
            self.request = record

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other) -> bool:
        return self is other


@dataclass
class RedirectChain:
    requests: list[RequestRecords] = field(default_factory=list)

    def add(self, request: RequestRecords) -> None:
        if request not in self.requests:
            self.requests.append(request)

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other) -> bool:
        return self is other


def parse_warc_fields(record: ArcWarcRecord) -> StatusAndHeaders:
    if record.rec_headers.get('content-type') != 'application/warc-fields':
        raise ValueError('Record does not have "application/warc-fields" content')

    parser = StatusAndHeadersParser([], verify=False)
    return parser.parse(record.content_stream(), 'WARC/1.1').headers


def each_redirect_chain(warc_path: str, seeds: set[str]) -> Generator[RedirectChain, None, None]:
    max_open_request_age = 250
    open_requests: dict[str, RequestRecords] = {}
    open_redirects: dict[str, RedirectChain] = {}

    warc_info = {'warc_name': Path(warc_path).name}

    with open(warc_path, 'rb') as warc_file:
        for index, record in enumerate(ArchiveIterator(warc_file)):
            if record.rec_type == 'warcinfo':
                info = parse_warc_fields(record)
                if 'software' in info:
                    warc_info['crawler'] = info['software']
                continue

            target = record.rec_headers.get_header('WARC-Target-URI')
            request = open_requests.get(target)
            if request is None and target not in seeds and target not in open_redirects:
                continue

            if not request:
                request = RequestRecords(target, warc_info=warc_info)
                open_requests[target] = request

            request.add(record, index)

            chain = open_redirects.get(target)
            if not chain:
                chain = RedirectChain()
                open_redirects[target] = chain
            chain.add(request)

            if request.redirect_target:
                open_redirects[request.redirect_target] = chain

            if index >= max_open_request_age:
                for chain in set(open_redirects.values()):
                    last = chain.requests[-1]
                    if last.last_index + max_open_request_age < index and not last.redirect_target:
                        for request in chain.requests:
                            del open_redirects[request.url]

                        yield chain

        for chain in set(open_redirects.values()):
            yield chain


def get_response_media(response: ArcWarcRecord) -> tuple[str, str]:
    """Extract media type and media type parameters from a memento."""
    media, *parameters = response.http_headers.get('Content-Type', '').split(';')

    # Clean up media type
    media = media.strip().lower()
    if not MEDIA_TYPE_EXPRESSION.match(media):
        url = response.rec_headers.get('WARC-Target-URI')
        logger.info('Unknown media type "%s" for "%s"', media, url)
        media = ''

    # Clean up whitespace, remove empty parameters, etc.
    clean_parameters = (param.strip() for param in parameters)
    parameters = [param for param in clean_parameters if param]
    parameter_string = '; '.join(parameters)

    return media, parameter_string


def format_version(chain: RedirectChain) -> dict:
    final = chain.requests[-1]
    final_response = final.response
    assert final_response

    iso_date = chain.requests[0].response.rec_headers.get_header('WARC-Date')
    assert len(iso_date) >= 20 and iso_date.endswith('Z')

    metadata = {
        **final.warc_info,
        'warc_page_id': final.response.rec_headers.get('WARC-Page-ID'),
        'warc_record_ids': [r.rec_headers.get('WARC-Record-ID')
                            for requests in chain.requests
                            for r in requests.records],
    }

    record_metadata = final.response.rec_headers.get('WARC-JSON-Metadata')
    if record_metadata:
        try:
            metadata['warc_record_meta'] = json.loads(record_metadata)
        except Exception as error:
            logger.warning(f'Error parsing WARC-JSON-Metadata: {error}')

    for request in chain.requests:
        if request.request:
            metadata['user_agent'] = request.request.http_headers.get_header('user-agent')
            break

    # If there were redirects, list every URL in the chain of requests.
    if len(chain.requests) > 1:
        metadata['redirected_url'] = final.url
        metadata['redirects'] = [r.url for r in chain.requests]
        metadata['statuses'] = [r.response.http_headers.get_statuscode() for r in chain.requests]

    media_type, media_type_parameters = get_response_media(final_response)
    if not media_type or media_type in SNIFF_MEDIA_TYPES:
        media_type = sniff_media_type(final_response.content_stream().read(), media_type)

    title = ''
    if media_type in HTML_MEDIA_TYPES:
        encoding = detect_encoding(final.response_body, final.response.http_headers)
        title = utils.extract_title(final.response_body, encoding)
    elif media_type in PDF_MEDIA_TYPES or final.response_body.startswith(b'%PDF-'):
        title = utils.extract_pdf_title(final.response_body) or title

    return dict(
        url=chain.requests[0].url,
        capture_time=iso_date,
        body_url=None,
        body_hash=utils.hash_content(final.response_body),
        status=int(final.response.http_headers.get_statuscode()),
        headers=dict(final.response.http_headers.headers),
        source_type='edgi_crawl_v0',
        source_metadata=metadata,
        media_type=media_type or None,
        content_length=len(final.response_body),
        title=title,
    )


def format_and_preupload(redirect_chains: Iterable[RedirectChain], storage: S3HashStore) -> Generator[dict, None, None]:
    for chain in redirect_chains:
        version = format_version(chain)
        url = storage.store(
            chain.requests[-1].response_body,
            hash=version['body_hash'],
            content_type=version['media_type']
        )
        version['body_url'] = url
        yield version


def main():
    sentry_sdk.init()

    parser = ArgumentParser()
    parser.add_argument('warc_path', help='Path to WARC file to extract data from')
    parser.add_argument('--seeds', help='List of seed URLs to extract from WARC.')
    parser.add_argument('--archive-s3', help='S3 bucket to upload raw response bodies to.', required=True)
    parser.add_argument('--dry-run', action='store_true', help='Do not actually upload results')
    parser.add_argument('--limit', type=int, help='Stop after this many records')
    args = parser.parse_args()

    if not args.seeds:
        print('For now, you *must* supply a Browsertrix `pages.jsonl` file as the `--seeds` option')
        sys.exit(1)

    db_client = db.Client.from_env()

    storage = S3HashStore(
        args.archive_s3,
        gzip=True,
        dry_run=args.dry_run,
        extra_args={'ACL': 'public-read'}
    )

    seeds = set(read_seeds_file(args.seeds))
    total = len(seeds)

    records = each_redirect_chain(args.warc_path, seeds=seeds)
    versions = format_and_preupload(records, storage)
    if args.limit:
        versions = islice(versions, args.limit)
        total = args.limit

    import_ids = []
    with tqdm_logging_redirect(versions, total=total) as progress:
        if args.dry_run:
            for version in progress:
                print(json.dumps(version))
        else:
            import_ids = db_client.add_versions(
                # HACK: this generator expression comprehension is here to make
                # sure the sequence of versions has no known length. (tqdm's
                # iterator has a length, which causes problems for batching).
                # https://github.com/pytoolz/toolz/issues/602
                (version for version in progress),
                create_pages=False,
                skip_unchanged_versions=False
            )

    if len(import_ids):
        print('Waiting for import jobs to complete...', file=sys.stderr)
        errors = db_client.monitor_import_statuses(import_ids)
        total = sum(len(job_errors) for job_errors in errors.values())
        if total > 0:
            print('Import job errors:', file=sys.stderr)
            for job_id, job_errors in errors.items():
                print(f'  {job_id}: {len(job_errors):>3} errors {job_errors}', file=sys.stderr)
            print(f'  Total: {total:>3} errors', file=sys.stderr)


if __name__ == '__main__':
    main()
