from argparse import ArgumentParser
from dataclasses import dataclass, field
from functools import lru_cache
import gzip
from itertools import islice
import json
import logging
from pathlib import Path
import re
import sys
import threading
from typing import Any, Generator
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

        if upload and not self.dry_run and not path.exists():
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
    metadata: list[dict] = field(default_factory=list)
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

    def add(self, record: ArcWarcRecord, index: int, offset: int, length: int, body: bytes | None) -> None:
        self.last_index = index
        meta = {
            'id': record.rec_headers.get('WARC-Record-ID'),
            'type': record.rec_type,
            'offset': offset,
            'length': length,
        }
        if record.rec_type == 'request':
            self.request = record
            self.records.insert(0, record)
            self.metadata.insert(0, meta)
        elif record.rec_type == 'response':
            assert body is not None, f'Response record had no body ({meta["id"]})'
            self.response = record
            self.response_body = body
            position = 1 if self.request else 0
            self.records.insert(position, record)
            self.metadata.insert(position, meta)
        else:
            self.records.append(record)
            self.metadata.append(meta)

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


@lru_cache(maxsize=256)
def extract_record(warc: str, offset: int) -> tuple[ArcWarcRecord, bytes]:
    with open(warc, 'rb') as file:
        file.seek(offset)
        record = next(iter(ArchiveIterator(file)))
        return record, record.content_stream().read()


def each_redirect_chain(warc: str, seeds: set[str]) -> Generator[RedirectChain, None, None]:
    max_open_request_age = 250
    # TODO: maybe should be using the WARC-Concurrent-To header to match up
    # request & response records? Browsertrix reliably does this, but it's not
    # required and I don't know about other crawlers.
    open_requests: dict[str, RequestRecords] = {}
    # TODO: should we use the WARC-Page-ID header for this? It's non-standard,
    # but seems to be designed sort of for this purpose in Browsertrix. Info:
    # - https://github.com/webrecorder/browsertrix-crawler/issues/429#issuecomment-2389762045
    # - https://github.com/webrecorder/browsertrix/issues/1588#issuecomment-1998128354
    open_redirects: dict[str, RedirectChain] = {}
    seen_seeds: set[str] = set()
    warc_info: dict[str, Any] = {}

    response_index = {}

    warc_path = Path(warc).absolute()
    warc_info['warc_name'] = warc_path.name
    if warc_path.parent.name == 'archive' and warc_path.parent.parent.parent.name == 'collections':
        # Not sure if we want this.
        # warc_info['warc_name'] = str(warc_path.relative_to(warc_path.parent.parent.parent))
        warc_info['crawl'] = warc_path.parent.parent.name

    with warc_path.open('rb') as warc_file:
        reader = ArchiveIterator(warc_file)
        for index, record in enumerate(reader):
            if record.rec_type == 'warcinfo':
                # TODO: There might be other stuff we want to read in WARCs
                # generated from non-Browsertrix sources.
                info = parse_warc_fields(record)
                if 'software' in info:
                    warc_info['crawler'] = info['software']
                continue

            target = record.rec_headers.get_header('WARC-Target-URI')
            body = record.content_stream().read()
            offset = reader.get_record_offset()

            if target and record.rec_type == 'response':
                response_index[target] = offset

            request = open_requests.get(target)
            if request is None and target not in seeds and target not in open_redirects:
                continue

            if not request:
                request = RequestRecords(target, warc_info=warc_info)
                open_requests[target] = request

            request.add(
                record,
                index=index,
                offset=offset,
                length=reader.get_record_length(),
                body=body
            )

            chain = open_redirects.get(target)
            if not chain:
                chain = RedirectChain()
                open_redirects[target] = chain
                seen_seeds.add(request.url)
            chain.add(request)

            if request.redirect_target:
                open_redirects[request.redirect_target] = chain

            if index >= max_open_request_age:
                for chain in set(open_redirects.values()):
                    last = chain.requests[-1]
                    if last.last_index + max_open_request_age < index:
                        for _ in range(10):
                            redirect = chain.requests[-1].redirect_target
                            if redirect:
                                offset = response_index.get(redirect)
                                if offset:
                                    target_record, body = extract_record(warc_path, offset)
                                    request = RequestRecords(redirect)
                                    request.add(target_record, last.last_index, offset=offset, length=0, body=body)
                                    chain.add(request)
                                    if request.redirect_target:
                                        open_redirects[request.redirect_target] = chain
                                else:
                                    break
                            else:
                                break

                        if chain.requests[-1].response and not chain.requests[-1].redirect_target:
                            for request in chain.requests:
                                del open_redirects[request.url]

                            yield chain

        for chain in set(open_redirects.values()):
            for _ in range(10):
                redirect = chain.requests[-1].redirect_target
                if redirect:
                    offset = response_index.get(redirect)
                    if offset:
                        target_record, body = extract_record(warc_path, offset)
                        request = RequestRecords(redirect)
                        request.add(target_record, last.last_index, offset=offset, length=0, body=body)
                        chain.add(request)
                    else:
                        break
                else:
                    break

            if not chain.requests[-1].response:
                logger.warning(f'Chain of requests had no final response (started with {chain.requests[0].url})')
            elif chain.requests[-1].redirect_target:
                logger.warning(f'Chain of requests ended with a redirect (started with {chain.requests[0].url})')
            else:
                yield chain

    # What's happening with not always getting as many chains as seeds?
    missing_seeds = seeds - seen_seeds
    new_seeds = seen_seeds - seeds
    if len(missing_seeds):
        logger.warning(f'{len(missing_seeds)} seed URLs did not have initial requests: {missing_seeds}')
    if len(new_seeds):
        logger.warning(f'{len(new_seeds)} URLs had initial requests but were not in seed list: {new_seeds}')


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
        'warc_records': [meta
                         for requests in chain.requests
                         for meta in requests.metadata],
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


def preupload(storage: S3HashStore, version: dict, body: bytes) -> tuple[dict, bytes]:
    url = storage.store(
        body,
        hash=version['body_hash'],
        content_type=version['media_type']
    )
    version['body_url'] = url
    return version, body


def main():
    sentry_sdk.init()

    parser = ArgumentParser()
    parser.add_argument('warc_path', help='Path to WARC file to extract data from')
    parser.add_argument('--seeds', help='List of seed URLs to extract from WARC.')
    parser.add_argument('--archive-s3', help='S3 bucket to upload raw response bodies to.', required=True)
    parser.add_argument('--dry-run', action='store_true', help='Do not actually upload results')
    parser.add_argument('--limit', type=int, help='Stop after this many records')
    parser.add_argument('--update', action='store_const', default='skip', const='replace',
                        help='Update existing records in DB instead of skipping')
    args = parser.parse_args()

    # TODO: we'll probably eventually want to support WARCs from IA or maybe
    # other sources. This will need
    if not args.seeds:
        print('For now, you *must* supply a Browsertrix `pages.jsonl` file as the `--seeds` option')
        sys.exit(1)

    db_client = db.Client.from_env()

    # TODO: preload some hashes from DB
    storage = S3HashStore(
        args.archive_s3,
        gzip=True,
        dry_run=args.dry_run,
        extra_args={'ACL': 'public-read'}
    )

    seeds = set(read_seeds_file(args.seeds))
    total = min(len(seeds), args.limit or float('inf'))

    chains = each_redirect_chain(args.warc_path, seeds=seeds)
    chains = utils.QuitSignal().stop_iteration(islice(chains, args.limit))

    versions = ((format_version(c), c.requests[-1].response_body)
                for c in chains)
    versions = (preupload(storage, version, body)
                for version, body in versions)

    import_ids = []
    with tqdm_logging_redirect(versions, total=total) as progress:
        if args.dry_run:
            for version, _ in progress:
                print(json.dumps(version))
        else:
            import_ids = db_client.add_versions(
                (version for version, _ in progress),
                create_pages=False,
                skip_unchanged_versions=False,
                update=args.update
            )

    if import_ids:
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
