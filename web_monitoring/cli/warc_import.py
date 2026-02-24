from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import dateutil.parser
from functools import lru_cache
from itertools import islice
import json
import logging
from pathlib import Path
import sys
from typing import Any, Generator
from urllib.parse import urljoin
import sentry_sdk
from tqdm.contrib.logging import tqdm_logging_redirect
from warcio import ArchiveIterator
from warcio.recordloader import ArcWarcRecord, StatusAndHeadersParser, StatusAndHeaders
import yaml
from .. import db
from .. import utils
from ..media import HTML_MEDIA_TYPES, PDF_MEDIA_TYPES, find_media_type
from ..utils import S3HashStore, detect_encoding, matchable_url, normalize_url


logger = logging.getLogger(__name__)


def read_browsertrix_pages_seeds(seeds_path: str) -> list[str]:
    with open(seeds_path, 'r') as file:
        try:
            first = json.loads(file.readline())
            if first['format'] != 'json-pages-1.0':
                raise ValueError(f'Incorrect format: {first["format"]}')
        except Exception:
            raise ValueError('Seeds file is not a Browsertrix "json-pages-1.0" file.')

        pages = (json.loads(line) for line in file if line != '')
        return [normalize_url(page['url'])
                for page in pages
                if page['seed']]


def read_browsertrix_config_seeds(seeds_path: str) -> list[str]:
    with open(seeds_path, 'r') as file:
        data = yaml.safe_load(file)
        seeds = data.get('seeds')
        if isinstance(seeds, list):
            return [normalize_url(seed if isinstance(seed, str) else seed['url'])
                    for seed in seeds]
        else:
            raise ValueError(f'Seeds file is missing `seeds` key that is an array of URL strings: "{seeds_path}"')


def read_seeds_file(seeds_path: str) -> list[str]:
    if seeds_path.endswith('.yaml') or seeds_path.endswith('.yml'):
        return read_browsertrix_config_seeds(seeds_path)
    elif seeds_path.endswith('.json') or seeds_path.endswith('.jsonl'):
        return read_browsertrix_pages_seeds(seeds_path)
    else:
        raise ValueError(f'Unknown seed file type: "{seeds_path}"')


@dataclass
class HttpExchange:
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
                return normalize_url(urljoin(self.url, location))
            # Amazon WAF browser challenge works reloading the same URL with a
            # cookie. Treat this like a redirect; we should have captured the
            # second request to the same URL.
            elif status == '202' and self.response.http_headers.get_header('x-amzn-waf-action') == 'challenge':
                logger.warning(f'Handling Amazon WAF challenge: "{self.url}"')
                return self.url

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
    requests: list[HttpExchange] = field(default_factory=list)

    def add(self, request: HttpExchange) -> None:
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


@dataclass
class RecordIndexEntry:
    id: str
    timestamp: datetime
    uri: str
    type: str
    file: str
    offset: int
    length: int


class HttpExchangeIndexEntry:
    request: RecordIndexEntry | None = None
    response: RecordIndexEntry | None = None
    records: list[RecordIndexEntry]
    timestamp: datetime
    uri: str

    def __init__(self, records: list[RecordIndexEntry] = []):
        self.records = []
        for record in records:
            self.add(record)

    def add(self, record: RecordIndexEntry):
        if len(self.records) == 0:
            self.timestamp = record.timestamp
            self.uri = record.uri
        elif record.timestamp != self.timestamp:
            raise ValueError('Records has a different timestamp from others in this request')

        self.records.append(record)
        if record.type == 'request':
            self.request = record
        elif record.type == 'response':
            self.response = record


def each_redirect_chain(warcs: list[str], seeds: set[str]) -> Generator[RedirectChain, None, None]:
    record_index: dict[str, RecordIndexEntry] = {}
    exchanges_by_url: dict[str, list[HttpExchangeIndexEntry]] = defaultdict(list)
    indexable = set(['request', 'response'])  # TODO: support metadata, revisit
    # This only supports one warcinfo record per WARC; not technically correct.
    warc_infos: dict[str, dict[str, Any]] = {}

    for warc in warcs:
        warc_path = Path(warc).absolute()
        warc_info: dict[str, Any] = {'warc_name': warc_path.name}
        warc_infos[warc] = warc_info
        if warc_path.parent.name == 'archive' and warc_path.parent.parent.parent.name == 'collections':
            # Not sure if we want this.
            # warc_info['warc_name'] = str(warc_path.relative_to(warc_path.parent.parent.parent))
            warc_info['crawl'] = warc_path.parent.parent.name

        logger.info(f'Indexing {warc_path}...')
        with warc_path.open('rb') as warc_file:
            reader = ArchiveIterator(warc_file)
            for record in reader:
                if record.rec_type == 'warcinfo':
                    # TODO: There might be other stuff we want to read in WARCs
                    # generated from non-Browsertrix sources.
                    info = parse_warc_fields(record)
                    if 'software' in info:
                        warc_info['crawler'] = info['software']

                elif record.rec_type in indexable:
                    entry = RecordIndexEntry(
                        id=record.rec_headers.get('WARC-Record-ID'),
                        timestamp=dateutil.parser.parse(record.rec_headers.get('WARC-Date')).astimezone(timezone.utc),
                        uri=normalize_url(record.rec_headers.get('WARC-Target-URI')),
                        type=record.rec_type,
                        file=warc,
                        offset=reader.get_record_offset(),
                        length=reader.get_record_length()
                    )
                    record_index[entry.id] = entry

                    # This is a bit special to Browsertrix in that it gives the
                    # same timestamp to all records associated with the same
                    # HTTP exchange. Make it easy to join them together. This
                    # is almost certainly not a safe, generic assumption about
                    # WARCs from other crawlers!
                    #
                    # TODO: handle looking up related entries in record_index via
                    # WARC-Concurrent-To, WARC-Refers-To, and in request_index via
                    # WARC-Refers-To-Target-URI, WARC-Refers-To-Date
                    exchanges = exchanges_by_url[matchable_url(entry.uri)]
                    for existing in exchanges:
                        if existing.timestamp == entry.timestamp:
                            existing.add(entry)
                            break
                    else:
                        exchanges.append(HttpExchangeIndexEntry([entry]))

                # TODO: optimize by tracking the last few records and yielding
                # immediately for any request/response pairs that do not redirect.
                # This won't work if we are expecting metadata records, but
                # Browsertrix (our only source right now) does not produce them.
                # (It does produce related resource records that are a bit more
                # complicated to match up, and they don't have anything we want
                # right now.)

    logger.info('Yielding matching records for seeds...')
    for seed in seeds:
        # FIXME: This approach expects each seed will only be requested once in
        # the collection of WARCs being examined, which is not necessarily
        # accurate. It's good enough for WARCs we create with Browsertrix, but
        # not other sources that may have been collected differently.
        chain = RedirectChain()
        next_url = seed
        next_timestamp = datetime(1, 1, 1, tzinfo=timezone.utc)
        seen_entries = []
        while next_url:
            match_url = matchable_url(next_url)
            warc_exchanges = exchanges_by_url[match_url]
            if not warc_exchanges and match_url.startswith('http://'):
                warc_exchanges = exchanges_by_url['https' + match_url[4:]]

            if not warc_exchanges:
                if next_url == seed:
                    logger.warning(f'No WARC records for seed: "{seed}"')
                else:
                    logger.error(f'Incomplete redirect chain for seed: "{seed}" (no records for "{next_url}")')
                chain = None
                next_url = None
                break

            warc_exchange = next(
                (
                    e for e in warc_exchanges
                    if e.timestamp > next_timestamp and e not in seen_entries
                ),
                warc_exchanges[-1]
            )
            if warc_exchange in seen_entries:
                raise RuntimeError(f'Circular redirect detected for "{warc_exchange.uri}" at {warc_exchange.timestamp}')
            elif not warc_exchange.response:
                raise RuntimeError(f'Request index entry missing response record for "{warc_exchange.uri}" at {warc_exchange.timestamp}')
            seen_entries.append(warc_exchange)

            warc_info = warc_infos[warc_exchange.response.file]
            exchange = HttpExchange(warc_exchange.uri, warc_info=warc_info)
            response_record, body = extract_record(warc_exchange.response.file, warc_exchange.response.offset)
            exchange.add(
                response_record,
                index=0,
                offset=warc_exchange.response.offset,
                length=warc_exchange.response.length,
                body=body
            )
            if warc_exchange.request:
                request_record, _ = extract_record(warc_exchange.request.file, warc_exchange.request.offset)
                exchange.add(
                    request_record,
                    index=0,
                    offset=warc_exchange.request.offset,
                    length=warc_exchange.request.length,
                    body=None
                )
            chain.add(exchange)
            next_url = exchange.redirect_target
            next_timestamp = warc_exchange.timestamp

        if chain:
            yield chain


def format_version(chain: RedirectChain) -> dict:
    final = chain.requests[-1]
    final_response = final.response
    assert final_response

    iso_date = chain.requests[0].response.rec_headers.get_header('WARC-Date')
    assert len(iso_date) >= 20 and iso_date.endswith('Z')

    metadata = {
        **final.warc_info,
        # 'warc_page_id': final.response.rec_headers.get('WARC-Page-ID'),
        'warc_records': [meta
                         for requests in chain.requests
                         for meta in requests.metadata
                         if meta['type'] == 'response'],
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

    media_type, _ = find_media_type(final_response.http_headers,
                                    final.response_body,
                                    url=chain.requests[0].url)

    title = ''
    if media_type in HTML_MEDIA_TYPES:
        encoding = detect_encoding(final.response_body, final.response.http_headers)
        title = utils.extract_html_title(final.response_body, encoding)
    # FIXME: remove the sniffing here; I think this is an anachronism from
    # before we had better sniffing support when setting `media_type`.
    elif media_type in PDF_MEDIA_TYPES or final.response_body.startswith(b'%PDF-'):
        title = utils.extract_pdf_title(final.response_body) or title

    return dict(
        url=chain.requests[0].url,
        capture_time=iso_date,
        body_url=None,
        body_hash=utils.hash_content(final.response_body),
        status=int(final.response.http_headers.get_statuscode()),
        headers={k.lower(): v for k, v in final.response.http_headers.headers},
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
    parser.add_argument('warc_path', nargs='+', help='Path to WARC file to extract data from')
    parser.add_argument('--seeds', help='List of seed URLs to extract from WARC.')
    parser.add_argument('--archive-s3', help='S3 bucket to upload raw response bodies to.', required=True)
    parser.add_argument('--dry-run', action='store_true', help='Do not actually upload results')
    parser.add_argument('--limit', type=int, help='Stop after this many records')
    parser.add_argument('--update', action='store_const', default='skip', const='replace',
                        help='Update existing records in DB instead of skipping')
    parser.add_argument('--progress', action='store_true',
                        help='Show progress bar (only for non-interactive '
                             'sessions; it shows automatically on a TTY).')
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
    hide_progress = False if args.progress else None
    with tqdm_logging_redirect(versions, total=total, disable=hide_progress) as progress:
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
