import csv
from docopt import docopt
import logging
import os
import re
from tqdm import tqdm
from web_monitoring import db
from web_monitoring.db import Client

logger = logging.getLogger(__name__)
log_level = os.getenv('LOG_LEVEL', 'WARNING')
logger.setLevel(logging.__dict__[log_level])

class DictReaderStrip(csv.DictReader):
    @property
    def fieldnames(self):
        return [name.strip() for name in super().fieldnames]

def read_csv(csv_path):
    with open(csv_path, newline='') as csvfile:
        reader = DictReaderStrip(csvfile)
        for row in reader:
            yield row

DIFF_URL_REGEX = re.compile('^.*\/page\/(.*)/(.*)\.\.(.*)')
def find_change_ids(csv_row):
    diff_url = csv_row['Last Two - Side by Side']
    regex_result = DIFF_URL_REGEX.match(diff_url)
    if regex_result:
        (page_id, from_version_id, to_version_id) = regex_result.groups()
        return {'page_id': page_id,
                'from_version_id': from_version_id,
                'to_version_id': to_version_id}
    else:
        return None

def create_annotation(csv_row, is_important_changes):
    # Missing step: capture info from the "Who Found This" column and map it to
    # the author's user record in the database eventually, but that requires a
    # change to the API

    bool_keys = [
        'Language alteration',
        'Content change/addition/removal',
        'Link change/addition/removal',
        'Repeated Change across many pages or a domain',
        'Alteration within sections of a webpage',
        'Alteration, removal, or addition of entire section(s) of a webpage',
        'Alteration, removal, or addition of an entire webpage or document',
        'Overhaul, removal, or addition of an entire website',
        'Alteration, removal, or addition of datasets'
    ]
    string_keys = [
        'Is this primarily a content or access change (or both)?',
        'Brief Description',
        'Topic 1',
        'Subtopic 1a',
        'Subtopic 1b',
        'Topic 2',
        'Subtopic 2a',
        'Subtopic 2b',
        'Topic 3',
        'Subtopic 3a',
        'Subtopic 3b',
        'Any keywords to monitor (e.g. for term analyses)?',
        'Further Notes',
        'Ask/tell other working groups?',

        'Who Found This?' # Including this so that we can eventually map it to
                          # users in the database
    ]
    bools_dict = {k: csv_row[k].strip() == '1' for k in bool_keys}
    strings_dict = {k: csv_row[k].strip() for k in string_keys}
    annotation = {**bools_dict, **strings_dict}

    # This will need additional logic to determine the actual sheet schema
    annotation['annotation_schema'] = 'edgi_analyst_v2'

    significance = 0.0
    if is_important_changes:
        importance_significance_mapping = {
            'low': 0.5,
            'medium': 0.75,
            'high': 1.0
        }
        row_importance = csv_row['Importance?'].lower().strip()
        significance = importance_significance_mapping.get(row_importance, 0.0)
    annotation['significance'] = significance

    return annotation

def post_annotation(change_ids, annotation):
    client = Client.from_env()
    response = client.add_annotation(annotation=annotation,
                                     page_id=change_ids['page_id'],
                                     to_version_id=change_ids['to_version_id'],
                                     from_version_id=change_ids['from_version_id'])
    return response

def main():
    doc = """Add analyst annotations from a csv file to the Web Monitoring db.

Usage:
path/to/annotations_import.py <csv_path> [--is_important_changes]

Options:
--is_important_changes  Was this CSV generated from an Important Changes sheet?
"""
    arguments = docopt(doc)
    is_important_changes = arguments['--is_important_changes']
    csv_path = arguments['<csv_path>']

    # Missing step: Analyze CSV to determine spreadsheet schema version
    for row in tqdm(read_csv(csv_path), unit=' rows'):
        change_ids = find_change_ids(row)
        annotation = create_annotation(row, is_important_changes)
        if not change_ids:
            logger.warning(f'failed to extract IDs from {row}')
        if not annotation:
            logger.warning(f'failed to extract annotation data from {row}')
        if change_ids and annotation:
            try:
                response = post_annotation(change_ids, annotation)
                logger.debug(response)
            except db.WebMonitoringDbError as e:
                logger.warning(
                    f'failed to post annotation for row {row} with error: {e}')

if __name__ == '__main__':
    main()
