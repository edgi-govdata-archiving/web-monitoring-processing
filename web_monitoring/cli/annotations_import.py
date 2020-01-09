#!/usr/bin/env python
import csv
from docopt import docopt
import logging
import os
import re
from tqdm import tqdm
from web_monitoring import db

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

DIFF_URL_REGEX = re.compile(r'^.*/page/(.*)/(.*)\.\.(.*)')
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

class AnnotationAttributeInfo:
    def __init__(self, column_names, json_key):
        self.column_names = column_names
        self.json_key = json_key

class CsvSchemaError(Exception):
    ...

# If column names ever change while leaving the value semantics intact,
# add the new  name to the correct list of column names here
BOOL_ANNOTATION_ATTRIBUTES = [AnnotationAttributeInfo(*info) for info in [
    (['Language alteration'],
     'language_alteration'),
    (['Link change/addition/removal'],
     'link_change'),
    (['Repeated Change across many pages or a domain'],
     'repeated_change'),
    (['Alteration within sections of a webpage'],
     'alteration_within_sections'),
    (['Alteration, removal, or addition of entire section(s) of a webpage'],
     'alteration_entire_sections'),
    (['Alteration, removal, or addition of an entire webpage or document'],
     'alteration_entire_webpage_or_document'),
    (['Overhaul, removal, or addition of an entire website'],
     'alteration_entire_website'),
    (['Alteration, removal, or addition of datasets'],
     'alteration_dataset')]]

STRING_ANNOTATION_ATTRIBUTES = [AnnotationAttributeInfo(*info) for info in [
    (['Is this primarily a content or access change (or both)?'],
     'content_or_access_change'),
    (['Brief Description'],
     'brief_description'),
    (['Topic 1'],
     'topic_1'),
    (['Subtopic 1a'],
     'subtopic_1a'),
    (['Subtopic 1b'],
     'subtopic_1b'),
    (['Topic 2'],
     'topic_2'),
    (['Subtopic 2a'],
     'subtopic_2a'),
    (['Subtopic 2b'],
     'subtopic_2b'),
    (['Topic 3'],
     'topic_3'),
    (['Subtopic 3a'],
     'subtopic_3a'),
    (['Subtopic 3b'],
     'subtopic_3b'),
    (['Any keywords to monitor (e.g. for term analyses)?'],
     'keywords_to_monitor'),
    (['Further Notes'],
     'further_notes'),
    (['Ask/tell other working groups?'],
     'ask_tell_other_working_groups'),

    # Including this so that we can eventually map it to
    # users in the database
    (['Who Found This?'],
     'annotation_author')]]

def get_attribute_value(attribute_info, csv_row):
    for column_name in attribute_info.column_names:
        if column_name in csv_row:
            return csv_row[column_name].strip()

    # Despite being raised in a row-level function, this error means that the
    # whole sheet is missing a column, so we don't catch and allow it to crash
    raise CsvSchemaError(f'Expected to find one of {attribute_info.column_names} '
                         f'in {csv_row.keys()}')

def create_annotation(csv_row, is_important_changes):
    annotation = {}

    for attribute_info in BOOL_ANNOTATION_ATTRIBUTES:
        attribute_value = get_attribute_value(attribute_info, csv_row)
        annotation[attribute_info.json_key] = attribute_value == '1'
    for attribute_info in STRING_ANNOTATION_ATTRIBUTES:
        attribute_value = get_attribute_value(attribute_info, csv_row)
        annotation[attribute_info.json_key] = attribute_value

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

    client = db.Client.from_env()
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
                response = client.add_annotation(**change_ids,
                                                 annotation=annotation)
                logger.debug(response)
            except db.WebMonitoringDbError as e:
                logger.warning(
                    f'failed to post annotation for row {row} with error: {e}')
