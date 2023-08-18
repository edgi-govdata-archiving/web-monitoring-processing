#!/usr/bin/env python
import csv
from dataclasses import asdict, dataclass
from docopt import docopt
import json
import logging
import os
import re
from tqdm import tqdm
from web_monitoring import db

logger = logging.getLogger(__name__)
log_level = os.getenv('LOG_LEVEL', 'WARNING')
logger.setLevel(logging.__dict__[log_level])

# These were set in the original sheet by color coding, so we've just kept an
# index here. Not great. 2 types of data in this list:
#   1. (row_number, significance)
#   2. (start_row_number_inclusive, end_row_number_exclusive, significance)
#
# Row numbers are from the original sheet, which has 5 header rows and is
# 1-based. So subtract 6 from these to get the indexes used in this script.
V1_SIGNIFICANT_ROWS = [
    (6, 'low'),
    (8, 'low'),
    (14, 'high'),
    (37, 'low'),
    (41, 'high'),
    (45, 'low'),
    (47, 'low'),
    (48, 'low'),
    (49, 'low'),
    (51, 'low'),
    (55, 'high'),
    (58, 'low'),
    (61, 'low'),
    (68, 'low'),
    (83, 'low'),
    (89, 'high'),
    (93, 'low'),
    (94, 'low'),
    (95, 'low'),
    (96, 'low'),
    (98, 'low'),
    (99, 'low'),
    (100, 'low'),
    (101, 'low'),
    (105, 'low'),
    (106, 'low'),
    (107, 'low'),
    (108, 'low'),
    (109, 'low'),
    (110, 'low'),
    (111, 'low'),
    (112, 'low'),
    (113, 'low'),
    (114, 'low'),
    (115, 'low'),
    (116, 'low'),
    (117, 'low'),
    (118, 'low'),
    (119, 'low'),
    (120, 'low'),
    (121, 'low'),
    (122, 'low'),
    (123, 'low'),
    (124, 'low'),
    (125, 'low'),
    (126, 'low'),
    (127, 'low'),
    (128, 'low'),
    (129, 'low'),
    (130, 'low'),
    (131, 'low'),
    (132, 'low'),
    (133, 'low'),
    (134, 'low'),
    (135, 'low'),
    (136, 'low'),
    (137, 'low'),
    (141, 'high'),
    (145, 193, 'low'),
    (195, 'low'),
    (201, 'high'),
    (218, 238, 'low'),
    (263, 'high'),
    (266, 'high'),
    (271, 'low'),
    (273, 'low'),
    (274, 'low'),
    (276, 'low'),
    (277, 'low'),
    (279, 'low'),
    (283, 288, 'low'),
    (291, 'low'),
    (353, 'high'),
    (359, 'high'),
    (367, 'high'),
    (369, 372, 'high'),
    (416, 'high'),
    (428, 'high'),
    (432, 'low'),
    (447, 'high'),
    (451, 'high'),
    (469, 473, 'low'),
    (492, 'high'),
    (511, 'high'),
    (537, 542, 'high'),
    (549, 'high'),
    (552, 'low'),
    (553, 556, 'high'),
    (565, 570, 'high'),
    (572, 'low'),
    (573, 'low'),
    (666, 'low'),
    (683, 'low'),
    (684, 'low'),
    (1322, 'high'),
]


class DictReaderStrip(csv.DictReader):
    @property
    def fieldnames(self):
        return [name.strip() for name in super().fieldnames]


DIFF_URL_REGEX = re.compile(r'^.*/page/(.*)/(.*)\.\.(.*)')


def find_change_ids(diff_url):
    regex_result = DIFF_URL_REGEX.match(diff_url)
    if regex_result:
        (page_id, from_version_id, to_version_id) = regex_result.groups()
        return {'page_id': page_id,
                'from_version_id': from_version_id,
                'to_version_id': to_version_id}
    else:
        return None


def sheet_str(raw):
    return raw.strip()


def sheet_bool(raw):
    value = raw.lower().strip()
    if value == '1' or value == 'y' or value == 'yes' or value == 'x':
        return True
    elif value == '' or value == '0' or value == 'n' or value == 'no':
        return False
    else:
        raise TypeError(f'Bad value for boolean column: "{raw}"')


class CsvSchemaError(Exception):
    ...


@dataclass
class ResultRow:
    number: int
    change_ids: dict
    annotation: dict


class AnalystSheet:
    schema = ()
    row_offset = 0
    is_important = False

    def __init__(self, csv_path, is_important=False) -> None:
        self.path = csv_path
        self.is_important = is_important

    def parse(self):
        ids = set()
        for index, row in enumerate(self.read_csv()):
            row_number = index + self.row_offset + 1
            change_ids = self.get_change_ids(row)
            try:
                annotation = self.create_annotation(row, index)
                id = annotation['analyst_sheet_id']
                if id in ids:
                    logger.warn(f"DUPLICATE ID: {id}")
            except Exception as error:
                raise TypeError(f'Could not parse row {row_number}: {error}')

            if not change_ids:
                logger.warning(f'failed to extract IDs from row {row_number}')
            if not annotation:
                logger.warning(f'failed to extract annotation data from row {row_number}')

            yield ResultRow(row_number, change_ids, annotation)

    def get_change_ids(self, row):
        raise NotImplementedError()


class V1ChangesSheet(AnalystSheet):
    schema = (
        # Useful info for analysts, but mostly duplicative of data in DB.
        ('Checked (2-significant)', None),
        ('Index', None),
        ('Unique ID', 'analyst_sheet_id', sheet_str),
        ('Output Date/Time', None),
        ('Agency', None),
        ('Site Name', None),
        ('Page Name', None),
        ('URL', None),
        ('Page View URL', None),
        ('Last Two - Side by Side', None),
        ('Latest to Base - Side by Side', None),
        ('Date Found - Latest', None),
        ('Date Found - Base', None),
        ('Diff length', None),
        ('Diff hash', None),
        ('Text diff length', None),
        ('Text diff hash', None),
        ('Who Found This?', None),

        ('1', 'page_date_time_change_only', sheet_bool),  # Individual page: Date and time change only
        ('2', 'page_text_or_numeric_content_change', sheet_bool),  # Individual page: Text or numeric content removal or change
        ('3', 'page_image_content_change', sheet_bool),  # Individual page: Image content removal or change
        ('4', 'page_hyperlink_change', sheet_bool),  # Individual page: Hyperlink removal or change
        ('5', 'page_form_or_interactive_component_change', sheet_bool),  # Individual page: Text-box, entry field, or interactive component removal or change
        ('6', 'page_page_removal', sheet_bool),  # Individual page: Page removal (whether it has happened in the past or is currently removed)
        ('7', 'repeat_header_menu_change', sheet_bool),  # Repeated change: Header menu removal or change
        ('8', 'repeat_template_text_page_format_or_comment_change', sheet_bool),  # Repeated change: Template text, page format, or comment field removal or change
        ('9', 'repeat_footer_or_site_map_change', sheet_bool),  # Repeated change: Footer or site map removal or change
        ('10', 'repeat_sidebar_change', sheet_bool),  # Repeated change: Sidebar removal or change
        ('11', 'repeat_banner_or_ad_change', sheet_bool),  # Repeated change: Banner/advertisement removal or change
        ('12', 'repeat_scrolling_news', sheet_bool),  # Repeated change: Scrolling news/reports
        ('1', 'significance_energy_environment_climate', sheet_bool),  # Significant: Change related to energy, environment, or climate
        ('2', 'significance_language', sheet_bool),  # Significant: Language is significantly altered
        ('3', 'significance_content_removed', sheet_bool),  # Significant: Content is removed
        ('4', 'significance_page_removed', sheet_bool),  # Significant: Page is removed
        ('5', 'significance_not_significant', sheet_bool),  # Significant: Insignificant
        ('6', 'significance_repeated_insignificant_change', sheet_bool),  # Significant: Repeated Insignificant
        ('Further Notes', 'notes', sheet_str),
        ('Choose from drop down menu', 'broad_topic', sheet_str),  # Broad Topic
        ('', 'keywords', str),  # Keywords

        # There are extra notes in the second of these two columns. Sometimes
        # they are accidentally put in the first one, so they should be merged.
        ('Leave blank (used on Patterns sheet)', 'notes_2_a', sheet_str),
        ('', 'notes_2_b', sheet_str),

        # Ignore this column
        ('', None),
    )

    row_offset = 5

    def read_csv(self):
        with open(self.path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for i in range(self.row_offset):
                raw_headers = next(reader)
            expected_headers = [header[0] for header in self.schema]
            actual_headers = raw_headers[0:len(expected_headers)]
            if actual_headers != expected_headers:
                raise CsvSchemaError(f'Sheet did not have expected v1 header row!\n  Expected: {expected_headers}\n    Actual: {raw_headers}')
            # for row in reader:
            #     yield {
            #         header[1] if len(header) > 1 else header[0]: row[index]
            #         for index, header in enumerate(self.headers)
            #     }
            yield from reader

    def get_change_ids(self, row):
        # FIXME: don't use index here
        return find_change_ids(row[9])

    def create_annotation(self, csv_row, row_index):
        annotation = {
            'annotation_schema': 'edgi_analyst_v1',
            'annotation_author': csv_row[17],
        }
        for index, field in enumerate(self.schema):
            value = csv_row[index]
            key = field[1]
            if key:
                annotation[key] = field[2](value)

        annotation['notes_2'] = f"{annotation['notes_2_a']} {annotation['notes_2_b']}".strip()
        del annotation['notes_2_a']
        del annotation['notes_2_b']

        significance = 0.0
        if self.is_important:
            significance = self.get_row_significance(row_index)
        annotation['significance'] = significance

        return annotation

    def get_row_significance(self, row_number):
        value = None
        offset = self.row_offset + 1
        for candidate in V1_SIGNIFICANT_ROWS:
            if row_number == candidate[0] - offset:
                value = candidate[1] if len(candidate) == 2 else candidate[2]
                break
            elif len(candidate) == 3 and row_number > candidate[0] - offset and row_number < candidate[1] - offset:
                value = candidate[2]
                break
        if value == 'low':
            return 0.5
        elif value == 'high':
            return 1.0
        else:
            return 0.75


class AnnotationAttributeInfo:
    def __init__(self, column_names, json_key):
        self.column_names = column_names
        self.json_key = json_key


class V2ChangesSheet(AnalystSheet):
    row_offset = 1

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

        # Skipping this column; not relevant for archival purposes
        # (['Ask/tell other working groups?'],
        #  'ask_tell_other_working_groups'),

        # Including this so that we can eventually map it to
        # users in the database
        (['Who Found This?'],
        'annotation_author'),

        (['Unique ID'],
         'analyst_sheet_id')]]

    def read_csv(self):
        with open(self.path, newline='') as csvfile:
            for _ in range(self.row_offset - 1):
                csvfile.readline()
            reader = DictReaderStrip(csvfile)
            # FIXME: assert schema matches?
            for row in reader:
                yield row

    def get_change_ids(self, row):
        return find_change_ids(row['Last Two - Side by Side'])

    def get_attribute_value(self, attribute_info, csv_row):
        for column_name in attribute_info.column_names:
            if column_name in csv_row:
                return csv_row[column_name].strip()

        # Despite being raised in a row-level function, this error means that the
        # whole sheet is missing a column, so we don't catch and allow it to crash
        raise CsvSchemaError(f'Expected to find one of {attribute_info.column_names} '
                             f'in {csv_row.keys()}')

    def create_annotation(self, csv_row, _row_index):
        annotation = {}

        for attribute_info in self.BOOL_ANNOTATION_ATTRIBUTES:
            attribute_value = self.get_attribute_value(attribute_info, csv_row)
            annotation[attribute_info.json_key] = sheet_bool(attribute_value)
        for attribute_info in self.STRING_ANNOTATION_ATTRIBUTES:
            attribute_value = self.get_attribute_value(attribute_info, csv_row)
            annotation[attribute_info.json_key] = attribute_value

        # This will need additional logic to determine the actual sheet schema
        annotation['annotation_schema'] = 'edgi_analyst_v2'

        significance = 0.0
        if self.is_important:
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
path/to/annotations_import.py <csv_path> [options]

Options:
--is_important_changes  Was this CSV generated from an Important Changes sheet?
--commit                Send annotatins to DB
--schema <version>      Should be 'v1' or 'v2'.
"""
    arguments = docopt(doc)
    is_important_changes = arguments['--is_important_changes']
    commit = arguments['--commit']
    schema_version = arguments.get('--schema') or 'v2'
    csv_path = arguments['<csv_path>']

    if schema_version == 'v1':
        sheet = V1ChangesSheet(csv_path, is_important_changes)
    elif schema_version == 'v2':
        sheet = V2ChangesSheet(csv_path, is_important_changes)
    else:
        logger.error(f'Unknown schema: "{schema_version}"')
        exit(1)

    client = db.Client.from_env()
    for row in tqdm(sheet.parse(), unit=' rows'):
        if row.change_ids and row.annotation:
            if commit:
                try:
                    response = client.add_annotation(**row.change_ids,
                                                     annotation=row.annotation)
                    logger.debug(response)
                except db.WebMonitoringDbError as e:
                    logger.warning(
                        f'failed to post annotation for row {row} with error: {e}')
            else:
                print(json.dumps(asdict(row)))
