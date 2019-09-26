import csv
from docopt import docopt
import re
from web_monitoring.db import Client

def read_csv(csv_path):
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield row

def find_change(csv_row):
    diff_url = csv_row['Last Two - Side by Side']
    url_regex = re.compile('^.*\/page\/(.*)/(.*)\.\.(.*)')
    regex_result = url_regex.match(diff_url)
    (page_id, from_version_id, to_version_id) = regex_result.groups()
    
    cli = Client.from_env()
    change = cli.get_change(page_id=page_id, from_version_id=from_version_id, to_version_id=to_version_id)

def main():
    doc = """Add analyst annotations from a csv file to the Web Monitoring database.

Usage:
annotations_import <csv_path>
"""
    arguments = docopt(doc)
    csv_path = arguments['<csv_path>']

    # Missing step: Analyze CSV to determine spreadsheet schema version
    for row in read_csv(csv_path):
        change = find_change(row)


if __name__ == '__main__':
    main()