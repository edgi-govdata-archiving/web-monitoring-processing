import argparse
import requests
from urllib.parse import urlparse
<<<<<<< HEAD
from web_monitoring.db import Client
=======
from web_monitoring.db import get_version_uri
>>>>>>> origin/machine_learning_work
import pandas as pd
import os
import time
from docopt import docopt
<<<<<<< HEAD
import warnings
=======
>>>>>>> origin/machine_learning_work

def parse_url(url):
    """
    Parse Versionista change urls to get version ids
    https://versionista.com/{site id}/{page id}/{to version id}:{from version id}

    Parameters
    ----------
    url : string

    Returns
    -------
<<<<<<< HEAD
    ids : dictionary
        Site, page, and version ids of the from and two versions extracted from the url
    """
    try:
        result = urlparse(url)
        ids = [element for element in result.path.split('/') if element]
        site_id = ids[0]
        page_id = ids[1]
        version_ids = ids[2].split(':')
        to_version_id = version_ids[0]
        from_version_id = version_ids[1]
        ids = {'site_id': site_id, 'page_id': page_id,
               'from_version_id': from_version_id, 'to_version_id': to_version_id}
        return ids
    except:
        warnings.warn('Url "' + url + '" is not in the required format and no data will be downloaded for this row', Warning)
=======
    from_version_id, to_version_id : string
        To and from version ids extracted from the url
    """
    if (url == ''):
        return None

    result = urlparse(url)
    if (result.netloc == 'versionista.com'):
        try:
            ids = [element for element in result.path.split('/') if element != '']
            site_id = ids[0]
            page_id = ids[1]
            version_ids = ids[2].split(':')
            to_version_id = version_ids[0]
            from_version_id = version_ids[1]
            return {'site_id': site_id, 'page_id': page_id,
                    'from_version_id': from_version_id, 'to_version_id': to_version_id}
        except:
            return None
    else:
        return None
>>>>>>> origin/machine_learning_work

def get_storage_uris(url):
    """
    Get uris of the versions stored in our db
<<<<<<< HEAD
    Uses get_version_by_versionista_id from web_monitoring.db
=======
    Uses get_version_uri from web_monitoring.db
>>>>>>> origin/machine_learning_work

    Parameters
    ----------
    url : string

    Returns
    -------
    from_version_uri, to_version_uri : string
        To and from version uris to access content in db
<<<<<<< HEAD
    ids : dictionary
        Site, page, and version ids of the from and two versions extracted from the url
    """
    ids = parse_url(url)
    from_version_id = ids['from_version_id']
    to_version_id = ids['to_version_id']
    print(ids)
    client = Client.from_env()
    """
    Some versionista uris have the format https://versionista.com/site_id/page_id/to_version_id:0/
    which automatically directs to the diff with the previous version.
    However, versions in our db are saved with their true version ids and when the from_version_id is '0',
    this is handled by taking advantage of the optional parameters of get_version_uri.
    """
    if from_version_id == '0':
        get_to_version = client.get_version_by_versionista_id(versionista_id=to_version_id)
        to_version_uri = get_to_version['data']['uri']
        valid_from_version_id = get_to_version['data']['source_metadata']['diff_with_previous_url'].split(':')[-1]
        from_version_uri = client.get_version_by_versionista_id(versionista_id=valid_from_version_id)['data']['uri']
        ids['from_version_id'] = valid_from_version_id
    else :
        to_version_uri = client.get_version_by_versionista_id(versionista_id=to_version_id)['data']['uri']
        from_version_uri = client.get_version_uri(versionista_id=from_version_id)['data']['uri']
    print(from_version_uri, to_version_uri)
    return from_version_uri, to_version_uri, ids

def download(filename, dirname):
=======
    """
    ids = parse_url(url)

    if (ids == None):
        return [None]*3
    else:
        from_version_id = ids['from_version_id']
        to_version_id = ids['to_version_id']
        if from_version_id == '0':
            to_version_uri, valid_from_version_id = get_version_uri(version_id=to_version_id,
                                                                    id_type='source',
                                                                    source_type='versionista',
                                                                    get_previous=True)
            from_version_uri = get_version_uri(version_id=valid_from_version_id,
                                               id_type='source',
                                               source_type='versionista')
            ids['from_version_id'] = valid_from_version_id
        else :
            to_version_uri = get_version_uri(version_id=to_version_id,
                                             id_type='source',
                                             source_type='versionista')
            from_version_uri = get_version_uri(version_id=from_version_id,
                                               id_type='source',
                                               source_type='versionista')
        return from_version_uri, to_version_uri, ids

def download(filename, dirname, path=''):
>>>>>>> origin/machine_learning_work
    """
    Downloader to read csv and download content for the dataset

    Parameters
    ----------
<<<<<<< HEAD
    filename : string
        The csv file which has the urls in the 'Last Two - Side by Side' column
    dirname : string
        The directory name in which you want to download the content
    """
    path = os.getcwd()
=======
    filename* : string
        The csv file which has the urls in the 'Last Two - Side by Side' column
    dirname* : string
        The directory name in which you want to download the content, creates a new directory
    path : string (default : current working directory)
        Path to the directory where the csv is stored and directory 'dirname' will be created
    """
    if (path == ''):
        path = os.getcwd()
>>>>>>> origin/machine_learning_work
    if (not os.path.exists(dirname)):
        os.mkdir(dirname)

    download_log_handler = open(str(dirname) + '.txt', 'w')
    filename = os.path.join(path, filename)
    df = pd.read_csv(filename, header=0, error_bad_lines=False)
    new_dir = os.path.join(path, dirname)
    os.chdir(new_dir)
    download_log_handler.write(time.asctime(time.localtime(time.time())) + '\n')
    urls = df['Last Two - Side by Side']

    for index in range(0, len(df['Last Two - Side by Side'])):
<<<<<<< HEAD
        try:
            from_version_uri, to_version_uri, ids = get_storage_uris(urls[index])
            from_version_response = requests.get(from_version_uri)
            to_version_response = requests.get(to_version_uri)
        except:
            download_log_handler.write('The data for url ' + str(urls[index]) + ' could not be downloaded.'+ '\n')
            continue

=======
        from_version_uri, to_version_uri, ids = get_storage_uris(urls[index])
        if (from_version_uri == None or to_version_uri == None):
            download_log_handler.write('Incorrect urls' + '\n')
            continue

        from_version_response = requests.get(from_version_uri)
        to_version_response = requests.get(to_version_uri)
>>>>>>> origin/machine_learning_work
        from_filename = str(ids['site_id'] + '_' + ids['page_id']
                            + '_' + ids['from_version_id'])
        to_filename = str(ids['site_id'] + '_' + ids['page_id']
                          + '_' + ids['to_version_id'])

        if (from_version_response.ok and to_version_response.ok):
            with open('change_' + str(index + 1) + '_' + from_filename
                      + '_from_version.html', 'w', encoding = 'utf-8') as f:
                f.write(from_version_response.text)
            with open('change_' + str(index + 1) + '_' + to_filename
                      + '_to_version.html', 'w', encoding = 'utf-8') as f:
                f.write(to_version_response.text)
            download_log_handler.write('Change ' + str(index + 1)
                                       + ' ' + ' '.join(from_filename.split('_'))
                                       + ' from version uri : '
                                       + from_version_uri + '\n')
            download_log_handler.write('Change ' + str(index + 1)
                                       + ' ' + ' '.join(to_filename.split('_'))
                                       + ' to version uri : '
                                       + to_version_uri + '\n')
        else:
            if (not from_version_response.ok):
                download_log_handler.write('Change ' + str(index + 1)
                                           + ' '.join(from_filename.split('_'))
                                           + ' from version issue :'
                                           + from_version_response.text)
            if (not to_version_response.ok):
                download_log_handler.write('Change ' + str(index + 1)
                                           + ' '.join(to_filename.split('_'))
                                           + ' to version issue :'
                                           + to_version_response.text)
            continue

    download_log_handler.close()
    os.chdir('..')
    return True

def main():
    doc = """Command Line Interface for the Machine Learning Data Downloader

Usage:
<<<<<<< HEAD
wm-data-downloader <filename> <dirname>
=======
data-downloader <filename> <dirname> [--path <path>]
>>>>>>> origin/machine_learning_work

Options:
-h --help     Show this screen.
--version     Show version.
"""
    arguments = docopt(doc, version='0.0.1')
    if arguments['run']:
        result = download(filename=arguments['<filename>'],
<<<<<<< HEAD
                          dirname=arguments['<dirname>'])
=======
                          dirname=arguments['<dirname>'],
                          path=arguments['<path>'])
>>>>>>> origin/machine_learning_work
