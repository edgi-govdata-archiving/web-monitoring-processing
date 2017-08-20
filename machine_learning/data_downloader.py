import argparse
import requests
from urllib.parse import urlparse
from web_monitoring.db import get_version_uri
import pandas as pd
import os
import time

def parse_url(url):
    """
    Parse Versionista change urls to get version ids
    https://versionista.com/{site id}/{page id}/{to version id}:{from version id}

    Parameters
    ----------
    url : string

    Returns
    -------
    from_version_id, to_version_id : string
        To and from version ids extracted from the url
    """
    result = urlparse(url)
    assert (result.netloc == 'versionista.com')
    ids = [element for element in result.path.split('/') if element != '']
    version_ids = ids[-1].split(':')
    to_version_id = version_ids[0]
    from_version_id = version_ids[1]
    return from_version_id, to_version_id

def get_storage_uris(url):
    """
    Get uris of the versions stored in our db
    Uses get_version_uri from web_monitoring.db

    Parameters
    ----------
    url : string

    Returns
    -------
    from_version_uri, to_version_uri : string
        To and from version uris to access content in db
    """
    from_version_id, to_version_id = parse_url(url)

    if from_version_id == '0':
        to_version_uri, valid_from_version_id = get_version_uri(version_id=to_version_id,
                                                                id_type='source',
                                                                source_type='versionista',
                                                                get_previous=True)
        from_version_uri = get_version_uri(version_id=valid_from_version_id,
                                           id_type='source',
                                           source_type='versionista')
    else :
        to_version_uri = get_version_uri(version_id=to_version_id,
                                         id_type='source',
                                         source_type='versionista')
        from_version_uri = get_version_uri(version_id=from_version_id,
                                           id_type='source',
                                           source_type='versionista')
    return from_version_uri, to_version_uri

def download(filename, dirname, path=''):
    """
    Downloader to read csv and download content for the dataset

    Parameters
    ----------
    filename* : string
        The csv file which has the urls in the 'Last Two - Side by Side' column
    dirname* : string
        The directory name in which you want to download the content, creates a new directory
    path : string (default : current working directory)
        Path to the directory where the csv is stored and downloaded content will be stored
    """
    if (path == ''):
        path = os.getcwd()
    os.mkdir(dirname)
    new_dir = os.path.join(path, dirname)
    download_log_handler = open(str(dirname)+ '.txt', 'w')
    filename = os.path.join(os.getcwd(), filename)
    os.chdir(new_dir)
    download_log_handler.write(time.asctime(time.localtime(time.time())) + '\n')
    df = pd.read_csv(filename, header=0, error_bad_lines=False)
    urls = df['Last Two - Side by Side']

    for index in range(len(df['Last Two - Side by Side'])):
        from_version_uri, to_version_uri = get_storage_uris(urls[index])

        from_version_response = requests.get(from_version_uri)
        to_version_response = requests.get(to_version_uri)

        if (from_version_response.ok and to_version_response.ok):
            with open('change_' + str(index + 1) + '_from_version.html',
                      'w', encoding = 'utf-8') as f:
                f.write(from_version_response.text)
            with open('change_' + str(index + 1) + '_to_version.html',
                      'w', encoding = 'utf-8') as f:
                f.write(to_version_response.text)
            download_log_handler.write('Change ' + str(index + 1)
                                       + ' from version uri : '
                                       + from_version_uri + '\n')
            download_log_handler.write('Change ' + str(index + 1)
                                       + ' to version uri : '
                                       + to_version_uri + '\n')
        else:
            if (not from_version_response.ok):
                download_log_handler.write('Change ' + str(index + 1)
                                           + ' from version issue :'
                                           + from_version_response.text)
            if (not to_version_response.ok):
                download_log_handler.write('Change ' + str(index + 1)
                                           + ' to version issue :'
            continue

    download_log_handler.close()
    os.chdir('..')
    return True

def main():

    parser = argparse.ArgumentParser(description='Run downloader script')
    parser.add_argument('filename', type=str, help='CSV file which has the urls')
    parser.add_argument('dirname', type=str, help='Directory name in which you want to download the content')
    parser.add_argument('--path', type=str, help='Path where you want to create the new directory', default='')
    arguments = parser.parse_args()
    result = download(filename=arguments.filename,
                      dirname=arguments.dirname,
                      path=arguments.path)

    if result:
        print('Download successful')

if __name__ == '__main__':
    main()
