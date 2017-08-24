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
    """
    Downloader to read csv and download content for the dataset

    Parameters
    ----------
    filename* : string
        The csv file which has the urls in the 'Last Two - Side by Side' column
    dirname* : string
        The directory name in which you want to download the content, creates a new directory
    path : string (default : current working directory)
        Path to the directory where the csv is stored and directory 'dirname' will be created
    """
    if (path == ''):
        path = os.getcwd()
    if (not os.path.exists(dirname)):
        os.mkdir(dirname)

    download_log_handler = open(str(dirname) + '.txt', 'w')
    filename = os.path.join(os.getcwd(), filename)
    df = pd.read_csv(filename, header=0, error_bad_lines=False)
    new_dir = os.path.join(path, dirname)
    os.chdir(new_dir)
    download_log_handler.write(time.asctime(time.localtime(time.time())) + '\n')
    urls = df['Last Two - Side by Side']

    for index in range(0, len(df['Last Two - Side by Side'])):
        from_version_uri, to_version_uri, ids = get_storage_uris(urls[index])
        if (from_version_uri == None or to_version_uri == None):
            download_log_handler.write('Incorrect urls' + '\n')
            continue

        from_version_response = requests.get(from_version_uri)
        to_version_response = requests.get(to_version_uri)
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
