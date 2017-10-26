import os
import argparse
import web_monitoring.differs as wd
import pandas as pd
from docopt import docopt

def text_diff(a_text, b_text):
    """
    Gets text changes between two versions.
    Returns all changes of a certain category joined into a single string

    Parameters
    ----------
    a_text, b_text : string

    Returns
    -------
    deleted : All deletions joined to form a single string
    added : All additions joined to form a single string
    combined : All changes joined to form a single string
    len(diffs) : Total no. of changes computed by html_text_diff
    """
    diffs = wd.html_text_diff(a_text=a_text, b_text=b_text)
    deleted = ' '.join([diff[1] for diff in diffs if diff[0] == -1])
    added = ' '.join([diff[1] for diff in diffs if diff[0] == 1])
    combined = ' '.join([diff[1] for diff in diffs])
    return [deleted, added, combined, str(len(diffs))]

def create_dataset(dirname):
    """
    Creates dataset for classification model

    Parameters
    ----------
    dirname : Name of the directory where all the versions(to be diffed)
              are stored

    Returns
    -------
    df : Pandas DataFrame with information about changes between
                versions as rows
    """
    path = os.path.join(os.getcwd(), dirname)
    filelist = os.listdir(path)
    os.chdir(path)
    columns = ['deleted', 'added', 'combined', 'no. of changes',
               'site id', 'page id', 'from version id', 'to version id']
    df = pd.DataFrame(columns=columns)
    for index in range(0, len(filelist), 2):
        assert filelist[index].split('_')[1] == filelist[index + 1].split('_')[1]
        site_id = filelist[index].split('_')[2]
        page_id = filelist[index].split('_')[3]
        from_version_id = filelist[index].split('_')[4]
        to_version_id = filelist[index + 1].split('_')[4]
        with open(filelist[index], 'r', encoding='utf-8') as f1:
            text1 = f1.read()
        with open(filelist[index + 1], 'r', encoding='utf-8') as f2:
            text2 = f2.read()
        data =  text_diff(text1, text2)
        data.extend([site_id, page_id,
                     from_version_id, to_version_id])
        df = df.append(pd.DataFrame([data], columns=columns),
                       ignore_index=True)

    os.chdir('..')
    return df, True

def main():
    doc = """Command Line Interface for creating dataset from downloaded data

Usage:
dataset-creator <dirname> <pkl_filename>

Options:
-h --help     Show this screen.
--version     Show version.
"""
    arguments = docopt(doc, version='0.0.1')
    if arguments['run']:
        result, status = create_dataset(dirname=arguments['<dirname>'])
    if status:
        result.to_pickle(arguments['<pkl_filename>'])
