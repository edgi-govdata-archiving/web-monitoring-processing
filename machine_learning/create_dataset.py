import os
import argparse
import web_monitoring.differs as wd
import pandas as pd

def text_diff(a_text, b_text):
    """
    Gets text changes between two versions.
    Returns all changes of a certain category joined into a single string

    Parameters
    ----------
    a_text, b_text* : string

    Returns
    -------
    deleted : All deletions joined to form a single string
    added : All additions joined to form a single string
    combined : All changes joined to form a single string
    len(diffs) : Total no. of changes computed by html_text_diff
    """
    diffs = wd.html_text_diff(a_text=a_text, b_text=b_text)
    state = []
    deleted = ' '.join([diff[1] for diff in diffs if diff[0] == -1])
    added = ' '.join([diff[1] for diff in diffs if diff[0] == 1])
    combined = ' '.join([diff[1] for diff in diffs])
    return deleted, added, combined, len(diffs)

def create_dataset(dirname):
    """
    Creates dataset for classification model

    Parameters
    ----------
    dirname* : Name of the directory where all the versions(to be diffed)
              are stored

    Returns
    -------
    df : Pandas DataFrame with information about changes between
                versions as rows
    """
    path = os.path.join(os.getcwd(), dirname)
    filelist = os.listdir(path)
    os.chdir(path)
    columns = ['deleted', 'added', 'combined', 'no. of changes']
    df = pd.DataFrame(columns=columns)
    for index in range(0,len(filelist),2):
        assert filelist[index][:filelist[index].find('_')+2] == filelist[index+1][:filelist[index+1].find('_')+2]
        with open(filelist[index], 'r', encoding='utf-8') as f1:
            text1 = f1.read()
        with open(filelist[index+1], 'r', encoding='utf-8') as f2:
            text2 = f2.read()
        df = df.append(pd.DataFrame([text_diff(text1, text2)], columns=columns), ignore_index=True)

    os.chdir('..')
    return df, True

def main():

    parser = argparse.ArgumentParser(description='Run dataset creation script')
    parser.add_argument('dirname', type=str, help='Directory name in which the versions are stored')
    parser.add_argument('pkl_filename', type=str, help='Name of the pickle file in which the dataset will be stored')
    arguments = parser.parse_args()
    result, status = create_dataset(dirname=arguments.dirname)

    if status:
        print('Download successful')
        result.to_pickle(arguments.pkl_filename)
        print('Dataset created and stored successfully')

if __name__ == '__main__':
    main()
