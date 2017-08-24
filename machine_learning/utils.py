import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sklearn.externals import joblib

def clean_text(raw_text):
    """
    This function cleans text data by performing
    various operations on it.
    1. Remove all characters except letters (a-z)
    2. Converts all text to lowercase
    3. Removes stopwords (articles, pronouns etc.)
    For more info about stopwords - https://en.wikipedia.org/wiki/Stop_words

    Parameters
    ----------
    raw_text : string

    Returns
    -------
    meaningful_text : string
    """

    #Remove special characters
    temp_text = re.sub('[^a-zA-Z]', ' ', raw_text)

    #Convert all words to lower case and split into words
    temp_text = temp_text.lower().split()

    #Get a list of stopwords
    stops = stopwords.words('english')

    #Remove the stopwords
    meaningful_text = [word for word in temp_text if not word in stops]

    #Join the words together to create a space separated string
    return ' '.join(meaningful_text)


def preprocess(vectorizer_func, text, labels, max_features, max_df=1.0, min_df=1):
    """
    This function converts raw text to numerical features.
    Classification models take in numerical features as input.
    This function also splits data into training & testing sets.

    Parameters
    ----------
    vectorizer_func : string; either 'count' or 'tfidf'
    text : input text
        sequence of indexables : lists, numpy arrays, scipy-sparse matrices
                                 or pandas dataframes
    labels : labels associated with text
        similar sequence of indexables
    max_features : int
        top n ordered by term frequency across the corpus
    max_df : float in range [0.0,1.0]
        To ignore words that have a document frequency than given threshold
        Parameter represents proportion of documents
    min_df : int
        To ignore words that have a document frequency strictly lower than threshold
        Parameter represents absolute counts of words

    Returns
    -------
    4 arrays, 2 feature arrays(train & test) and 2 label arrays(train & test)
    To know more, check out these resources:
    http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html
    http://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html
    http://scikit-learn.org/stable/tutorial/text_analytics/working_with_text_data.html
    """
    if (vectorizer_func == 'count'):
        vectorizer = CountVectorizer(analyzer='word',
                                     stop_words='english',
                                     max_features=max_features,
                                     max_df=max_df,
                                     min_df=min_df)
    elif (vectorizer_func == 'tfidf'):
        vectorizer = TfidfVectorizer(analyzer='word',
                                     stop_words='english',
                                     max_features=max_features,
                                     max_df=max_df,
                                     min_df=min_df,
                                     sublinear_tf=True)
    else:
        raise ValueError('Please select a valid vectorizer.')
    #Splitting data into training and testing sets
    train_text, test_text, train_labels, test_labels = train_test_split(text, labels, test_size=0.1)

    #Transforming text to numerical features
    train_features = vectorizer.fit_transform(train_text)
    test_features = vectorizer.transform(test_text)

    return train_features, test_features, train_labels, test_labels

def evaluate(eval_method, y_test, y_predicted):
    """
    This function computes evaluation metrics.

    Parameters
    ----------
    eval_method : string
        'accuracy' or 'report', more to be added soon
    y_test : list
        List of true labels
    y_predicted : list
        List of predicted labels

    Returns
    -------
    Eval method = 'accuracy' : float between 0.0 and 1.0
    Eval method = 'report' : string with detailed classification report

    For more info:
    http://scikit-learn.org/stable/modules/generated/sklearn.metrics.accuracy_score.html
    http://scikit-learn.org/stable/modules/generated/sklearn.metrics.classification_report.html
    """
    if (eval_method == 'accuracy'):
        return accuracy_score(y_true=y_test, y_pred=y_predicted)
    elif (eval_method == 'report'):
        return classification_report(y_true=y_test, y_pred=y_predicted)
    else :
        raise ValueError('Please select a valid evalaution method.')

def save_model(clf, filename):
    """
    Save a trained classifier model for later use

    Parameters
    ----------
    clf : object of the classifier class used
    filename : string
        Name of the file in which the model will be stored
    """
    dump_filenames = joblib.dump(value=clf, filename=filename)
    return dump_filenames
