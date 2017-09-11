from machine_learning import utils
from sklearn import svm

corpus = [['Climate change is an issue', 'important'],
          ['Data and climate change', 'important'],
          ['June 05 06', 'insignificant'],
          ['Twitter feed update', 'insignificant'],
          ['Environmental governance', 'important'],
          ['Environmental change', 'important'],
          ['Aug 25 2017', 'insignificant'],
          ['Durham North Carolina', 'insignificant'],
          ['Energy data', 'insignificant'],
          ['New York', 'insignificant'],
          ['Climate Action Plan removal', 'important'],
          ['US-China Energy collaboration', 'important'],
          ['Scrolling news', 'insignificant'],
          ['Banner ad', 'insignificant'],
          ['View this dataset', 'important'],
          ['Geothermal energy news', 'important'],
          ['2016 2017', 'insignificant'],
          ['Annual report', 'important'],
          ['New panel selected', 'important'],
          ['Jul Jun', 'important']]

text = []
labels = []

for tup in corpus:
    tup[0] = utils.clean_text(tup[0])
    text.append(tup[0])
    labels.append(tup[1])

train_features, test_features, train_labels, test_labels = utils.preprocess(vectorizer_func='count',
                                                                            text=text,
                                                                            labels=labels,

clf = svm.SVC(kernel='linear')
clf.fit(train_features, train_labels)

predicted_labels = clf.predict(test_features)
print(predicted_labels)
print(utils.evaluate(eval_method='report', y_test=test_labels,
                     y_predicted=predicted_labels))

