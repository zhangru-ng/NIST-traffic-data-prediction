import os

import numpy as np
import pandas as pd
from sklearn import linear_model
from collections import OrderedDict
from sklearn import neighbors
from sklearn.svm import SVR

__author__ = 'mebin'

training_data_loc = "/home/mebin/PycharmProjects/NIST/training_data.csv"
box_training_data_loc = "/home/mebin/PycharmProjects/NIST/box/"
# Note its tab separated
testing_data_loc = "/home/mebin/PycharmProjects/NIST/prediction_trials.tsv"
output_dict = OrderedDict()
missing_box = open("missing_box", "w")

def regressor(reg, feature_mat, targets, no_of_training_example):
    try:
        reg.fit(feature_mat, targets)
    except Exception, e:
        print e
        # raise Exception


def call_linear_regression(feature_vector, targets, counter):
    percentage = 0.75
    if counter == -1:
        counter = len(targets)
        percentage = 1
    no_of_training_example = percentage * counter
    linear_regressor = linear_model.LinearRegression(normalize=True)
    regressor(linear_regressor, feature_vector, targets, no_of_training_example)
    return linear_regressor


def call_nearest_neighbour_regressor(feature_vector, targets, counter):
    percentage = 0.75
    if counter == -1:
        counter = len(targets)
        percentage = 1
    no_of_training_example = percentage * counter
    n_neighbors = 3
    knn = neighbors.KNeighborsRegressor(n_neighbors=n_neighbors, algorithm='brute', metric='euclidean')
    regressor(knn, feature_vector, targets, no_of_training_example)
    return knn

def call_SVMR(feature_vector, targets, counter):
    percentage = 0.75
    if counter == -1:
        counter = len(targets)
        percentage = 1
    no_of_training_example = percentage * counter
    svm = SVR(C=1.0, epsilon=0.2)
    regressor(svm, feature_vector, targets, no_of_training_example)
    return svm


def training(training_attr):
    """
     Parameters
     ----------
     training_attr : Attribute for which training should be done using the date as feature..

    :return: Nothing
    """
    col_names = ['lat1', 'long1', 'lat2', 'long2', 'date', 'accidents', 'roadwork', 'precipitation', 'device_status',
                 'obstruction', 'traffic_condition']
    acc_col_names = ['lat1', 'long1', 'lat2', 'long2', 'date', training_attr]
    grp_by_cols = ['lat1', 'long1', 'lat2', 'long2']  # , 'date'
    events = pd.read_csv(training_data_loc, header=None, names=col_names)
    events = events[acc_col_names]
    for val in events.groupby(grp_by_cols):
        f = open(
            box_training_data_loc + training_attr + "/" + str(val[0]).replace(" ", "_").replace(",", "").replace("(",
                                                                                                                 "").replace(
                ")", ""),
            'w')
        date_list = val[1]['date'].tolist()
        accident_list = val[1][training_attr].tolist()
        for d, a in zip(date_list, accident_list):
            f.write(str(d) + "," + str(a) + "\n")
        f.close()


def prediction(prediction_attr, deg):
    """
     Parameters
     ----------
     prediction_attr : Attribute for which prediction should be done using the date as feature..

    :return: Nothing
    """
    pred_col_names = ['lat1', 'long1', 'lat2', 'long2', 'date', 'date2']
    pred_events = pd.read_csv(testing_data_loc, header=None, names=pred_col_names, sep="\t")
    reqd_cols = ['lat1', 'long1', 'lat2', 'long2', 'date']
    pred_events = pred_events[reqd_cols]
    pred_events['date'] = pred_events.apply(lambda x: x['date'].split('-')[0] + '-' + x['date'].split('-')[1], axis=1)
    pred_events = pred_events[reqd_cols]
    box_model_dict = dict()
    knn_box_model_dict = dict()
    learning_cols = ['date', prediction_attr]
    pred_events['year'] = pred_events['date'].apply(lambda x: x.split('-')[0])
    pred_events['month'] = pred_events['date'].apply(lambda x: x.split('-')[1])
    i = 0
    for row in pred_events.itertuples(index=False):  # [['lat1', 'long1', 'lat2', 'long2']]
        print row
        break
        i += 1
        file_name = str(row[:4]).replace(" ", "_").replace(",", "").replace("(", "").replace(")", "")
        if os.path.isfile(box_training_data_loc + prediction_attr + "/" + file_name):
            reg1 = None
            if file_name not in box_model_dict:
                learning_file_df = pd.read_csv(box_training_data_loc + prediction_attr + "/" + file_name, header=None,
                                               names=learning_cols)

                learning_file_df['year'] = learning_file_df['date'].apply(lambda x: x.split('-')[0])
                learning_file_df['month'] = learning_file_df['date'].apply(lambda x: x.split('-')[1])
                if prediction_attr == 'accidents':
                    reg1 = call_linear_regression(learning_file_df[['year', 'month']].as_matrix(),
                                              learning_file_df[prediction_attr].as_matrix(),
                                              -1)
                elif prediction_attr == 'traffic_condition' or prediction_attr == 'device_status' or prediction_attr == 'obstruction':
                    reg1 = call_SVMR(learning_file_df[['year', 'month']].as_matrix(),
                                                             learning_file_df[prediction_attr].as_matrix(), -1)

                else: # and precipitation prediction_attr == 'roadwork' or
                    reg1 = call_nearest_neighbour_regressor(learning_file_df[['year', 'month']].as_matrix(),
                                              learning_file_df[prediction_attr].as_matrix(), -1)
                box_model_dict[file_name] = reg1
            else:
                reg1 = box_model_dict[file_name]
            try:
                predicted_val = reg1.predict(np.array(row[5:], dtype=np.float32))

            except ValueError as e:
                print e
                predicted_val = np.float32(np.array([0.0], dtype=np.float32))
                print "Value missed"
                print file_name
            key = str(row)  # date plus bounding box
            if key not in output_dict:
                output_dict[key] = predicted_val.tolist()
            else:
                output_dict[key].extend(predicted_val)
        else:
            predicted_val = np.float32(np.array([0.0], dtype=np.float32))
            output_dict[file_name] = predicted_val
            print i
            missing_box.write(file_name + "\n")


def accident_prediction():
    prediction('accidents', 4)


def precipitation_prediction():
    prediction('precipitation', 2)


def roadwork_prediction():
    prediction('roadwork', 2)


def device_status_prediction():
    prediction('device_status', 2)


def obstruction_prediction():
    prediction("obstruction", 2)


def traffic_condition_prediction():
    prediction("traffic_condition", 2)

print 'Accident Prediction : -'
# linear
training('accidents')
accident_prediction()

# print "Precipitation Prediction"
# # knn
# training('precipitation')
# precipitation_prediction()
#
# # KNN
# print "Roadwork prediction"
# training('roadwork')
# roadwork_prediction()
#
# print 'device_status'
# training('device_status')
# device_status_prediction()
#
# print 'obstruction'
# training('obstruction')
# obstruction_prediction()
#
# print "Traffic condition prediction "
# training('traffic_condition')
# traffic_condition_prediction()

final_output_file = open("predicted_values.tsv", "w")
for val in output_dict.itervalues():
    final_output_file.write("\t".join(str(x) for x in val) + "\n")
final_output_file.close()
missing_box.close()