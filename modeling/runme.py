import json
import os

import pandas as pd
from comet_ml import Experiment

from modeling.models.azure_anomaly_detection.azure_ad import AzureAnomalyTrendinessDetector
from modeling.models.data_reader import read_labeled_data
from modeling.models.eval import eval_models, get_evaluation_metrics, get_final_metrics
from modeling.models.stl.stl_model import StlTrendinessDetector
from modeling.models.twitter.anomaly_detect_multicategory import TwitterAnomalyTrendinessDetector


def run_model(datapath, freq, min_date='01-01-2018', plot=True, model_name = 'stl', min_value = 10):
    print("Loading file {0}, with frequency {1}. Model name = {2}".format(datapath, freq, model_name))
    dataset = read_labeled_data(datapath)
    dataset['date'] = pd.DatetimeIndex(dataset['date'])
    dataset.set_index('date', inplace=True)
    dataset = dataset.rename(columns={'is_anomaly': 'label'})
    dataset = dataset[dataset.index > min_date]
    dataset = dataset.reset_index().set_index(['date', 'category'])

    categories = dataset.index.levels[1]
    print("categories found = {}".format(categories))

    if model_name == 'twitter':
        model = TwitterAnomalyTrendinessDetector(is_multicategory=True, freq=freq, min_value=min_value , threshold=None, max_anoms=0.49, seasonality_freq=7)

    if model_name == 'stl':
        model = StlTrendinessDetector(is_multicategory=True, freq=freq, min_value=min_value , anomaly_type='or', num_of_std=3)


    if model_name == 'azure':
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'config/config.json')
        subscription_key = get_azure_subscription_key(filename)
        model = AzureAnomalyTrendinessDetector(is_multicategory=True, freq=freq, min_value=min_value,subscription_key = subscription_key)

    prediction = model.predict(dataset, verbose=True)
    raw_metrics = get_evaluation_metrics(dataset[['value']], prediction[['prediction']], dataset[['label']],
                                         window_size_for_metrics=5)
    metrics = get_final_metrics(raw_metrics)
    print(metrics)

    ## Plot each category
    _, file = os.path.split(datapath)
    model.plot(labels=dataset['label'], postfix=file)

def get_azure_subscription_key(file):

    try:
        with open(file) as f:
            data = json.load(f)
            return data['subscription_key']
    except Exception as e:
        raise Exception("Error loading Azure subscription key for Azure Anomaly Finder.\n"
              "Please create a json file and put your subscription_key value in it.\n"
              "See https://docs.microsoft.com/en-us/azure/machine-learning/team-data-science-process/apps-anomaly-detection-api\n" + str(e))




def eval_model(datapath="../SF3H_labeled.csv", min_date='01-01-2018', freq='3H', use_comet=True):
    dataset = read_labeled_data(datapath)
    dataset['date'] = pd.DatetimeIndex(dataset['date'])
    dataset.set_index('date', inplace=True)
    dataset = dataset[dataset.index > min_date]
    dataset = dataset.reset_index().set_index(['date', 'category'])

    dataset = dataset[~dataset.index.duplicated(keep='first')]

    X = dataset[['value']]
    y = dataset[['is_anomaly']]

    ## First model: STL

    anomaly_types = ['residual', 'trend', 'and', 'or']
    # anomaly_types = ['residual', ['trend', 'residual']]
    anomaly_type = 'residual'
    num_std = 3
    window_size_for_metrics = 3
    min_value = 15
    for num_std in [2, 2.5, 3, 3.5, 4]:
        for anomaly_type in anomaly_types:
            model = StlTrendinessDetector(is_multicategory=True, freq=freq, min_value=min_value,
                                          anomaly_type=anomaly_type, num_of_std=num_std)
            result = eval_models(X, y, [model], label_col_name='is_anomaly', train_percent=50,
                                 window_size_for_metrics=window_size_for_metrics)
            print('num_std = ' + str(num_std) + ', anomaly_type = ' + str(anomaly_type) + ', min_value = ' + str(
                min_value) + ', dataset = ' + datapath)
            print('F1 score = ' + str(result[model.__name__]['f1']) + ", precision = " + str(result[model.__name__][
                                                                                                 'precision']) + ", recall = " + str(
                result[model.__name__]['recall']))
            # model.plot(labels = y.reset_index().set_index('date'))

            if use_comet:
                experiment = Experiment(api_key="Uv0lx3yRDH7kk8h1vtR9ZRiD2s16gnYTxfsvK2VnpV2xRrMbFobYDZRRA4tvoYiR",
                                        project_name="trending-topics")
                experiment.log_dataset_hash(dataset)
                experiment.log_parameter("model", model.__name__)
                experiment.log_parameter("dataset", datapath)
                experiment.log_parameter("num_of_std", num_std)
                experiment.log_parameter("anomaly_type", anomaly_type)
                experiment.log_parameter("window_size_for_metrics", window_size_for_metrics)
                experiment.log_metric("f1", result[str(model.__name__)]['f1'])
                experiment.log_metric("f0.5", result[str(model.__name__)]['f0.5'])
                experiment.log_metric("precision", result[str(model.__name__)]['precision'])
                experiment.log_metric("recall", result[str(model.__name__)]['recall'])

    max_anoms_list = [0.05, 0.1]
    for max_anoms in max_anoms_list:
        for threshold in [None, 'med_max', 'p95', 'p99']:
            for alpha in [0.05, 0.1, 0.15]:
                model = TwitterAnomalyTrendinessDetector(is_multicategory=True, freq=freq, min_value=min_value,
                                                         threshold=threshold, max_anoms=max_anoms,
                                                         longterm=False, alpha=alpha, seasonality_freq=7)

                result = eval_models(X, y, [model], label_col_name='is_anomaly', train_percent=50,
                                     window_size_for_metrics=window_size_for_metrics)
                if threshold is None:
                    print('Threshold = None, Alpha = ' + str(alpha) + ', max_anoms = None, min_value = ' + str(
                        min_value) + ', dataset = ' + datapath)
                else:
                    print('Threshold = ' + threshold + ', Alpha = ' + str(
                        alpha) + ', max_anoms = None, min_value = ' + str(min_value) + ', dataset = ' + datapath)
                print('F1 score = ' + str(result[model.__name__]['f1']) + ", precision = " + str(
                    result[model.__name__]['precision']) + ", recall = " + str(
                    result[model.__name__]['recall']))

                if use_comet:
                    experiment = Experiment(api_key="Uv0lx3yRDH7kk8h1vtR9ZRiD2s16gnYTxfsvK2VnpV2xRrMbFobYDZRRA4tvoYiR",
                                            project_name="trending-topics")
                    experiment.log_dataset_hash(dataset)
                    experiment.log_parameter("model", model.__name__)
                    experiment.log_parameter("max_anoms", 0.49)
                    experiment.log_parameter("threshold", threshold)
                    experiment.log_parameter("alpha", alpha)
                    experiment.log_parameter("longterm", True)
                    experiment.log_parameter("dataset", datapath)
                    experiment.log_parameter("window_size_for_metrics", window_size_for_metrics)
                    experiment.log_metric("f1", result[str(model.__name__)]['f1'])
                    experiment.log_metric("f0.5", result[str(model.__name__)]['f0.5'])
                    experiment.log_metric("precision", result[str(model.__name__)]['precision'])
                    experiment.log_metric("recall", result[str(model.__name__)]['recall'])


if __name__ == '__main__':

    inp1 = ''
    while inp1 not in ['r','e']:
        inp1 = input("Run (r) or evaluate (e)?")

    city = 0
    while city not in ['1','2','3','9']:
        city = input("Select city: Corona (1), Pompano (2), SF (3), all (9):")

    paths = {'1': "../datasets/corona_labeled.csv",
             '2': "../datasets/pompano_labeled.csv",
             '3': "../datasets/SF30min_labeled.csv"}

    freqs = {'1': '12H', '2': '24H', '3': '30min'}

    models = {'s':'stl','t':'twitter','a':'azure'}

    if inp1 == 'r':
        model = input("Select model: STL (s), Twitter (t), Azure Anomaly Detector (a):")
        run_model(datapath=paths[city], freq=freqs[city], model_name=models[model])
    if inp1 == 'e':
        if city == '9':
            print("Evaluating all cities")
            # eval_model(datapath="../test/dummy3.txt", freq='12H',use_comet=False)
            for val,freq in zip(paths,freqs):
                eval_model(datapath=paths[val], freq=freqs[freq], use_comet=True)
            #eval_model(datapath="../datasets/corona_labeled.csv", freq='12H')
            #eval_model(datapath="../datasets/SF30min_labeled.csv", freq='30min')
        else:
            print("Loading file {0}, with frequency {1}. Evaluating all models".format(paths[city], freqs[city]))
            eval_model(datapath=paths[city], freq=freqs[city])
