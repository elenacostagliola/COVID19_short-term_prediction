import sys, os
import time

from pandas import Timestamp
import numpy as np
import pandas as pd
import json
import argparse

from pymongo.errors import ConfigurationError

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from ml.train_test_splitting import TsCvSplitter
from ml import Sarimax
from ml import model_selection
from ml.performance_measures import rmse
from data.models import *
from data.dao import *


def main(configuration_path, parallel=None, debug=None):
    # Find and read configuration file
    if os.path.isfile(configuration_path):
        with open(configuration_path, "r") as f:
            configuration = json.load(f)
    else:
        raise FileNotFoundError("Cannot find specified configuration file")

    # Override parameters # TODO change to "if different"
    if parallel is not None:
        configuration["parallel"] = bool(parallel)
    if debug is not None:
        configuration["debug"] = bool(debug)

    log = {
        "timestamp": pd.Timestamp.utcnow()
    }

    # # Ingestion
    print("Connecting to DB")
    success = False
    n_retries = 0
    while n_retries <= 3:
        try:
            cddao = CuratedDataMongoDao()
            htrdao = HyperparameterTuningResultMongoDao()
            success = True
            break
        except ConfigurationError:
            n_retries += 1
            print(f"Connection attempt {n_retries} failed. Retrying in 3 s...")
            time.sleep(3)

    if not success:
        print("Connection to DB failed.")
        exit()

    print("Ingesting data")
    df = CuratedData.to_df(cddao.get_by_date()).set_index("date")

    # Crop data
    if configuration["date_from"] != "":
        print(f"Filtering date after {configuration['date_from']}")
        df = df.loc[df.index >= configuration["date_from"]]

    # Check data
    print("Date range:", df.index[0].date(), "-", df.index[-1].date())
    print("Shape (rows, columns):", df.shape)

    log["date_range"] = [df.index[0], df.index[-1]]

    # # Preprocessing

    # set daily frequency in datetime index (ensures there is one row per day)
    df = df.asfreq("D")

    # Handle NA in the last rows if data are missing
    # Warn if there are NA (except for risk)
    cols_with_na = df.isna().sum()
    for variable, na_count in cols_with_na.items():
        if na_count > 0:
            print(
                f"Warning: variable '{variable}' has {na_count} missing value{'s' if na_count > 1 else ''} --> forward fill")
    # Fill NA with last known value
    df = df.fillna(method="ffill")

    # Extracting outputs (endogenous variables) and regressors (exogenous variables)

    outputs = configuration["outputs"]
    regressors = configuration["regressors"]

    print("Endogenous variable:", outputs[0])
    if len(regressors) > 0:
        print("Exogenous variable(s):", regressors)

    y = df[outputs]
    if len(regressors) > 0:
        x = df[regressors]
        data = y.join(x)
    else:
        x = None
        data = y

    # Create splits for cross validation
    splitter = TsCvSplitter(n_splits=configuration["cv_n_splits"], max_test_size=configuration["cv_max_test_size"])
    splits = splitter.split(y)

    print(f"Cross validation --> training set size: min {splitter.min_train_size}, max {splitter.max_train_size},"
          f" test set size: {splitter.test_size}")

    log["splits_train_size_min"] = splitter.min_train_size
    log["splits_train_size_max"] = splitter.max_train_size
    log["splits_test_size"] = splitter.test_size

    # Set of hyperparameters
    p_values = configuration["p_values"]
    d_values = configuration["d_values"]
    q_values = configuration["q_values"]
    P_values = configuration["P_values"]
    D_values = configuration["D_values"]
    Q_values = configuration["Q_values"]
    m_values = configuration["m_values"]
    t_values = configuration["t_values"]

    # Prepare set of configurations
    configs = model_selection.get_sarima_configurations(p_values, d_values, q_values,
                                                        P_values, D_values, Q_values,
                                                        m_values, t_values)

    # TODO debug
    # configs = configs[8:18]

    # # SARIMAX (with exog)
    # ## Grid search (with exog)

    performance_measure = configuration["performance_measure"]
    if performance_measure.lower() == "rmse":
        performance_measure = rmse

    results = model_selection.grid_search(data=data, model=Sarimax, configurations=configs,
                                          splits=splits, debug=configuration["debug"],
                                          parallel=configuration["parallel"],
                                          performance_measure=performance_measure, transformation="sqrt")

    best_config = results[0]

    print("Best configuration:", best_config["cfg"])
    print("Average score:", best_config["score_mean"], "| standard error:", best_config["score_se"])

    log["elapsed_time"] = str(pd.Timestamp.utcnow() - log["timestamp"])

    print("Saving configuration")
    htr = HyperparameterTuningResult(log, configuration, results)
    htrdao.save(htr)

    print("Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tune model hyperparameters.')
    parser.add_argument('configuration_path', type=str,
                        help='path to configuration file (JSON)')
    parser.add_argument('--parallel', type=int,
                        help='parallel computation (1=true, 0=false), '
                             'default true, overrides value in configuration file')
    parser.add_argument('--debug', type=int,
                        help='enable warnings and errors (1=true, 0=false), default false, '
                             'overrides value in configuration file')
    args = parser.parse_args()

    main(args.configuration_path, parallel=args.parallel, debug=args.debug)
