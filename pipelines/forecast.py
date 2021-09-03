import json
import sys, os
import argparse
import time

from pymongo.errors import ConfigurationError

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from ml import Sarimax
from data.models import *
from data.dao import *


def main(configuration_path, steps=7):
    # Find and read configuration file
    if os.path.isfile(configuration_path):
        print(f"Reading configuration file at {configuration_path}")
        with open(configuration_path, "r") as f:
            models = json.load(f)
    else:
        raise FileNotFoundError("Cannot find specified configuration file")

    # # Ingestion
    print("Connecting to DB")
    success = False
    n_retries = 0
    while n_retries <= 3:
        try:
            cddao = CuratedDataMongoDao()
            fdao = ForecastMongoDao()
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

    # Check data
    print("Date range:", df.index[0].date(), "-", df.index[-1].date())
    print("Shape (rows, columns):", df.shape)

    # # Preprocessing

    # set daily frequency in datetime index (ensures there is one row per day)
    df = df.asfreq("D")

    # Handle NA in the last rows if data are missing
    # Warn if there are NA (except for risk)
    cols_with_na = df.isna().sum()
    for variable, na_count in cols_with_na.items():
        if na_count > 0:
            print(
                f"Warning: variable '{variable}' has {na_count} missing "
                f"value{'s' if na_count > 1 else ''} --> forward fill")
    # Fill NA with last known value
    df = df.fillna(method="ffill")

    for m in models:
        # Get best configuration for variable
        print("Getting best configuration for variable", m["output"])
        if len(m["regressors"]) > 0:
            print("with regressors", m["regressors"])
        htr = htrdao.get_most_recent_record(m["output"], m["regressors"])
        if len(htr) == 0:
            print("Could not find the configuration for the current model.",
                  "Make sure hyperparameter tuning has been run and the corresponding model configuration",
                  "has been successfully inserted into the DB. Exiting.")
            exit()
        else:
            htr = htr[0]

        # Extracting outputs (endogenous variables) and regressors (exogenous variables)
        outputs = htr.configuration["outputs"]
        regressors = htr.configuration["regressors"]

        y = df[outputs]
        if len(regressors) > 0:
            x = df[regressors]
            data = y.join(x)
        else:
            x = None
            data = y

        # Extract best configuration from htr result
        model_best_config = htr.results[0]["cfg"]

        print("Fitting model for variable", m["output"])
        model = Sarimax(data, config=model_best_config, convergence_warnings=False, transformation="sqrt")
        model.fit()
        print(model)

        exog_scenario = None
        if x is not None:
            # Scenario: constant future values for exog
            dtidx = pd.date_range(start=y.index[-1] + pd.Timedelta(days=1), end=y.index[-1] + pd.Timedelta(days=7))
            scenario = pd.DataFrame(index=dtidx)
            exog_scenario = scenario.join(x, how="outer").fillna(method="ffill").reindex(dtidx)

        print("Forecasting variable", m["output"])
        fcast = model.forecast(steps=steps, exog=exog_scenario if x is not None else None)
        fcast["output_variable"] = m["output"]
        fcast.index.name = "date"

        print("Saving results for variable", m["output"])
        data = Forecast.from_df(fcast.reset_index(drop=False))
        fdao.save(data)

    print("Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Produce forecasts according to the best hyperparameters for a specified model.')
    parser.add_argument('configuration_path', type=str,
                        help='path to configuration file (JSON)')
    parser.add_argument('--steps', type=int, default=7,
                        help='how many steps in the future')
    args = parser.parse_args()

    main(args.configuration_path, steps=args.steps)
