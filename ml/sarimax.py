from warnings import catch_warnings, filterwarnings
import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX

from .cross_validation import model_cross_validation
from .performance_measures import rmse
from .viz import plot_model


class SarimaxException(Exception):

    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return repr(self.message)


class Sarimax:
    def __init__(self, data=None, endog_column=None, endog=None, exog=None, transformation=None,
                 config=None,
                 relax_constraints=True,
                 convergence_warnings=True):

        if transformation is not None:
            assert transformation in ["sqrt"], "Invalid transformation"
        self.transformation = transformation

        # If data is provided, split endog and exog
        if data is not None:
            # If endog column name is provided, pick that column
            if endog_column is not None:
                if endog_column in data.columns:
                    self.endog = data[[endog_column]]
                    self.exog = data.drop(columns=[endog_column]) if data.shape[1] > 1 else None
            else:
                # Pick first column as endog
                self.endog = pd.DataFrame(data.iloc[:, 0])
                self.exog = pd.DataFrame(data.iloc[:, 1:]) if data.shape[1] > 1 else None
        else:
            self.endog = endog
            self.exog = exog

        assert self.endog is not None, "Must provide endogenous variable"

        # Apply transformation to endogenous variable
        if self.transformation == "sqrt":
            self.endog = np.sqrt(self.endog)

        self.endog_name = self.endog.columns[0]
        self.exog_names = self.exog.columns.to_list() if self.exog is not None else None
        self.config = config
        self.arima_order = config["arima_order"] if "arima_order" in config else (1, 0, 0)
        self.seasonal_order = config["seasonal_order"] if "seasonal_order" in config else (0, 0, 0, 0)
        self.trend = config["trend"] if "trend" in config else None

        if self.arima_order[1] + self.seasonal_order[1] > 2:
            raise SarimaxException("Invalid configuration")

        self.enforce_stationarity = not relax_constraints
        self.enforce_invertibility = not relax_constraints
        self.convergence_warnings = convergence_warnings

        try:
            self.model = SARIMAX(endog=self.endog, exog=self.exog,
                                 order=self.arima_order, seasonal_order=self.seasonal_order,
                                 trend=self.trend,
                                 enforce_stationarity=self.enforce_stationarity,
                                 enforce_invertibility=self.enforce_invertibility)
        except ValueError as e:
            if e.args[0] == 'Must include nonzero seasonal periodicity if including seasonal AR, MA, or differencing.':
                raise SarimaxException("Invalid configuration")
            else:
                raise e

        self.trained = False
        self.cross_validated = False
        self.fitted_model = None

        self.aic = None
        self.training_performance = {}  # Warning: if endog are transformed, measurement errors are not the
        # same units as the untransformed quantity
        self.test_performance = {}

    def fit(self):
        if self.convergence_warnings:
            self.fitted_model = self.model.fit(disp=False)
        else:
            with catch_warnings():
                filterwarnings("ignore")
                self.fitted_model = self.model.fit(disp=False)

        self.trained = True
        self.aic = self.fitted_model.aic
        training_mse = self.fitted_model.mse
        self.training_performance["MSE"] = training_mse
        self.training_performance["RMSE"] = np.sqrt(training_mse)
        return self

    def cross_validate(self, splits=None, performance_measure=rmse):

        data = self.endog  # if transformation != None, data are tranformed
        if self.exog is not None:
            data = data.join(self.exog)

        performance_mean, performance_se = model_cross_validation(data, splits,
                                                                  performance_measure=performance_measure,
                                                                  model=Sarimax,
                                                                  config=self.config,
                                                                  convergence_warnings=False,
                                                                  transformed=self.transformation)
        self.cross_validated = True
        self.test_performance[performance_measure.__name__.upper()] = performance_mean
        self.test_performance[performance_measure.__name__.upper() + "_se"] = performance_se
        return self

    def get_summary(self):
        assert self.trained, "Untrained model"

        return self.fitted_model.summary()

    def plot_diagnostics(self):
        assert self.trained, "Untrained model"
        with catch_warnings():
            filterwarnings("ignore")
            self.fitted_model.plot_diagnostics()

    def forecast(self, steps=1, exog=None):
        assert self.trained, "Untrained model"

        forecast = self.fitted_model.get_forecast(steps=steps, exog=exog)
        forecast_df = forecast.summary_frame()
        forecast_df.columns = ["forecast", "se", "lower_ci", "upper_ci"]

        if self.transformation == "sqrt":
            forecast_df = np.power(forecast_df, 2)
        return forecast_df

    def get_prediction_and_forecast_df(self, steps=1, exog=None):
        assert self.trained, "Untrained model"

        forecast_df = self.forecast(steps=steps, exog=exog)
        fit_result_df = self.endog.copy()
        fit_result_df.columns = ["data"]
        fit_result_df["fitted"] = self.fitted_model.fittedvalues

        if self.transformation == "sqrt":
            fit_result_df = np.power(fit_result_df, 2)
        return fit_result_df.join(forecast_df.iloc[:, [0, 2, 3]], how="outer")

    def plot_prediction_and_forecast(self, steps=1, exog=None, plot_residuals=False):
        assert self.trained, "Untrained model"

        fit_result_df = self.get_prediction_and_forecast_df(steps=steps, exog=exog)
        residuals = self.fitted_model.resid if plot_residuals else None

        if self.transformation == "sqrt" and plot_residuals:
            residuals = np.power(residuals, 2)

        title = f"SARIMAX {self.config}"
        return plot_model(fit_result_df, quantity=self.endog_name,
                          title=title,
                          residuals=residuals,
                          confint="95%")

    def __str__(self):
        s = f"SARIMAX {self.config}"
        if self.transformation is not None:
            s += f"\n   transformation: {self.transformation}"
        if self.trained:
            s += f"\n   trained (AIC={self.aic})"
        if self.cross_validated:
            s += f"\n   cross_validated {self.test_performance}"
        return s

    def __repr__(self):
        s = f"SARIMAX {self.config}"
        if self.trained:
            s += ", trained"
        return s
