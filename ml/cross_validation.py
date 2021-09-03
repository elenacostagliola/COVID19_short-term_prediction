import numpy as np
from .performance_measures import rmse


def model_cross_validation(data=None, splits=None, model=None, transformed=None, performance_measure=rmse,
                           **kwargs):
    assert data is not None, "Missing data"
    assert splits is not None, "Missing splits"
    assert model is not None, "Missing model"

    performance_list = []

    for split in splits:
        # Train
        model_instance = model(data=data.iloc[split[0], :], **kwargs).fit()

        # Test
        forecast_df = model_instance.forecast(len(split[1]), exog=data.iloc[split[1], 1:] if data.shape[1]>1 else None)

        # Calculate performance
        actual = data.iloc[split[1], :]
        prediction = forecast_df["forecast"]

        if transformed == "sqrt":
            actual = np.power(actual, 2)
            prediction = np.power(prediction, 2)

        performance = performance_measure(actual, prediction)
        performance_list.append(performance)

    # Compute average performance measure
    performance_mean = np.mean(performance_list)
    performance_se = np.std(performance_list, ddof=1) / np.sqrt(len(performance_list))

    return performance_mean, performance_se
