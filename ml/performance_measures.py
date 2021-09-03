from sklearn.metrics import mean_squared_error
from numpy import sqrt


def rmse(actual, predicted):
    try:
        score = sqrt(mean_squared_error(actual.iloc[:, 0], predicted))
    except ValueError as e:
        if 'Input contains NaN, infinity or a value too large for dtype' in e.args[0]:
            # Set high score
            score = 1e6
        else:
            raise e
    return score
