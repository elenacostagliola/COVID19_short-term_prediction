from sklearn.model_selection import TimeSeriesSplit


def split_train_test_last_n(data, n=1):
    # Split dataset into train/test sets for walk-forward validation
    # Default leaves last element as test set
    return data.iloc[:-n, :], data.iloc[-n:, :]


class TsCvSplitter:
    def __init__(self, n_splits=10, max_test_size=None):
        self.n_splits = n_splits
        self.max_test_size = max_test_size
        self.__splitter = TimeSeriesSplit(n_splits=n_splits)
        self.min_train_size = None
        self.max_train_size = None
        self.test_size = None
        self.splits = None

    def split(self, data):
        splits = []
        train_sizes = []
        for train, test in self.__splitter.split(data):
            train_sizes.append(len(train))
            if self.max_test_size is not None:
                if len(test) > self.max_test_size:
                    test = test[:self.max_test_size]
            splits.append([train, test])
        self.min_train_size = min(train_sizes)
        self.max_train_size = max(train_sizes)
        self.test_size = len(splits[0][1])
        self.splits = splits
        return splits
