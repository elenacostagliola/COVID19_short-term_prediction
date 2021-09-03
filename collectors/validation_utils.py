from pandas import Timestamp

COVID_START = "2020-02-23"


def validate_dates(date_from, date_to) -> (Timestamp, Timestamp):
    """
    Validation function to conform dates to a standard.

    :param date_from: starting date. If None, uses February, 23rd 2021, i.e. date of first measurement.
    :param date_to: ending date. If None, uses today date.
    :return: validated date timestamps.
    """
    from_date = Timestamp(COVID_START, tz='Europe/Vatican') if date_from is None else Timestamp(date_from,
                                                                                                tz='Europe/Vatican')
    to_date = Timestamp("now", tz='Europe/Vatican') if date_to is None else Timestamp(date_to, tz='Europe/Vatican')

    return from_date.date(), to_date.date()
