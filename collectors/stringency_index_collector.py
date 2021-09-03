import time
from typing import Optional, List
import pandas as pd
from pandas import Timestamp
import requests

from data.models import StringencyIndex
from .validation_utils import validate_dates


class StringencyIndexCollector:

    def __init__(self):
        self.base_url = "https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/"
        self.max_retries = 5

    def get_url(self, date_from: Timestamp, date_to: Timestamp) -> str:
        """
        Creates the url with the time frame and on which to make the request.
        :param date_from: starting date.
        :param date_to: end date.
        :return: url as a string.
        """
        return f"{self.base_url}/{date_from.strftime('%Y-%m-%d')}/{date_to.strftime('%Y-%m-%d')}"

    def get_data(self, date_from: Timestamp, date_to: Timestamp) -> dict:
        """
        Collects worldwide Stringency index within a time range.
        :param date_from: starting date.
        :param date_to: end date.
        :return: a dictionary containing worldwide stringency index values.
        """
        url = self.get_url(date_from, date_to)
        n_retries = 0
        data = None
        while n_retries < self.max_retries:
            res = requests.get(url)
            if res.status_code == 200:
                data = res.json()
                break
            n_retries += 1
            time.sleep(1 + n_retries)
        assert data is not None, "Could not get stringency index from API"
        return data

    def clean_data(self, data: dict) -> list:
        """
        Filters data on Italy and formats date.
        :param data: stringency index's dictionary.
        :return: list of dictionaries.
        """
        if "status" in data:
            if data["status"] == "error":
                if data["code"] == 404:
                    return []
        else:
            return [{"date": pd.to_datetime(d, format="%Y-%m-%d"), "value": data["data"][d]["ITA"]["stringency"]} for d in
                    data["data"]]

    def search(self, date_from=None, date_to=None) -> List[StringencyIndex]:
        """
        Collects Italian Stringency index within a date range.
        :param date_from: start date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: end date string. If None, uses today date.
        :return: data modelled as List[StringencyIndex].
        """
        date_from, date_to = validate_dates(date_from, date_to)
        data = self.get_data(date_from, date_to)
        data = self.clean_data(data)

        return StringencyIndex.from_repr(data)
