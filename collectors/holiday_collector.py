from typing import Optional, List
import requests
from pandas import Timestamp

from data.models import Holiday
from configuration.googleapis_config import GOOGLE_APIKEY
from .validation_utils import validate_dates


class HolidayCollector:

    def __init__(self):
        pass

    def get_data(self, date_from: Timestamp, date_to: Timestamp) -> List[dict]:
        """
        Gets data as string about Italian Holidays within a date range.
        :param date_from: starting date.
        :param date_to: end date.
        :return: string containing Italian Holidays.
        """
        url = f"https://www.googleapis.com/calendar/v3/calendars/en.italian%23holiday%40group.v.calendar.google.com/events?key={GOOGLE_APIKEY}" + \
              f"&timeMin={date_from.isoformat() + 'T00:00:00Z'}&timeMax={date_to.isoformat() + 'T00:00:00Z'}"

        return requests.get(url).json()['items']

    def clean_data(self, data: List[dict]) -> List[dict]:
        """
        Filters data by keeping the name and the start and end dates of each Italian Holiday.
        :param data: items referred to Italian Holidays.
        :return: Return a sorted by starting date list of dictionaries containing the Italian Holidays and the days
        they start and end as timestamps.
        """

        lst = [{'holiday': x['summary'], 'start': Timestamp(x['start']['date']), 'end': Timestamp(x['end']['date'])} for
               x in data]

        return sorted(lst, key=lambda k: k['start'])

    def search(self, date_from=None, date_to=None) -> List[Holiday]:
        """
        Collects data about Italian Holidays within a range of date.
        :param date_from: start date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: end date string. If None, uses today date.
        :return: data modelled as List[Holiday].
        """
        # dates validation
        date_from, date_to = validate_dates(date_from, date_to)

        data = self.get_data(date_from, date_to)
        data = self.clean_data(data)
        data = Holiday.from_repr(data)
        return data
