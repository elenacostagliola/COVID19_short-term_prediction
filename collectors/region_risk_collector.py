from typing import Optional, List
import pandas as pd

from data.models import RegionRisk
from .validation_utils import validate_dates


class RegionRiskCollector:

    def __init__(self):
        pass

    def get_data(self) -> pd.DataFrame:
        """
        Gets data from github repo about day by day Italian regions' epidemiological scenarios identified by colors.
        and stipulated by official ordinances.
        :return: pd.DataFrame containing 4 levels of risk by dates.
        """
        url = 'https://raw.githubusercontent.com/imcatta/restrizioni_regionali_covid/main/dataset.json'
        return pd.read_json(url)

    def clean_data(self, df: pd.DataFrame, date_from: str, date_to: str) -> pd.DataFrame:
        """
        Renames both columns and rows. Data filtered by timestamp dates and keeping Province of Trento risk of
        infection.
        :param df: pd.Dataframe about Province of Trento risk.
        :param date_from: start date string.
        :param date_to: end date string.
        :return: cleaned and filtered by date pd.Dataframe.
        """
        # rename columns
        df = df.rename(columns={'data': 'date', 'denominazione_regione': 'region', 'colore': 'risk'}, errors='raise')
        # filter by Trentino and rename
        df = df[df['region'] == 'Provincia autonoma Trento']
        df['region'] = 'PAT'
        # translate in English
        df['risk'] = df['risk'].map({'giallo': 'yellow', 'arancione': 'orange', 'rosso': 'red', 'bianco':'white'})
        # reformat date into d/m/Y
        df['date'] = df['date'].apply(lambda d: pd.to_datetime(d, format="%Y-%m-%d").date())
        # filter by date
        mask = (df['date'] > date_from) & (df['date'] <= date_to)
        df = df.loc[mask]
        df['date'] = df['date'].apply(lambda d: pd.Timestamp(d))
        return df

    def search(self, date_from=None, date_to=None) -> List[RegionRisk]:
        """
        Collects data about Province of Trento risk of infection within a range of dates.
        :param date_from: start date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: end date string. If None, uses today date.
        :return: List[RegionRisk] object.
        """
        date_from, date_to = validate_dates(date_from, date_to)
        df = self.get_data()
        df = self.clean_data(df, date_from, date_to)

        return RegionRisk.from_df(df)
