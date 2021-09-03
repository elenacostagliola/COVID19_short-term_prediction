from __future__ import absolute_import, annotations

import pandas as pd
import datetime

from .validation_utils import validate_dates
from .github_utils import get_commits_table, get_data_version
from data.models import *


class MunicipalityDataCollector:

    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self.repo = "napo/covid19apssdashboard"
        self.path = "data/stato_comuni_td.csv"

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans municipality data by formatting date column, dropping unused columns and translating the others.

        :param df: input dataframe.
        :return: sorted by date and municipality's code dataframe.
        """

        def parse_date_custom(d):
            try:
                d = pd.to_datetime(d, format="%d/%m/%Y")
            except ValueError:
                d = pd.to_datetime(d, format="%d/%m/%y")
            return d

        df['aggiornamento'] = df['aggiornamento'].apply(parse_date_custom)
        df = df.drop(columns=["lat", "lon", "nome"]) \
            .rename(columns={"aggiornamento": "date",
                             "codice": "code",
                             "contagi": "cases",
                             "guariti": "recovered",
                             "decessi": "deaths",
                             "dimessi": "discharged"}) \
            .sort_values(["date", "code"])
        return df

    def search(self, date_from=None, date_to=None) -> List[MunicipalityData]:
        """
        Gets covid 19 data for municipalities in the Province of Trento within a date range.

        :param date_from: starting date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: ending date string. If None, uses today date.
        :return:
        """
        # dates validation
        from_date, to_date = validate_dates(date_from, date_to)

        # setting starting parameters
        page = 0
        more_commits = True
        more_pages = True
        commits_df = pd.DataFrame(columns=['date', 'commit_id'])

        # filling dataframe with all available commits
        while more_commits and more_pages:
            page += 1

            newpage_commits = get_commits_table(self.repo, self.path, page)

            if newpage_commits.shape[0] > 0:
                # Filter by date
                newer = newpage_commits["date"] > to_date
                older = newpage_commits["date"] < from_date
                newpage_commits = newpage_commits[~(older | newer)]

                # If the last commit is old, there will be no other commits to search for
                more_commits = ~older.iloc[-1]

                commits_df = commits_df.append(newpage_commits, ignore_index=True)
            else:
                more_pages = False

        # gets data by commit id
        data_version_df = pd.DataFrame()
        for index, commit in commits_df.iterrows():
            data_version_commit_df = get_data_version(self.repo, self.path, commit['commit_id'])
            data_version_df = data_version_df.append(data_version_commit_df, ignore_index=True)

        # data cleaning
        data_version_df = self.clean_data(data_version_df)

        data = MunicipalityData.from_df(data_version_df)
        return data


class ProvinceDataCollector:

    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self.repo = "napo/covid19apssdashboard"
        self.path = "data/stato_clinico_td.csv"

    def clean_data(self, df: pd.DataFrame, date_from: datetime.date, date_to: datetime.date) -> pd.DataFrame:
        """
        Cleans the province data by removing unused columns and translating the others. Data are filtered within
        a date range.

        :param df: province pd.Dataframe.
        :param date_from: starting date.
        :param date_to: ending date.
        :return: cleaned pd.Dataframe.
        """
        df = df.rename(columns={"giorno": "date", "domicilio": "quarantined",
                                "infettive": "hospitalized_infectious_diseases",
                                "alta_int": "hospitalized_high_intensity", "terapia_in": "hospitalized_intensive_care",
                                "guariti": "recovered", "deceduti": "deaths",
                                "totale_pos": "cases", "pos_att": "active", "rsa": "active_rsa",
                                "incremento": "new_cases", "casa_cura": "active_nursing_homes",
                                "strut_int": "active_int_struct", "tot_rsa": "active_rsa_total",
                                "dimessi": "discharged"})
        df = df.drop(columns=["nuovi", "nuo_screen"])
        df = df[(df["date"].apply(lambda x: x.date()) >= date_from) & (
                df["date"].apply(lambda x: x.date()) <= date_to)]

        return df

    def search(self, date_from=None, date_to=None) -> List[ProvinceData]:
        """
        Collects covid 19 data for the Province of Trento within a date range.

        :param date_from: start date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: end date string. If None, uses today date.
        :return:
        """
        # Date validation
        from_date, to_date = validate_dates(date_from, date_to)

        url = f"https://raw.githubusercontent.com/{self.repo}/master/{self.path}"
        df = pd.read_csv(url)

        # Data cleaning
        df["giorno"] = pd.to_datetime(df["giorno"], format="%d/%m/%Y")
        assert len(df) > 0, "Province search returned no data"
        df = self.clean_data(df, from_date, to_date)

        # Add province name
        df["name"] = "PAT"

        # Export. Models data as List[ProvinceData]
        data = ProvinceData.from_df(df)
        return data
