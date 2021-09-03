import pandas as pd
from data.models import *


class MunicipalityCollector:

    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self.repo = "napo/covid19apssdashboard"
        self.path_pop = "data/stato_comuni_td.csv"
        self.path_codes = "data/codici_comuni.csv"

    def get_data(self, path: str) -> pd.DataFrame:
        """
        Gets data about municipalities in Trentino.

        :param path: path of data file.
        :return: pd.DataFrame.
        """
        url = f"https://raw.githubusercontent.com/{self.repo}/master/{path}"
        return pd.read_csv(url)

    def clean_data(self, df_codes: pd.DataFrame, df_pop: pd.DataFrame) -> pd.DataFrame:
        """
        Drops and renames columns from dataframes referred to municipalities' codes and population.
        The two input dataframes are then merged.
        :param df_codes: dataframe containing Province of Trento municipalities codes.
        :param df_pop: dataframe containing Province of Trento municipalities number of inhabitants.
        :return: a sorted by code pd.DataFrame with municipalities' code, name, lat, lon and number of population.
        """
        df_pop = df_pop.drop(columns=["aggiornamento", "nome", "contagi", "guariti", "decessi", "dimessi"]) \
            .rename(columns={"codice": "code"}) \
            .sort_values(["code"])
        df_codes = df_codes.rename(columns={"codice": "code",
                                            "comune": "name",
                                            "abitanti": "population"}) \
            .sort_values(["code"])
        df_codes['province'] = "PAT"
        return pd.merge(df_codes, df_pop, on='code')

    def search(self) -> List[Municipality]:
        """
        Collects demographic data about municipalities in Trentino.

        :return: a List[Municipality] object.
        """
        df_pop = self.get_data(self.path_pop)
        df_codes = self.get_data(self.path_codes)
        df = self.clean_data(df_codes, df_pop)

        return Municipality.from_df(df)
