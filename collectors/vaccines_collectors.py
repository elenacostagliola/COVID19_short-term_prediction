import datetime

import pandas as pd

from .validation_utils import validate_dates
from data.models import *


class VaccinesDataCollector:

    def __init__(self, repo: str, path: str, date_column: str, batch_size=10):
        self.batch_size = batch_size
        self.repo = repo
        self.path = path
        self.date = date_column

    def clean_data(self, df:pd.DataFrame, date_from: datetime.date, date_to: datetime.date) -> pd.DataFrame:
        """
        Renames columns and filter by date range.
        :param df: pd.Dataframe about Italian vaccination campaign.
        :param date_from: start date.
        :param date_to: end date.
        :return:
        """
        df = df[df['area'] == 'PAT']
        df = df.rename(columns={"area": "region", "fornitore": "supplier"})
        df = df.drop(columns=["codice_NUTS1", "codice_NUTS2", "nome_area", "codice_regione_ISTAT"])
        df = df[(df[self.date].apply(lambda x: x.date()) >= date_from) & (
                df[self.date].apply(lambda x: x.date()) <= date_to)]

        return df

    def search_vax(self, date_from=None, date_to=None) ->pd.DataFrame:
        """
        Collects data about Italian vaccination campaign.
        :param date_from: start date string. If None, uses February, 23rd 2021, i.e. date of first measurement.
        :param date_to: end date string. If None, uses today date.
        :return: pd.Dataframe.
        """
        from_date, to_date = validate_dates(date_from, date_to)

        url = f"https://raw.githubusercontent.com/{self.repo}/master/{self.path}"
        df = pd.read_csv(url)
        df[self.date] = pd.to_datetime(df[self.date], format="%Y-%m-%d")
        assert len(df) > 0, "Vaccines delivery search returned no data"
        return self.clean_data(df, from_date, to_date)


class VaccinesDeliveryDataCollector(VaccinesDataCollector):

    def __init__(self, batch_size=10):
        super().__init__("italia/covid19-opendata-vaccini",
                         "dati/consegne-vaccini-latest.csv",
                         "data_consegna",
                         batch_size=batch_size)

    def search(self, date_from=None, date_to=None) -> List[VaccinesDeliveryData]:
        """
        Collects data about vaccines delivery.
        :param date_from: start date string.
        :param date_to: end date string.
        :return: List[VaccinesDeliveryData]
        """
        vax = self.search_vax(date_from, date_to)
        vax = vax.rename(columns={"numero_dosi": "n_doses", "data_consegna": "delivery_date"})
        data = VaccinesDeliveryData.from_df(vax)
        return data


class VaccinesAdministrationDataCollector(VaccinesDataCollector):

    def __init__(self, batch_size=10):
        super().__init__("italia/covid19-opendata-vaccini",
                         "dati/somministrazioni-vaccini-latest.csv",
                         "data_somministrazione",
                         batch_size=batch_size)

    def search(self, date_from=None, date_to=None) -> List[VaccinesAdministrationData]:
        """
        Collects data about vaccines administrations.
        :param date_from: start date string.
        :param date_to: end date string.
        :return: List[VaccinesDeliveryData]
        """
        vax = self.search_vax(date_from, date_to)
        vax = vax.rename(columns={"data_somministrazione": "administration_date", "fascia_anagrafica": "age_group",
                                   "sesso_maschile": "male", "sesso_femminile": "female", "prima_dose": "first_dose",
                                   "seconda_dose": "second_dose"})
        data = VaccinesAdministrationData.from_df(vax)
        return data
