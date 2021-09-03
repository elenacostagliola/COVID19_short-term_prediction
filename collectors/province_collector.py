import pandas as pd
from data.models import *


class ProvinceCollector:

    def __init__(self, batch_size=10):
        self.batch_size = batch_size
        self.repo = "napo/covid19apssdashboard"
        self.path = "data/codici_comuni.csv"

    def get_data(self) -> pd.DataFrame:
        """
        Gets data about municipalities in Trentino.

        :return: pd.DataFrame.
        """
        url = f"https://raw.githubusercontent.com/{self.repo}/master/{self.path}"
        return pd.read_csv(url)

    def search(self) -> List[Province]:
        """
        Collects data about municipality and sums by their population.

        :return: one row pd.Dataframe about Province of Trento population and code.
        """
        data = self.get_data()
        df = pd.DataFrame({"name": ["PAT"], "code": [22], "population": [data['abitanti'].sum()]})
        return Province.from_df(df)
