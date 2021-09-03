from time import sleep
from urllib.error import URLError
from typing import List

import pandas as pd
import requests

from . import validation_utils
from data.models import WeatherStation, WeatherData, Municipality
from .timeout import exit_after


class WeatherStationCollector:

    def __init__(self, _test=False):
        self.gpkg_path = "./resources/istat_administrative_units_2020.gpkg"
        self.sensor_information_retrieval_lag = 7  # days ago
        self.sensor_information_retrieval_window = 3  # days
        self._test = _test

    def _fetch_active_weather_stations(self, municipalities):
        import geopandas as gpd

        # Source 1: get municipalities df (read from DB by the pipeline calling this collector)
        mun_df = Municipality.to_df(municipalities)
        mun_geodf = gpd.GeoDataFrame(mun_df,
                                     geometry=gpd.points_from_xy(
                                         mun_df.lon, mun_df.lat),
                                     crs="EPSG:4326")

        # Source 2: Get ISTAT administrative units dataset (file)

        provincies_italy_istat = gpd.read_file(self.gpkg_path, layer="provincies")
        municipalities_italy_istat = gpd.read_file(self.gpkg_path, layer="municipalities")
        mun_istat_geodf = municipalities_italy_istat[municipalities_italy_istat.COD_PROV == provincies_italy_istat[
            provincies_italy_istat.DEN_PROV == "Trento"].COD_PROV.values[0]].to_crs(epsg=4326)

        # Source 3: Get weather station table from meteotrentino.it and filter active stations
        weather_stations_table_url = "https://content.meteotrentino.it/dati-meteo/stazioni/dati-Stazioni-Json.aspx"
        res = requests.get(weather_stations_table_url)
        wstations_all = pd.DataFrame(res.json()["records"])
        wstations_active = wstations_all[wstations_all["fine"] == '']
        wstations_geodf = gpd.GeoDataFrame(wstations_active.drop(columns=["est", "nord", "fine", "inizio"]),
                                           geometry=gpd.points_from_xy(
                                               wstations_active.lon, wstations_active.lat),
                                           crs="EPSG:4326")

        # Perform spatial join
        joined = gpd.sjoin(gpd.sjoin(mun_istat_geodf, wstations_geodf, op="contains", lsuffix="istat", rsuffix="ws"),
                           mun_geodf, op="contains", lsuffix="joined", rsuffix="db")

        # Clean weather station data
        wstations_cleaned = joined[["id", "nome", "lat_joined", "lon_joined", "elev", "code"]] \
            .rename(
            columns={"id": "station_id", "nome": "name",
                     "lat_joined": "lat", "lon_joined": "lon",
                     "code": "mun_code"}) \
            .reset_index(drop=True)

        return wstations_cleaned

    def _expand_with_sensor_data(self, ws):
        # Calculate sensor availability
        today = pd.Timestamp("now", tz="Europe/Rome")
        date_from = (today - pd.Timedelta(days=self.sensor_information_retrieval_lag))
        date_to = (today - pd.Timedelta(
            days=self.sensor_information_retrieval_lag - self.sensor_information_retrieval_window))

        def calculate_sensor_information(row):
            print(
                f"\rProcessing station {row.name + 1} of {len(ws)} [{round((row.name + 1) / len(ws) * 100)} %]",
                end="")

            res = WeatherDataCollector()._scrape_weather_data(row["station_id"], date_from, date_to)

            # Find available quantities
            avail_q = list(res.loc[:, res.isna().sum() == 0].columns)
            n_avail_q = len(avail_q) - 2

            row["n_sensors"] = n_avail_q
            row["precipitation_available"] = "precipitation_total_mm" in avail_q
            row["temperature_available"] = "temperature_mean_c" in avail_q
            row["humidity_available"] = "humidity_mean_percent" in avail_q
            row["wind_speed_available"] = "wind_speed_mean_ms" in avail_q
            row["atmospheric_pressure_available"] = "atmospheric_pressure_mean_hpa" in avail_q
            row["solar_rad_available"] = "solar_rad_total_kjm2" in avail_q

            return row

        if self._test:
            ws = ws.iloc[:10]

        ws_withsensors = ws.apply(calculate_sensor_information, axis=1)
        print()

        return ws_withsensors

    def search(self, municipalities):
        ws = self._fetch_active_weather_stations(municipalities)
        ws = self._expand_with_sensor_data(ws)

        # Export
        ws = WeatherStation.from_df(ws)
        return ws


@exit_after(15)
def get_weather_data_from_api(url):
    return pd.read_html(url)


class WeatherDataCollector:

    def __init__(self):
        self.max_attempts = 5
        self.retry_initial_interval = 1  # seconds
        self.retry_interval_increment = 3  # seconds

    def _get_weatherdata_url(self, station_id: str, date_from, date_to):
        # Parse dates and transform into required string format
        date_from = pd.Timestamp(date_from).strftime("%d/%m/%Y")
        date_to = pd.Timestamp(date_to).strftime("%d/%m/%Y")

        endpoint = "storico.meteotrentino.it/cgi/webhyd.pl"
        url = f"http://{endpoint}?co={station_id}&v=10.00_10.00,400.00_400.00,430.00_430.00,515.00_515.00," \
              f"550.00_550.00,750.00_750.00&vn=Pioggia%20(millimetri)%20,Temperatura%20aria%20(gradi%20Celsius)%20," \
              f"Umidita%27%20aria%20(percentuale)%20,Velocita%27%20vento%20media%20(metri/secondo)%20," \
              f"Pressione%20atmosferica%20(Ettopascal)%20,Radiazione%20solare%20totale%20(" \
              f"KJoule/metroquadro)%20Globale&p=Altro,1,1,custom,1&o=Tabella,data&i=Giornaliera,Day,1&cat=rs&d1=" \
              f"{date_from}&d2={date_to}&1620143092583 "
        return url

    def _clean_weather_data_table(self, df):
        df.columns = df.loc[2, :]
        df.columns.name = None
        df = df.iloc[4:, :12] \
            .rename(columns={'Date': 'date',
                             'Pioggia (mm)': 'precipitation_total_mm',
                             'Temp. aria (°C)': 'temperature_mean_c',
                             "Umidita' aria (%)": 'humidity_mean_percent',
                             'Vel. Vento (m/s)': 'wind_speed_mean_ms',
                             'Pressione atm. (hPa)': 'atmospheric_pressure_mean_hpa',
                             'Rad.Sol.Tot. (kJ/m2)': 'solar_rad_total_kjm2'})
        df = df.loc[df.date.notna(), df.columns.notna()]
        df.date = pd.to_datetime(df.date, format="%H:%M:%S %d/%m/%Y")
        df = df.set_index("date").sort_index()
        df = df.loc[df.notna().sum(axis=1) > 0].reset_index(drop=False)
        df.iloc[:, 1:] = df.iloc[:, 1:].astype(float)
        return df

    def _scrape_weather_data(self, station_id: str, date_from, date_to):
        url = self._get_weatherdata_url(station_id, date_from, date_to)

        sleep_duration = self.retry_initial_interval
        n_attempts = 0
        tables_list = None

        while n_attempts < self.max_attempts:
            try:
                try:
                    tables_list = get_weather_data_from_api(url)
                    break
                except KeyboardInterrupt:
                    raise TimeoutError
            except (URLError, TimeoutError):
                print(f"Failed connection attempt n. {n_attempts + 1}. Retrying in {sleep_duration} s")
                sleep(sleep_duration)
                n_attempts += 1
                sleep_duration += self.retry_interval_increment

        if tables_list is not None:
            df = tables_list[0]
            df = self._clean_weather_data_table(df)
            df["station_id"] = station_id
            return df
        else:
            print(f"WeatherDataCollector was unable to connect to the server.")

            # Return empty dataframe
            return pd.DataFrame()

    def search(self, station_id: str, date_from=None, date_to=None) -> List[WeatherData]:
        date_from, date_to = validation_utils.validate_dates(date_from, date_to)
        wdata = self._scrape_weather_data(station_id, date_from, date_to)
        wdata = WeatherData.from_df(wdata)
        return wdata
