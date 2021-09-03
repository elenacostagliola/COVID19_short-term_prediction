import pandas as pd
from typing import List

from data.models import WeatherStation, MunicipalityWeatherStationLink

ALLOWED_RULES = ["max_sensors_min_elevation"]


class MunicipalityWeatherStationLinker:

    def __init__(self, elevation_threshold=1600, rule="max_sensors_min_elevation"):
        self.elevation_threshold = elevation_threshold
        assert rule in ALLOWED_RULES, "Unrecognised rule"
        self.rule = rule  # Allows for having multiple rules

    def run(self, weather_stations: List[WeatherStation]) \
            -> List[MunicipalityWeatherStationLink]:

        ws = WeatherStation.to_df(weather_stations)

        edgelist = None

        # Apply rule(s)
        if self.rule == "max_sensors_min_elevation":

            # Keep stations below elevation threshold and with a non-null number of active sensors
            ws = ws[(ws.elev < self.elevation_threshold) & (ws.n_sensors > 0)]

            # Find maximum number of sensors for each municipality
            max_n_sensors = ws.groupby("mun_code")["n_sensors"].max()

            # For each municipality, select station(s) with max number of sensors
            candidate_stations = pd.DataFrame()
            for mun_c, n_sens in max_n_sensors.items():
                candidate_stations = candidate_stations.append(ws[(ws.mun_code == mun_c) & (ws.n_sensors == n_sens)])

            # If there are more stations for each municipality, pick one lowest in altitude
            # Note: this step is currently unnecessary but still performed
            min_altitude = candidate_stations.groupby("mun_code")["elev"].min()
            chosen_stations = pd.DataFrame()
            for mun_c, elev in min_altitude.items():
                chosen_stations = chosen_stations.append(
                    candidate_stations[(candidate_stations.mun_code == mun_c) & (candidate_stations.elev == elev)])

            # Pick edges (defined by nodes)
            edgelist = chosen_stations[["mun_code", "station_id"]] \
                .rename(columns={"mun_code": "municipality_code"}) \
                .reset_index(drop=True)

        # Export
        edgelist = MunicipalityWeatherStationLink.from_df(edgelist)
        return edgelist
