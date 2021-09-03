from __future__ import absolute_import, annotations

import pandas as pd
from numpy import isnan
from typing import Optional, List


class Municipality:

    def __init__(self, code: int, name: str, province: str, lat: float, lon: float, population: int):
        self.code = code  # pk
        self.name = name
        self.province = province  # fk references Province(name)
        self.lat = lat
        self.lon = lon
        self.population = population  # Loaded one-time and might be updatable later

    @staticmethod
    def from_repr(lst_dict: list) -> List[Municipality]:
        out = []
        for d in lst_dict:
            out.append(Municipality(
                d["code"],
                d["name"],
                d["province"],
                d["lat"],
                d["lon"],
                d["population"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[Municipality]) -> list:
        out = []
        for instance in lst:
            out.append({
                "code": instance.code,
                "name": instance.name,
                "province": instance.province,
                "lat": instance.lat,
                "lon": instance.lon,
                "population": instance.population
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[Municipality]:
        return Municipality.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[Municipality]) -> pd.DataFrame:
        return pd.DataFrame(Municipality.to_repr(lst))


class MunicipalityData:

    def __init__(self, date: pd.Timestamp, code: int, cases: int, recovered: int, deaths: int, discharged: bool):
        self.date = date
        self.code = code  # fk references Municipality(code)
        self.cases = cases
        self.recovered = recovered
        self.deaths = deaths
        self.discharged = discharged  # nullable

    @staticmethod
    def from_repr(lst_dict: list) -> List[MunicipalityData]:
        out = []
        for d in lst_dict:
            out.append(MunicipalityData(
                d["date"],
                d["code"],
                d["cases"],
                d["recovered"],
                d["deaths"],
                d["discharged"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[MunicipalityData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "date": instance.date,
                "code": instance.code,
                "cases": instance.cases,
                "recovered": instance.recovered,
                "deaths": instance.deaths,
                "discharged": instance.discharged
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[MunicipalityData]:
        return MunicipalityData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[MunicipalityData]) -> pd.DataFrame:
        return pd.DataFrame(MunicipalityData.to_repr(lst))


class Province:

    def __init__(self, name: int, code: int, population: int):
        self.name = name  # id
        self.code = code  # nullable
        self.population = population  # Loaded one-time and might be updatable later

    @staticmethod
    def from_repr(lst_dict: list) -> List[Province]:
        out = []
        for d in lst_dict:
            out.append(Province(
                d["name"],
                d["code"],
                d["population"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[Province]) -> list:
        out = []
        for instance in lst:
            out.append({
                "name": instance.name,
                "code": instance.code,
                "population": instance.population
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[Province]:
        return Province.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[Province]) -> pd.DataFrame:
        return pd.DataFrame(Province.to_repr(lst))


class ProvinceData:

    def __init__(self, date: pd.Timestamp, name: str, cases: int, new_cases: int, active: int, recovered: int,
                 deaths: int, quarantined: int, hospitalized_infectious_diseases: int, hospitalized_high_intensity: int,
                 hospitalized_intensive_care: int, discharged: int, active_rsa: int, active_nursing_homes: int,
                 active_int_struct: int, active_rsa_total: int):
        # General
        self.date = date
        self.name = name  # fk references Province(name)

        # Main metrics
        self.cases = cases
        self.new_cases = new_cases
        self.active = active
        self.recovered = recovered
        self.deaths = deaths
        self.quarantined = quarantined

        # Hospitalization - related
        self.hospitalized_infectious_diseases = hospitalized_infectious_diseases
        self.hospitalized_high_intensity = hospitalized_high_intensity
        self.hospitalized_intensive_care = hospitalized_intensive_care
        self.discharged = discharged

        # RSA - related
        self.active_rsa = active_rsa
        self.active_nursing_homes = active_nursing_homes
        self.active_int_struct = active_int_struct
        self.active_rsa_total = active_rsa_total

    @staticmethod
    def from_repr(lst_dict: list) -> List[ProvinceData]:
        out = []
        for d in lst_dict:
            out.append(ProvinceData(
                d["date"],
                d["name"],
                d["cases"],
                d["new_cases"],
                d["active"],
                d["recovered"],
                d["deaths"],
                d["quarantined"],
                d["hospitalized_infectious_diseases"],
                d["hospitalized_high_intensity"],
                d["hospitalized_intensive_care"],
                d["discharged"],
                d["active_rsa"],
                d["active_nursing_homes"],
                d["active_int_struct"],
                d["active_rsa_total"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[ProvinceData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "date": instance.date,
                "name": instance.name,
                "cases": instance.cases,
                "new_cases": instance.new_cases,
                "active": instance.active,
                "recovered": instance.recovered,
                "deaths": instance.deaths,
                "quarantined": instance.quarantined,
                "hospitalized_infectious_diseases": instance.hospitalized_infectious_diseases,
                "hospitalized_high_intensity": instance.hospitalized_high_intensity,
                "hospitalized_intensive_care": instance.hospitalized_intensive_care,
                "discharged": instance.discharged,
                "active_rsa": instance.active_rsa,
                "active_nursing_homes": instance.active_nursing_homes,
                "active_int_struct": instance.active_int_struct,
                "active_rsa_total": instance.active_rsa_total
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[ProvinceData]:
        return ProvinceData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[ProvinceData]) -> pd.DataFrame:
        return pd.DataFrame(ProvinceData.to_repr(lst))


class RegionRisk:

    def __init__(self, date: pd.Timestamp, region: str, risk: str):
        self.date = date
        self.region = region  # fk refereces Province(name)
        self.risk = risk

    @staticmethod
    def from_repr(lst_dict: list) -> List[RegionRisk]:
        out = []
        for d in lst_dict:
            out.append(RegionRisk(
                d["date"],
                d["region"],
                d["risk"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[RegionRisk]) -> list:
        out = []
        for instance in lst:
            out.append({
                "date": instance.date,
                "region": instance.region,
                "risk": instance.risk
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[RegionRisk]:
        return RegionRisk.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[RegionRisk]) -> pd.DataFrame:
        return pd.DataFrame(RegionRisk.to_repr(lst))


class Holiday:

    def __init__(self, holiday: str, start: pd.Timestamp, end: pd.Timestamp):
        self.holiday = holiday
        self.start = start
        self.end = end

    @staticmethod
    def from_repr(lst_dict: list) -> List[Holiday]:
        out = []
        for d in lst_dict:
            out.append(Holiday(
                d["holiday"],
                d["start"],
                d["end"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[Holiday]) -> list:
        out = []
        for instance in lst:
            out.append({
                "holiday": instance.holiday,
                "start": instance.start,
                "end": instance.end
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[Holiday]:
        return Holiday.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[Holiday]) -> pd.DataFrame:
        return pd.DataFrame(Holiday.to_repr(lst))


class WeatherStation:
    def __init__(self, station_id: str, name: str, lat: float, lon: float, elev: int, mun_code: int, n_sensors: int,
                 precipitation_available: bool, temperature_available: bool, humidity_available: bool,
                 wind_speed_available: bool, atmospheric_pressure_available: bool, solar_rad_available: bool):

        self.station_id = station_id  # pk
        self.name = name
        self.lat = lat
        self.lon = lon
        self.elev = elev
        self.mun_code = mun_code
        self.n_sensors = n_sensors
        self.precipitation_available = precipitation_available
        self.temperature_available = temperature_available
        self.humidity_available = humidity_available
        self.wind_speed_available = wind_speed_available
        self.atmospheric_pressure_available = atmospheric_pressure_available
        self.solar_rad_available = solar_rad_available

    @staticmethod
    def from_repr(lst_dict: list) -> List[WeatherStation]:
        out = []
        for d in lst_dict:
            out.append(WeatherStation(
                d["station_id"],
                d["name"],
                d["lat"],
                d["lon"],
                int(d["elev"]),
                int(d["mun_code"]),
                int(d["n_sensors"]),
                bool(d["precipitation_available"]),
                bool(d["temperature_available"]),
                bool(d["humidity_available"]),
                bool(d["wind_speed_available"]),
                bool(d["atmospheric_pressure_available"]),
                bool(d["solar_rad_available"])
            ))
        return out

    @staticmethod
    def to_repr(lst: List[WeatherStation]) -> list:
        out = []
        for instance in lst:
            out.append({
                "station_id": instance.station_id,
                "name": instance.name,
                "lat": instance.lat,
                "lon": instance.lon,
                "elev": instance.elev,
                "mun_code": instance.mun_code,
                "n_sensors": instance.n_sensors,
                "precipitation_available": instance.precipitation_available,
                "temperature_available": instance.temperature_available,
                "humidity_available": instance.humidity_available,
                "wind_speed_available": instance.wind_speed_available,
                "atmospheric_pressure_available": instance.atmospheric_pressure_available,
                "solar_rad_available": instance.solar_rad_available
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[WeatherStation]:
        return WeatherStation.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[WeatherStation]) -> pd.DataFrame:
        return pd.DataFrame(WeatherStation.to_repr(lst))


class MunicipalityWeatherStationLink:

    def __init__(self, municipality_code, station_id):
        self.municipality_code = municipality_code  # fk references Municipality(code)
        self.station_id = station_id  # fk references WeatherStation(station_id)

    @staticmethod
    def from_repr(lst_dict: list) -> List[MunicipalityWeatherStationLink]:
        out = []
        for d in lst_dict:
            out.append(MunicipalityWeatherStationLink(
                d["municipality_code"],
                d["station_id"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[MunicipalityWeatherStationLink]) -> list:
        out = []
        for instance in lst:
            out.append({
                "municipality_code": instance.municipality_code,
                "station_id": instance.station_id
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[MunicipalityWeatherStationLink]:
        return MunicipalityWeatherStationLink.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[MunicipalityWeatherStationLink]) -> pd.DataFrame:
        return pd.DataFrame(MunicipalityWeatherStationLink.to_repr(lst))


class WeatherData:

    # These are daily measurements for each municipality.
    # Could be chosen from a single station or computed from different stations

    def __init__(self, station_id: int, date: pd.Timestamp, precipitation_total: float, temperature_mean: float,
                 humidity_mean: float, wind_speed_mean: float, atmospheric_pressure_mean: float,
                 solar_rad_total: float):
        self.station_id = station_id  # fk references WeatherStation(station_id)
        self.date = date
        self.precipitation_total = precipitation_total  # units: mm
        self.temperature_mean = temperature_mean  # units: °C
        self.humidity_mean = humidity_mean  # units: %
        self.wind_speed_mean = wind_speed_mean  # units: m/s
        self.atmospheric_pressure_mean = atmospheric_pressure_mean  # units: hPa
        self.solar_rad_total = solar_rad_total  # solar radiation, units: kJ/m^2

    @staticmethod
    def from_repr(lst_dict: list) -> List[WeatherData]:
        out = []
        for d in lst_dict:
            out.append(WeatherData(
                d["station_id"],
                d["date"],
                d["precipitation_total_mm"] if not isnan(d["precipitation_total_mm"]) else None,
                d["temperature_mean_c"] if not isnan(d["temperature_mean_c"]) else None,
                d["humidity_mean_percent"] if not isnan(d["humidity_mean_percent"]) else None,
                d["wind_speed_mean_ms"] if not isnan(d["wind_speed_mean_ms"]) else None,
                d["atmospheric_pressure_mean_hpa"] if not isnan(d["atmospheric_pressure_mean_hpa"]) else None,
                d["solar_rad_total_kjm2"] if not isnan(d["solar_rad_total_kjm2"]) else None
            ))
        return out

    @staticmethod
    def to_repr(lst: List[WeatherData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "station_id": instance.station_id,
                "date": instance.date,
                "precipitation_total_mm": instance.precipitation_total,
                "temperature_mean_c": instance.temperature_mean,
                "humidity_mean_percent": instance.humidity_mean,
                "wind_speed_mean_ms": instance.wind_speed_mean,
                "atmospheric_pressure_mean_hpa": instance.atmospheric_pressure_mean,
                "solar_rad_total_kjm2": instance.solar_rad_total
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[WeatherData]:
        return WeatherData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[WeatherData]) -> pd.DataFrame:
        return pd.DataFrame(WeatherData.to_repr(lst))


class VaccinesAdministrationData:

    def __init__(self, date: pd.Timestamp, region: str, supplier: str, age_group: str, male: int,
                 female: int, first_dose: int, second_dose: int):
        self.date = date
        self.region = region  # fk references Province(name)
        self.supplier = supplier
        self.age_group = age_group
        self.male = male
        self.female = female
        self.first_dose = first_dose
        self.second_dose = second_dose

    @staticmethod
    def from_repr(lst_dict: list) -> List[VaccinesAdministrationData]:
        out = []
        for d in lst_dict:
            out.append(VaccinesAdministrationData(
                d["administration_date"],
                d["region"],
                d["supplier"],
                d["age_group"],
                d["male"],
                d["female"],
                d["first_dose"],
                d["second_dose"],
            ))
        return out

    @staticmethod
    def to_repr(lst: List[VaccinesAdministrationData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "administration_date": instance.date,
                "region": instance.region,
                "supplier": instance.supplier,
                "age_group": instance.age_group,
                "male": instance.male,
                "female": instance.female,
                "first_dose": instance.first_dose,
                "second_dose": instance.second_dose
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[VaccinesAdministrationData]:
        return VaccinesAdministrationData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[VaccinesAdministrationData]) -> pd.DataFrame:
        return pd.DataFrame(VaccinesAdministrationData.to_repr(lst))


class VaccinesDeliveryData:

    def __init__(self, date: pd.Timestamp, region: str, supplier: str, n_doses: int):
        self.date = date
        self.region = region  # fk references Province(name)
        self.supplier = supplier
        self.n_doses = n_doses

    @staticmethod
    def from_repr(lst_dict: list) -> List[VaccinesDeliveryData]:
        out = []
        for d in lst_dict:
            out.append(VaccinesDeliveryData(
                d["delivery_date"],
                d["region"],
                d["supplier"],
                d["n_doses"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[VaccinesDeliveryData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "delivery_date": instance.date,
                "region": instance.region,
                "supplier": instance.supplier,
                "n_doses": instance.n_doses
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[VaccinesDeliveryData]:
        return VaccinesDeliveryData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[VaccinesDeliveryData]) -> pd.DataFrame:
        return pd.DataFrame(VaccinesDeliveryData.to_repr(lst))


class StringencyIndex:

    def __init__(self, date: pd.Timestamp, value: float):
        self.date = date
        self.value = value

    @staticmethod
    def from_repr(lst_dict: list) -> List[StringencyIndex]:
        out = []
        for d in lst_dict:
            out.append(StringencyIndex(
                d["date"],
                d["value"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[StringencyIndex]) -> list:
        out = []
        for instance in lst:
            out.append({
                "date": instance.date,
                "value": instance.value
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[StringencyIndex]:
        return StringencyIndex.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[StringencyIndex]) -> pd.DataFrame:
        return pd.DataFrame(StringencyIndex.to_repr(lst))


class CuratedData:

    def __init__(self, date: pd.Timestamp, cases: int, new_cases: int, deaths: int, new_deaths: int, active: int,
                 recovered: int, quarantined: int, hospitalized: int, hospitalized_high_intensity: int,
                 hospitalized_intensive_care: int, discharged: int, active_rsa_total: int, holiday: bool, risk: str,
                 stringency_index: float, new_first_doses_ag0: int, new_first_doses_ag1: int, new_first_doses_ag2: int,
                 new_second_doses_ag0: int, new_second_doses_ag1: int, new_second_doses_ag2: int, new_first_doses: int,
                 new_second_doses: int, first_doses_ag0: int, first_doses_ag1: int, first_doses_ag2: int,
                 second_doses_ag0: int, second_doses_ag1: int, second_doses_ag2: int, first_doses: int,
                 second_doses: int, vaccinated_population: float, fully_vaccinated_population: float):
        self.date = date
        self.cases = cases
        self.new_cases = new_cases
        self.deaths = deaths
        self.new_deaths = new_deaths
        self.active = active
        self.recovered = recovered
        self.quarantined = quarantined
        self.hospitalized = hospitalized
        self.hospitalized_high_intensity = hospitalized_high_intensity
        self.hospitalized_intensive_care = hospitalized_intensive_care
        self.discharged = discharged
        self.active_rsa_total = active_rsa_total
        self.holiday = holiday
        self.risk = risk
        self.stringency_index = stringency_index
        self.new_first_doses_ag0 = new_first_doses_ag0
        self.new_first_doses_ag1 = new_first_doses_ag1
        self.new_first_doses_ag2 = new_first_doses_ag2
        self.new_second_doses_ag0 = new_second_doses_ag0
        self.new_second_doses_ag1 = new_second_doses_ag1
        self.new_second_doses_ag2 = new_second_doses_ag2
        self.new_first_doses = new_first_doses
        self.new_second_doses = new_second_doses
        self.first_doses_ag0 = first_doses_ag0
        self.first_doses_ag1 = first_doses_ag1
        self.first_doses_ag2 = first_doses_ag2
        self.second_doses_ag0 = second_doses_ag0
        self.second_doses_ag1 = second_doses_ag1
        self.second_doses_ag2 = second_doses_ag2
        self.first_doses = first_doses
        self.second_doses = second_doses
        self.vaccinated_population = vaccinated_population
        self.fully_vaccinated_population = fully_vaccinated_population

    @staticmethod
    def from_repr(lst_dict: list) -> List[CuratedData]:
        out = []
        for d in lst_dict:
            out.append(CuratedData(
                d["date"],
                d["cases"],
                d["new_cases"],
                d["deaths"],
                d["new_deaths"],
                d["active"],
                d["recovered"],
                d["quarantined"],
                d["hospitalized"],
                d["hospitalized_high_intensity"],
                d["hospitalized_intensive_care"],
                d["discharged"],
                d["active_rsa_total"],
                d["holiday"],
                d["risk"],
                d["stringency_index"],
                d["new_first_doses_ag0"],
                d["new_first_doses_ag1"],
                d["new_first_doses_ag2"],
                d["new_second_doses_ag0"],
                d["new_second_doses_ag1"],
                d["new_second_doses_ag2"],
                d["new_first_doses"],
                d["new_second_doses"],
                d["first_doses_ag0"],
                d["first_doses_ag1"],
                d["first_doses_ag2"],
                d["second_doses_ag0"],
                d["second_doses_ag1"],
                d["second_doses_ag2"],
                d["first_doses"],
                d["second_doses"],
                d["vaccinated_population"],
                d["fully_vaccinated_population"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[CuratedData]) -> list:
        out = []
        for instance in lst:
            out.append({
                "date": instance.date,
                "cases": instance.cases,
                "new_cases": instance.new_cases,
                "deaths": instance.deaths,
                "new_deaths": instance.new_deaths,
                "active": instance.active,
                "recovered": instance.recovered,
                "quarantined": instance.quarantined,
                "hospitalized": instance.hospitalized,
                "hospitalized_high_intensity": instance.hospitalized_high_intensity,
                "hospitalized_intensive_care": instance.hospitalized_intensive_care,
                "discharged": instance.discharged,
                "active_rsa_total": instance.active_rsa_total,
                "holiday": instance.holiday,
                "risk": instance.risk,
                "stringency_index": instance.stringency_index,
                "new_first_doses_ag0": instance.new_first_doses_ag0,
                "new_first_doses_ag1": instance.new_first_doses_ag1,
                "new_first_doses_ag2": instance.new_first_doses_ag2,
                "new_second_doses_ag0": instance.new_second_doses_ag0,
                "new_second_doses_ag1": instance.new_second_doses_ag1,
                "new_second_doses_ag2": instance.new_second_doses_ag2,
                "new_first_doses": instance.new_first_doses,
                "new_second_doses": instance.new_second_doses,
                "first_doses_ag0": instance.first_doses_ag0,
                "first_doses_ag1": instance.first_doses_ag1,
                "first_doses_ag2": instance.first_doses_ag2,
                "second_doses_ag0": instance.second_doses_ag0,
                "second_doses_ag1": instance.second_doses_ag1,
                "second_doses_ag2": instance.second_doses_ag2,
                "first_doses": instance.first_doses,
                "second_doses": instance.second_doses,
                "vaccinated_population": instance.vaccinated_population,
                "fully_vaccinated_population": instance.fully_vaccinated_population
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[CuratedData]:
        return CuratedData.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[CuratedData]) -> pd.DataFrame:
        return pd.DataFrame(CuratedData.to_repr(lst))


class HyperparameterTuningResult:

    def __init__(self, log: dict, configuration: dict, results: list):
        self.log = log
        self.configuration = configuration
        self.results = results

    @staticmethod
    def from_repr(lst_dict: list) -> List[HyperparameterTuningResult]:
        out = []
        for d in lst_dict:
            out.append(HyperparameterTuningResult(
                d["log"],
                d["configuration"],
                d["results"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[HyperparameterTuningResult]) -> list:
        out = []
        for instance in lst:
            out.append({
                "log": instance.log,
                "configuration": instance.configuration,
                "results": instance.results
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[HyperparameterTuningResult]:
        return HyperparameterTuningResult.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[HyperparameterTuningResult]) -> pd.DataFrame:
        return pd.DataFrame(HyperparameterTuningResult.to_repr(lst))


class Forecast:

    def __init__(self, output_variable: str, date: pd.Timestamp, forecast: float, se: float, upper_ci: float, lower_ci: float):
        self.output_variable = output_variable
        self.date = date
        self.forecast = forecast
        self.se = se
        self.upper_ci = upper_ci
        self.lower_ci = lower_ci

    @staticmethod
    def from_repr(lst_dict: list) -> List[Forecast]:
        out = []
        for d in lst_dict:
            out.append(Forecast(
                d["output_variable"],
                d["date"],
                d["forecast"],
                d["se"],
                d["upper_ci"],
                d["lower_ci"]
            ))
        return out

    @staticmethod
    def to_repr(lst: List[Forecast]) -> list:
        out = []
        for instance in lst:
            out.append({
                "output_variable": instance.output_variable,
                "date": instance.date,
                "forecast": instance.forecast,
                "se": instance.se,
                "upper_ci": instance.upper_ci,
                "lower_ci": instance.lower_ci
            })
        return out

    @staticmethod
    def from_df(df: pd.DataFrame) -> List[Forecast]:
        return Forecast.from_repr(df.to_dict(orient="records"))

    @staticmethod
    def to_df(lst: List[Forecast]) -> pd.DataFrame:
        return pd.DataFrame(Forecast.to_repr(lst))
