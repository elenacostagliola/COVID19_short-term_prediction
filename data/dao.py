import numpy as np
from pandas import Timestamp
from abc import ABC, abstractmethod

from collectors.validation_utils import validate_dates
from configuration import dbconfig
from .models import *
from .mysql_wrapper import MySqlDB
from .mongo_wrapper import MongoDB

ALLOWED_STORAGE = ["default"]


class ProvinceDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, prov: List[Province]):
        pass

    @abstractmethod
    def get_population(self, name: str):
        pass


class ProvinceMySqlDao(ProvinceDao):

    def save(self, prov: List[Province]):
        assert isinstance(prov, list)
        if len(prov) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into Province (name, code, population) values (%s, %s, %s)"
                values = [(p.name, p.code, p.population) for p in prov]
                if len(prov) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(prov) > 1)
                assert inserted == len(prov), 'MySql insertion error: inserted less entities than provided'

    def get_population(self, name: str) -> int:
        db = MySqlDB()
        sql = f'select population from Province where name = "{name}"'
        with db.connect():
            result = db.read(sql)
        return result[0][0] if len(result) > 0 else None


class ProvinceDataDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: ProvinceData or List[ProvinceData]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[ProvinceData]:
        # Read in date range
        pass


class ProvinceDataMySqlDao(ProvinceDataDao):

    def save(self, data: ProvinceData or List[ProvinceData]):
        if isinstance(data, ProvinceData):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into ProvinceData (date, name, cases, new_cases, active, recovered, deaths, quarantined, " \
                      + "hospitalized_infectious_diseases, hospitalized_high_intensity, hospitalized_intensive_care, " \
                      + "discharged,  active_rsa, active_nursing_homes, active_int_struct, active_rsa_total) values " \
                      + f"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = [(p.date.to_pydatetime(), p.name, p.cases, p.new_cases, p.active, p.recovered, p.deaths,
                           p.quarantined, p.hospitalized_infectious_diseases, p.hospitalized_high_intensity,
                           p.hospitalized_intensive_care, p.discharged, p.active_rsa,
                           p.active_nursing_homes, p.active_int_struct, p.active_rsa_total) for p in
                          data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # In principle, the query returns [name, date], which is transformed into a dictionary {name: date}
        # in order to handle different latest updates in case of multiple provinces available.
        # Here, only the Timestamp for "PAT" is returned.

        # If there are no records in the db, return None (pass to updaters to collect data from the beginning
        # of the epidemic.

        db = MySqlDB()
        sql = "select name, max(date) from ProvinceData group by name"
        with db.connect():
            result = db.read(sql)
        response = dict((t[0], Timestamp(t[1])) for t in result)
        return response["PAT"] if "PAT" in response else None

    def get_by_date(self, date_from=None, date_to=None) -> List[ProvinceData]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from ProvinceData where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [ProvinceData(i[0], i[1], i[2],
                             i[3], i[4], i[5],
                             i[6], i[7], i[8],
                             i[9], i[10], i[11],
                             i[12], i[13], i[14], i[15]) for i in result]


class MunicipalityDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, mun: List[Municipality]):
        pass

    @abstractmethod
    def read_all(self) -> List[Municipality]:
        pass


class MunicipalityMySqlDao(MunicipalityDao):

    def save(self, mun: List[Municipality]):
        if len(mun) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into Municipality (code, name, province, lat, lon, population) values (%s, %s, %s, %s, %s, %s)"
                values = [(m.code, m.name, m.province, m.lat, m.lon, m.population) for m in mun]
                if len(mun) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(mun) > 1)
                assert inserted == len(mun), 'MySql insertion error: inserted less entities than provided'

    def read_all(self) -> List[Municipality]:
        db = MySqlDB()
        sql = 'select * from Municipality'
        with db.connect():
            result = db.read(sql)
        return [Municipality(i[0], i[1], i[2],
                             i[3], i[4], i[5]) for i in result]


class MunicipalityDataDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: MunicipalityData or List[MunicipalityData]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[MunicipalityData]:
        # Read in date range
        pass


class MunicipalityDataMySqlDao(MunicipalityDataDao):

    def save(self, data: MunicipalityData or List[MunicipalityData]):
        if isinstance(data, MunicipalityData):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into MunicipalityData (date, code, cases, recovered, deaths, discharged) values " \
                      + f"(%s, %s, %s, %s, %s, %s)"
                values = [(m.date.to_pydatetime(), m.code, m.cases, m.recovered, m.deaths, m.discharged) for m in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # In principle, the query returns [code, date], which is transformed into a dictionary {code: date}
        # in order to handle different latest updates in case of multiple municipalities available.
        # Here, a single timestamp is returned.

        # If there are no records in the db, return None (pass to updaters to collect data from the beginning
        # of the epidemic.

        db = MySqlDB()
        sql = "select code, max(date) from MunicipalityData group by code"
        with db.connect():
            result = db.read(sql)
        response = dict((t[0], Timestamp(t[1])) for t in result)
        return list(result.values())[0] if len(result) > 0 else None

    def get_by_date(self, date_from=None, date_to=None) -> List[MunicipalityData]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from MunicipalityData where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [MunicipalityData(i[0], i[1], i[2],
                                 i[3], i[4], i[5]) for i in result]


class RegionRiskDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: RegionRisk or List[RegionRisk]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[RegionRisk]:
        # Read in date range
        pass


class RegionRiskMySqlDao(RegionRiskDao):

    def save(self, data: RegionRisk or List[RegionRisk]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        if isinstance(data, RegionRisk):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into RegionRisk (date, region, risk) values (%s, %s, %s)"
                values = [(d.date.to_pydatetime(), d.region, d.risk) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # In principle, the query returns [name, date], which is transformed into a dictionary {name: date}
        # in order to handle different latest updates in case of multiple provinces available.
        # Here, only the Timestamp for "PAT" is returned.

        # If there are no records in the db, return None (pass to updaters to collect data from the beginning
        # of the epidemic.

        db = MySqlDB()
        sql = "select region, max(date) from RegionRisk group by region"
        with db.connect():
            result = db.read(sql)
        response = dict((t[0], Timestamp(t[1])) for t in result)
        return response["PAT"] if "PAT" in response else None

    def get_by_date(self, date_from=None, date_to=None) -> List[RegionRisk]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from RegionRisk where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [RegionRisk(i[0], i[1], i[2]) for i in result]


class HolidayDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: Holiday or List[Holiday]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[Holiday]:
        # Read in date range
        pass


class HolidayMySqlDao(HolidayDao):

    def save(self, data: Holiday or List[Holiday]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        if isinstance(data, Holiday):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into Holiday (holiday, start, end) values (%s, %s, %s)"
                values = [(d.holiday, d.start.to_pydatetime(), d.end.to_pydatetime()) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        db = MySqlDB()
        sql = "select max(start) from Holiday"
        with db.connect():
            result = db.read(sql)
        return Timestamp(result[0][0]) if result[0][0] is not None else None

    def get_by_date(self, date_from=None, date_to=None) -> List[Holiday]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from Holiday where start between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [Holiday(i[0], i[1], i[2]) for i in result]


class VaccinesDeliveryDataDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: VaccinesDeliveryData or List[VaccinesDeliveryData]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[VaccinesDeliveryData]:
        # Read in date range
        pass


class VaccinesDeliveryDataMySqlDao(VaccinesDeliveryDataDao):

    def save(self, data: VaccinesDeliveryData or List[VaccinesDeliveryData]):
        if isinstance(data, VaccinesDeliveryData):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into VaccinesDeliveryData (date, region, supplier, n_doses) values (%s, %s, %s, %s)"
                values = [(d.date.to_pydatetime(), d.region, d.supplier, d.n_doses) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:

        # In principle, the query returns [region, date], which is transformed into a dictionary {name: date}
        # in order to handle different latest updates in case of multiple provinces available.
        # Here, only the Timestamp for "PAT" is returned.

        # If there are no records in the db, return None (pass to updaters to collect data from the beginning
        # of the epidemic.

        db = MySqlDB()
        sql = "select region, max(date) from VaccinesDeliveryData group by region"
        with db.connect():
            result = db.read(sql)
        response = dict((t[0], Timestamp(t[1])) for t in result)
        return response["PAT"] if "PAT" in response else None

    def get_by_date(self, date_from=None, date_to=None) -> List[VaccinesDeliveryData]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from VaccinesDeliveryData where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [VaccinesDeliveryData(i[0], i[1], i[2], i[3]) for i in result]


class VaccinesAdministrationDataDao(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: VaccinesAdministrationData or List[VaccinesAdministrationData]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[VaccinesAdministrationData]:
        # Read in date range
        pass


class VaccinesAdministrationDataMySqlDao(VaccinesAdministrationDataDao):

    def save(self, data: VaccinesAdministrationData or List[VaccinesAdministrationData]):
        if isinstance(data, VaccinesAdministrationData):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into VaccinesAdministrationData (date, region, supplier, age_group, male,  female, " \
                      + "first_dose, second_dose) values (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = [(d.date.to_pydatetime(), d.region, d.supplier, d.age_group, d.male, d.female, d.first_dose,
                           d.second_dose) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # In principle, the query returns [region, date], which is transformed into a dictionary {name: date}
        # in order to handle different latest updates in case of multiple provinces available.
        # Here, only the Timestamp for "PAT" is returned.

        # If there are no records in the db, return None (pass to updaters to collect data from the beginning
        # of the epidemic.

        db = MySqlDB()
        sql = "select region, max(date) from VaccinesAdministrationData group by region"
        with db.connect():
            result = db.read(sql)
        response = dict((t[0], Timestamp(t[1])) for t in result)
        return response["PAT"] if "PAT" in response else None

    def get_by_date(self, date_from=None, date_to=None) -> List[VaccinesAdministrationData]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from VaccinesAdministrationData where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [VaccinesAdministrationData(i[0], i[1], i[2], i[3], i[4],
                                           i[5], i[6], i[7]) for i in result]


class WeatherStationDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, s: WeatherStation or List[WeatherStation]):
        # Inserts station or list of stations
        pass

    @abstractmethod
    def read_linked_stations(self) -> List[WeatherStation]:
        # Return a list of stations currently linked to municipalities
        # Actually joins WeatherStation and MunicipalityWeatherStationLink
        pass


class WeatherStationMySqlDao(WeatherStationDao):
    def save(self, s: WeatherStation or List[WeatherStation]):
        if isinstance(s, WeatherStation):
            s = [s]
        assert isinstance(s, list)
        if len(s) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into WeatherStation (station_id, name, lat, lon, elev, n_sensors, " \
                      + "precipitation_available, temperature_available, humidity_available, " \
                      + "wind_speed_available, atmospheric_pressure_available, solar_rad_available) " \
                      + "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = [(d.station_id, d.name, d.lat, d.lon, d.elev, d.n_sensors,
                           d.precipitation_available, d.temperature_available, d.humidity_available,
                           d.wind_speed_available, d.atmospheric_pressure_available, d.solar_rad_available) for d in s]
                if len(s) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(s) > 1)
                assert inserted == len(s), 'MySql insertion error: inserted less entities than provided'

    def read_linked_stations(self) -> List[WeatherStation]:
        # Return a list of stations currently linked to municipalities
        # Actually joins WeatherStation and MunicipalityWeatherStationLink

        db = MySqlDB()
        sql = "select * from WeatherStation WS inner join MunicipalityWeatherStationLink MWSL " \
              + "on WS.station_id = MWSL.station_id"
        with db.connect():
            result = db.read(sql)
        return [WeatherStation(i[0], i[1], i[2], i[3], i[4],
                               i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12]) for i in result]


class MunicipalityWeatherStationLinkDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, l: MunicipalityWeatherStationLink or List[MunicipalityWeatherStationLink]):
        # Insert link or list of links
        pass


class MunicipalityWeatherStationLinkMySqlDao(MunicipalityWeatherStationLinkDao):
    def save(self, l: MunicipalityWeatherStationLink or List[MunicipalityWeatherStationLink]):
        if isinstance(l, MunicipalityWeatherStationLink):
            l = [l]
        assert isinstance(l, list)
        if len(l) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into MunicipalityWeatherStationLink (municipality_code, station_id) " \
                      + "values (%s, %s)"
                values = [(d.municipality_code, d.station_id) for d in l]
                if len(l) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(l) > 1)
                assert inserted == len(l), 'MySql insertion error: inserted less entities than provided'


class WeatherDataDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, d: WeatherData or List[WeatherData]):
        # Inserts data
        pass

    @abstractmethod
    def get_most_recent_timestamp(self, station_id: str) -> Timestamp:
        # Find most recent timestamp of data for station identified by station_id
        pass

    @abstractmethod
    def get_average_values(self, date_from=None, date_to=None) -> List[WeatherData]:
        # Return average temperature from currently linked stations
        pass


class WeatherDataMySqlDao(WeatherDataDao):
    def save(self, data: WeatherData or List[WeatherData]):
        if isinstance(data, WeatherData):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into WeatherData (station_id, date, " \
                      + "precipitation_total, temperature_mean, humidity_mean, " \
                      + "wind_speed_mean, atmospheric_pressure_mean, solar_rad_total) " \
                      + "values (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = [(d.station_id, d.date.to_pydatetime(), d.precipitation_total,
                           d.temperature_mean, d.humidity_mean, d.wind_speed_mean,
                           d.atmospheric_pressure_mean, d.solar_rad_total) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self, station_id: str) -> Timestamp:
        db = MySqlDB()
        sql = f'select max(date) from WeatherData where station_id = "{station_id}"'
        with db.connect():
            result = db.read(sql)
        return Timestamp(result[0][0]) if result[0][0] is not None else None

    def get_average_values(self, date_from=None, date_to=None) -> List[WeatherData]:
        # Return average temperature from currently linked stations

        # issue #26

        # date_from, date_to = validate_dates(date_from, date_to)
        # str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        # str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        # db = MySqlDB()
        # sql = f'select date, avg(precipitation_total), avg(temperature_mean), avg(humidity_mean), avg(wind_speed_mean), ' \
        #       f'avg(atmospheric_pressure_mean), avg(solar_rad_total) ' \
        #       f'from WeatherData where station_id in (select station_id from MunicipalityWeatherStationLink) ' \
        #       f"and date between "
        # {str_date_from}
        # " and "
        # {str_date_to}
        # " group by date'
        # with db.connect():
        #     result = db.read(sql)
        # return [StringencyIndex(i[0], i[1]) for i in result]
        pass


class StringencyIndexDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: StringencyIndex or List[StringencyIndex]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_timestamp(self) -> pd.Timestamp:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[StringencyIndex]:
        # Read in date range
        pass


class StringencyIndexMySqlDao(StringencyIndexDao):

    def save(self, data: StringencyIndex or List[StringencyIndex]):
        # Insert data: single instance or list of instances
        # Inserts new data if not exists
        if isinstance(data, StringencyIndex):
            data = [data]
        assert isinstance(data, list)
        if len(data) > 0:
            db = MySqlDB()
            with db.connect():
                sql = "insert into StringencyIndex (date, value) values (%s, %s)"
                values = [(d.date.to_pydatetime(), d.value) for d in data]
                if len(data) == 1:
                    values = values[0]
                inserted = db.insert(sql=sql, values=values, many=len(data) > 1)
                assert inserted == len(data), 'MySql insertion error: inserted less entities than provided'

    def get_most_recent_timestamp(self) -> pd.Timestamp:
        db = MySqlDB()
        sql = "select max(date) from StringencyIndex"
        with db.connect():
            result = db.read(sql)
        return Timestamp(result[0][0]) if result[0][0] is not None else None

    def get_by_date(self, date_from=None, date_to=None) -> List[StringencyIndex]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        db = MySqlDB()
        sql = f'select * from StringencyIndex where date between "{str_date_from}" and "{str_date_to}"'
        with db.connect():
            result = db.read(sql)
        return [StringencyIndex(i[0], i[1]) for i in result]


class CuratedDataDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: List[CuratedData]):
        # Insert list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_record(self) -> CuratedData:
        # Return latest date (as pd.Timestamp)
        pass

    @abstractmethod
    def get_by_date(self, date_from=None, date_to=None) -> List[CuratedData]:
        # Read in date range
        pass

    @abstractmethod
    def clear(self):
        # Delete all records
        pass


class CuratedDataMongoDao(CuratedDataDao):

    def __init__(self):
        super().__init__()
        self.client = MongoDB()
        self.collection = "curated"

    def save(self, data: List[CuratedData]):
        assert isinstance(data, list)
        if len(data) > 0:
            status = self.client.insert(dbconfig.MONGODB_DEFAULT_DB, self.collection, CuratedData.to_repr(data))
            assert status["inserted"] == len(data), "Not all records have been inserted"

    def get_most_recent_record(self) -> List[CuratedData]:
        res = self.client.find(dbconfig.MONGODB_DEFAULT_DB, self.collection, {}, orderby=("date", -1), limit=10)
        if len(res) > 0:
            record = res[0]
            del record["_id"]
            data = CuratedData.from_repr([record])
            return data
        else:
            return []

    def get_by_date(self, date_from=None, date_to=None) -> List[CuratedData]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        records = self.client.find(dbconfig.MONGODB_DEFAULT_DB, self.collection,
                                   {"date": {"$gte": Timestamp(date_from), "$lte": Timestamp(date_to)}}, limit=None)
        data = CuratedData.from_repr(records)
        return data

    def clear(self):
        self.client.delete(dbconfig.MONGODB_DEFAULT_DB, self.collection, {}, delete_all=True)


class HyperparameterTuningResultDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: HyperparameterTuningResult or List[HyperparameterTuningResult]):
        # Insert list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_most_recent_record(self, output_variable: str) -> HyperparameterTuningResult:
        pass


class HyperparameterTuningResultMongoDao(HyperparameterTuningResultDao):

    def __init__(self):
        super().__init__()
        self.client = MongoDB()
        self.collection = "hyperparameters"

    def save(self, data: HyperparameterTuningResult or List[HyperparameterTuningResult]):
        if isinstance(data, HyperparameterTuningResult):
            data = [data]
        assert isinstance(data, list), "Input to save function is not a list"
        if len(data) > 0:
            status = self.client.insert(dbconfig.MONGODB_DEFAULT_DB, self.collection,
                                        HyperparameterTuningResult.to_repr(data))
            assert status["inserted"] == len(data), "Not all records have been inserted"

    def get_most_recent_record(self, output_variable: str, regressors: Optional[List] = []) -> List[
        HyperparameterTuningResult]:
        # Get all records for variable and with non-empty regressors list (i.e. exog model)
        res = self.client.find(dbconfig.MONGODB_DEFAULT_DB, self.collection,
                               {},
                               limit=None)

        if len(res) == 0:
            return []

        # Find results for variable
        filter1 = filter(lambda x: x["configuration"]["outputs"][0] == output_variable, res)

        # Filter exog model
        filter2 = filter(lambda x: set(x["configuration"]["regressors"]) == set(regressors), filter1)

        res = list(filter2)

        if len(res) == 0:
            return []

        # Find most recent
        res.sort(key=lambda x: x["log"]["timestamp"], reverse=True)
        record = res[0]

        data = HyperparameterTuningResult.from_repr([record])
        return data


class ForecastDao(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def save(self, data: List[Forecast]):
        # Insert list of instances
        # Inserts new data if not exists
        pass

    @abstractmethod
    def get_by_date(self, variable, date_from=None, date_to=None) -> List[CuratedData]:
        # Read in date range
        pass


class ForecastMongoDao(ForecastDao):

    def __init__(self):
        super().__init__()
        self.client = MongoDB()
        self.collection = "forecasts"

    def save(self, data: List[Forecast]):
        assert isinstance(data, list)
        out_variables = [d.output_variable for d in data]
        out_variables_unique = list(np.unique(out_variables))
        if len(data) > 0:
            for v in out_variables_unique:
                self.client.delete(dbconfig.MONGODB_DEFAULT_DB, self.collection, query={"output_variable": v},
                                   only_one=False)
            status = self.client.insert(dbconfig.MONGODB_DEFAULT_DB, self.collection, Forecast.to_repr(data))
            assert status["inserted"] == len(data), "Not all records have been inserted"

    def get_by_date(self, variable, date_from=None, date_to=None) -> List[Forecast]:
        date_from, date_to = validate_dates(date_from, date_to)
        str_date_from = Timestamp(date_from).strftime("%Y-%m-%d")
        str_date_to = Timestamp(date_to).strftime("%Y-%m-%d")
        records = self.client.find(dbconfig.MONGODB_DEFAULT_DB, self.collection,
                                   {"date": {"$gte": Timestamp(date_from), "$lte": Timestamp(date_to)},
                                    "output_variable": variable}, limit=None)
        data = Forecast.from_repr(records)
        return data
