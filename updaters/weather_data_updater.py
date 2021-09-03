import pandas as pd

from data import dao
from collectors import WeatherDataCollector


class WeatherDataUpdater:

    def __init__(self, storage="default"):
        self.wsdao = None
        self.wdatadao = None
        assert storage in dao.ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            self.wsdao = dao.WeatherStationMySqlDao()
            self.wdatadao = dao.WeatherDataMySqlDao()
        assert self.wsdao is not None and self.wdatadao is not None, "DAO has not been instantiated"

    def run(self):
        # Get List[WeatherStation] of stations to be processed (custom query via WeatherStationDao) --> checks Links
        ws = self.wsdao.read_linked_stations()

        # For each station, find most recent date in the db (custom query via WeatherDataDao) --> returns Date
        stations_dates = {}
        for station in ws:
            stations_dates[station.station_id] = self.wdatadao.get_most_recent_timestamp(station.station_id)

        # For each station, call WeatherDataCollector.search(...) with that station and latest date + 1 (search
        # from day after)
        wdc = WeatherDataCollector()
        n_records = 0
        i = 1
        for station in stations_dates:
            print(f"\rFetching data for station {i} of {len(stations_dates)}", end="")

            data = wdc.search(station,
                              date_from=stations_dates[station] + pd.Timedelta(days=1) if stations_dates[
                                                                                              station] is not None else None)

            # Write List[WeatherData] to DB with WeatherDataDao
            self.wdatadao.save(data)
            n_records += len(data)

            i += 1

        print()

        return n_records


if __name__ == '__main__':
    updater = WeatherDataUpdater()
    updater.run()
