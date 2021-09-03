from pandas import Timedelta

from collectors import *
from data.dao import *


class DataUpdater:

    def __init__(self, dao, collector):
        self.dao = dao
        self.collector = collector

    def run(self) -> int:
        # Check date of most recent data
        latest_date = self.dao.get_most_recent_timestamp()
        latest_date, _ = validate_dates(latest_date, None)

        # Collect new data from day after latest_date
        data = self.collector.search(date_from=latest_date + Timedelta(days=1))

        # Insert new data
        if len(data) > 0:
            self.dao.save(data)

        # Return the number of processed records
        return len(data)


class MunicipalityDataUpdater(DataUpdater):
    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = MunicipalityDataMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = MunicipalityDataCollector()
        super().__init__(dao, collector)


class ProvinceDataUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = ProvinceDataMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = ProvinceDataCollector()
        super().__init__(dao, collector)


class VaccinesDeliveryDataUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = VaccinesDeliveryDataMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = VaccinesDeliveryDataCollector()
        super().__init__(dao, collector)


class VaccinesAdministrationDataUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = VaccinesAdministrationDataMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = VaccinesAdministrationDataCollector()
        super().__init__(dao, collector)


class HolidayUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = HolidayMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = HolidayCollector()
        super().__init__(dao, collector)


class RegionRiskUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = RegionRiskMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = RegionRiskCollector()
        super().__init__(dao, collector)


class StringencyIndexUpdater(DataUpdater):

    def __init__(self, storage="default"):
        dao = None
        assert storage in ALLOWED_STORAGE, "Unrecognised storage option"
        if storage == "default":
            dao = StringencyIndexMySqlDao()
        assert dao is not None, "DAO has not been instantiated"
        collector = StringencyIndexCollector()
        super().__init__(dao, collector)
