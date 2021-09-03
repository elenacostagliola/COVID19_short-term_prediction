from .weather_data_updater import WeatherDataUpdater
from .data_updaters import MunicipalityDataUpdater, ProvinceDataUpdater, VaccinesAdministrationDataUpdater, \
    VaccinesDeliveryDataUpdater, HolidayUpdater, RegionRiskUpdater, StringencyIndexUpdater

avail_updaters = {
        "MunicipalityData": MunicipalityDataUpdater(), # fix NaT in MunicipalityDataUpdater (issue #19)
        "ProvinceData": ProvinceDataUpdater(),
        "Holiday": HolidayUpdater(),
        "RegionRisk": RegionRiskUpdater(),
        "VaccinesAdministrationData": VaccinesAdministrationDataUpdater(),
        "VaccinesDeliveryData": VaccinesDeliveryDataUpdater(),
        "StringencyIndex": StringencyIndexUpdater(),
        "WeatherData": WeatherDataUpdater()
    }