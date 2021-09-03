import sys, os

ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from collectors import WeatherStationCollector
from pipelines import MunicipalityWeatherStationLinker
from data import dao


def main(storage="default", _test=False):
    # Move to root dir in order for WeatherCollector to see the geopackage
    os.chdir(ROOT_DIR)

    # Instantiate DAOs according to storage option
    mundao = None
    wsdao = None
    wslinkdao = None
    assert storage in dao.ALLOWED_STORAGE, "Unrecognised storage option"
    if storage == "default":
        mundao = dao.MunicipalityMySqlDao()
        wsdao = dao.WeatherStationMySqlDao()
        wslinkdao = dao.MunicipalityWeatherStationLinkMySqlDao()
    assert wsdao is not None and wslinkdao is not None, "DAO has not been instantiated"

    # Get existing municipalities
    print("Reading municipalities")
    municipalities = mundao.read_all()

    # Get WeatherStations
    print("Collecting weather stations")
    wsc = WeatherStationCollector(_test=_test)
    ws = wsc.search(municipalities)

    # Write WeatherStation list to DB with WeatherStationDao
    print("Saving weather stations")
    wsdao.save(ws)

    # Call MunicipalityWeatherStationLinker passing List[WeatherStation]
    print("Linking weather stations to municipalities")
    mslinker = MunicipalityWeatherStationLinker()
    wslinks = mslinker.run(ws)

    # Write MWSLink list to DB with MWSLDao
    print("Saving links")
    wslinkdao.save(wslinks)

    return len(ws), len(wslinks)


if __name__ == '__main__':
    main()
