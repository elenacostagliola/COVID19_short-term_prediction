import sys, os
from pandas import Timestamp

sys.path.insert(0,
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(__file__))))

from collectors import *
from pipelines import weather_setup
from data.dao import *
from data.mysql_wrapper import MySqlDB


def collect_and_save(collector, dao) -> int:
    data = collector.search()
    dao.save(data)
    return len(data)


def main():
    ti = Timestamp("now")

    # Creates all tables in the db
    db = MySqlDB()
    with db.connect():
        print("Creating tables")
        db.create_default_tables()

    # Runs all static data collector and stores data via DAOs
    entities = {"Province": [ProvinceCollector(), ProvinceMySqlDao()],
                  "Municipality": [MunicipalityCollector(), MunicipalityMySqlDao()]}
    success = {}

    for entity in entities:
        tci = Timestamp("now")
        print("Collecting and storing data for entity " + entity)
        n_records = collect_and_save(entities[entity][0], entities[entity][1])
        tcf = Timestamp("now")
        success[entity] = [n_records, tcf - tci]

    # Run weather setup
    tci = Timestamp("now")
    print("Running weather setup script (WeatherStation, WeatherStationLink)")
    n_stations, n_links = weather_setup.main()
    tcf = Timestamp("now")
    success["WeatherSetup"] = [f"{n_stations} stations, {n_links} links", tcf - tci]

    tf = Timestamp("now")
    print("Done.")
    print("Total elapsed time: " + str(tf-ti))
    print("Execution report:")
    for s in success:
        print(f" - {s}: {success[s][0]} records in {success[s][1]}")


if __name__ == '__main__':
    main()
