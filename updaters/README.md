# Updaters

Updaters are pipelines in charge of searching new data from external resources, i.e. they call each entity's collector
but also take care of understanding what data are missing and then handle the insertion of the new records into the
database.

There is one updater for each entity, following a naming convention similar to that of collectors: `ProvinceData`
-> `ProvinceDataUpdater`.

An updater performs the following actions:

- initialises the DAO and the Collector of the corresponding entity
- uses the DAO to find what is the date of the most recent record for that entity in the database; only the data that
  are more recent than those already existing in the database will be searched
- calls the search method of the collector, passing the above date increased by one day as the lower range for the
  search
- uses the DAO to insert the data returned by the collector into the MySQL database

All updaters except for `WeatherDataUpdater` have a similar structure and can be found in the module `data_updaters.py`.

WeatherDataUpdater has a slightly more complex structure and its class can be found in a separate module. Its
functioning is described below.

### Weather Data Updater

This pipeline functions as follows:

- initialises the DAO for both `WeatherStation` and `WeatherData`, and the colletor of the latter entity
- the DAO of WeatherStation is used to retrieve the list of weather stations that are currently linked to
  municipalities: data from stations that are unused will not be collected
- for each station, the DAO of WeatherData is used to find the date of the most recent record for that station
- for each station, the collector of WeatherData is used to collect new records
- the DAO of WeatherData is used to save the new records into the MySQL database

## Running data updates

Updaters can be run sequentially or in parallel. Their work is currently coordinated by the
pipeline `pipelines/data_update.py`, which runs each updater sequentially.