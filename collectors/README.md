# Data collectors

## Introduction

Collectors are components that connect to external sources and collect data. Each data entity has its corresponding
collector. Collector names are the entity name (as defined in `data.models`) plus "Collector", e.g.
`StringencyIndex` -> `StringencyIndexCollector`.

## Static VS dynamic

We define two types of data sources and collectors: static and dynamic:

- **static** -> does not change on a daily basis; is not indexed by date; could be searched only once (e.g. a region's
  name, geographic coordinates and population - even though the population keeps changes, the datum is provided by the
  official statistics institute at most yearly)
- **dynamic** -> changes on a daily basis; it is indexed by time (time series); it must be searched on a daily basis (
  e.g. the count of new COVID-19 infections, which is updated daily).

## Implementation

Collectors are implemented as classes with some attributes and methods that are specific to the source they are
collecting data from.

All collectors expose a search method that starts the search process. Static collectors have a search method with no
arguments (as data are not indexed by time), whereas dynamic collectors require a date range.

Collectors return data in the form of list of instances of the class modelling the corresponding entity.

Here an example of the collection of static and dynamic data:

```python
from collectors import ProvinceCollector, ProvinceDataCollector

# Static data
collector_static = ProvinceCollector()
data_static = collector_static.search()

# Dynamic data
collector_dynamic = ProvinceDataCollector()
data_dynamic = collector_dynamic.search(date_from="2021-01", date_to="2021-04-02")
```

The keyword arguments `date_from` and `date_to` are strings that encode dates in the form "YYYY-mm-dd" and can be
limited to the month or year, as in the example. These arguments can also be omitted: if the lower bound is not
provided, a hard-coded date is used (that corresponds to the beginning of COVID-related data collection in Italy, i.e.
the beginning of February 2020); if the upper bound is not provided, it is considered to be the current day.

In this example, `data_static` is a list of objects `Province` from `data.models`, whereas `data_dynamic` is a list of
`ProvinceData`.

## Types of search processes

The functioning of the collectors can be divided into four groups depending on the search process.

### Github table

When data are provided on GitHub as tables to which the most recent record is appended, the GitHub API is used to
retrieve the whole table, which is then filtered by date in order to keep the desired records.

### Github table with daily records

When data are provided on GitHub as tables with the most recent records only, and those tables are overwritten daily
with new data, a single call to the GitHub API would retrieve records for a single day only. In this case, a more
complex mechanism is used in order to:

- get a list of valid commits in which the desired data table has been updated, limited to the range of dates of
  interest -> the GitHub API is called multiple times, navigating through multiple result pages, until all commits in
  the desired date range are found -> the result is a table that maps update dates to the GitHub commit ID in which the
  table has been updated
- for each date in the desired range, use the commit ID to retrieve the corresponding version of the data table
- combine all records.

### REST API

When data are provided through a REST API, a GET request to the properly formatted URL is performed. If a token is
required, it is pulled from the configuration package.

### Web scraping

If data need to be extracted from a web page, in which they are not necessarily in machine-readable form, web-scraping
is performed.

## Description of the available collectors

The following list describes the available collectors, the corresponding source and a general classification according
to the above definitions.

- `MunicipalityCollector`
    - collects information about the municipalities in Trentino, according to the data schema available at the
      documentation page for the `data` package; creates one instance of each municipality in the DB.
    - source: GitHub repository of the APSS COVID-19 Dashboard project by Maurizio Napolitano ([source](#sources) 1).
    - classification: static collector, GitHub table search process.
- `MunicipalityDataCollector`
    - collects COVID-related metrics at the level of single municipalities in Trentino.
    - source: [Covid-19 APSS Dashboard GitHub repository](https://github.com/napo/covid19apssdashboard/)
    - classification: dynamic collector, search process based on commits of daily-overwritten table.
- `ProvinceCollector`
    - collects information about Trentino, creating an instance for the province. Population is collected from the same
      source as MunicipalityCollector, by summing over each municipality's population. Municipalities in the relational
      DB have a foreign key that refers the entity collected by this collector. In principle, other collectors can be
      implemented to create instances of different provinces.
    - source: [Covid-19 APSS Dashboard GitHub repository](https://github.com/napo/covid19apssdashboard/).
    - classification: static collector, GitHub table search process.
- `ProvinceDataCollector`
    - collects COVID-related metrics for Trentino.
    - source: [Covid-19 APSS Dashboard GitHub repository](https://github.com/napo/covid19apssdashboard/).
    - classification: dynamic collector, GitHub table search process.
- `StringencyIndexCollector`
    - collects the "Stringency Index" computed by Oxford's Blavatnik School of Government
      project [COVID-19 Government Response Tracker](https://www.bsg.ox.ac.uk/research/research-projects/covid-19-government-response-tracker).
      . The index records the strictness of ‘lockdown style’ policies that primarily restrict people’s behaviour. It is
      calculated using all ordinal containment and closure policy indicators, plus an indicator recording public
      information campaigns (information on how the index is calculated can be
      found [here](https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/index_methodology.md)).
    - source: [COVID-19 Government Response Tracker API](https://covidtracker.bsg.ox.ac.uk/about-api).
    - classification: dynamic collector, uses a REST API.
- `RegionRiskCollector`
    - collects Italy's regional restrictions color code.
    - source: [Restrizioni regionali COVID-19 GitHub repository](https://github.com/imcatta/restrizioni_regionali_covid).
    - classification: dynamic collector, uses a Rest API.
- `HolidayCollector`
    - collects Italian holidays from Google Calendar API.
    - source: [Google Calendar Event list](https://developers.google.com/calendar/api/v3/reference/events/list).
- `VaccinesDeliveryDataCollector` and `VaccinesAdministrationDataCollector`
    - collects daily updated information about the vaccination campaign in Italy, i.e. statistics about vaccines
      delivery and the administration of doses to the population.
    - source: [Covid OpenData Vaccines](https://github.com/italia/covid19-opendata-vaccini).
    - classification: dynamic collectors, GitHub table search process.

### Weather pipeline

In order to collect data from weather stations in Trentino, two collectors are required:

- `WeatherStationCollector`
    - collects information about the available weather stations in Trentino, controlled by MeteoTrentino weather
      service. These data include the station's position (geographical coordinates) and number of available sensors;
      this information is needed to filter weather stations in a specific area, also according to the weather-related
      quantities that are needed. Geographical coordinates are used to detect the municipality in which they are
      installed, thanks to the spatial join operation performed through the library geopandas.
    - source: [Meteotrentino stations](https://content.meteotrentino.it/dati-meteo/stazioni/dati-Stazioni-Json.aspx).
    - classification: static collector, performs web scraping.
- `WeatherDataCollector`
    - collects daily averaged weather data from weather stations controlled by Meteotrentino weather service. Each
      municipality is linked to a weather station it pulls data from (see documentation for `weather_setup.py`). The
      available data depends on the number of available sensors.
    - source: [Meteotrentino data archive](http://storico.meteotrentino.it/).
    - classification: dynamic collector, performs web scraping.

Collecting weather data requires the following steps:

- the initial collection of weather station information through WeatherStationCollector.
- the spatial join operation that uses the stations coordinates and the administrative border data from the Italian
  statistics institute ISTAT (`resources/istat_administrative_units_2020.gpkg`) to detect in which municipality the
  stations are installed.
- the linking of weather stations to municipalities.
- the collection of data for the currently used weather stations through WeatherDataCollector.

The first two actions are performed by WeatherStationCollector. The third action is performed
by `municipality_weatherstation_linker.py`. Those three actions are coordinated by the pipeline `weather_setup.py` (see
documentation for the package `pipelines`).
