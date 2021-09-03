# Pipelines

This package contains pipelines, some of which constitute the main functionalities of the project.

### Usage

The main pipelines can be run both as a script:

```shell
python <pipeline> [args]
```

or as a module:

```python
from pipelines import < pipeline >

< pipeline >.main([args])
```

## DB setup (`db_setup.py`)

This pipeline is meant to be run only once, after having set up the application and the MySQL database. It performs the
following actions:

- It creates tables in the MySQL database (according to the script `data/sql_scripts/init_tables.sql`).
- It runs all static collectors (see documentation for collectors).

Usage as a script:

```shell
python db_setup.py
```

## Data update (`data_update.py`)

This pipeline instantiates and runs all the updaters sequentially. As described in the documentation page for the
updaters, each updater collects only the most recent data (i.e. not already available in the MySQL database) for a given
entity.

To run all the available updaters:

```shell
python data_update.py --all
```

It is possible to run just some specific updaters by passing a list of the names of the respective entities, for
example:

```shell
python data_update.py --updaters ProvinceData StringencyIndex
```

Similarly, one can run all updaters except those in a list, by using the option `--skip <UPDATERS>`.

## Data curation (`data_curation.py`)

This pipeline transforms preprocessed data from collectors, stored in MySQL (one table for each of the collected
entities) into a single table in MongoDB. It performs the following actions:

- Retrieves the datum of Trentino population from the entity Province.
- Reads data for the other dynamic entities in the form of pandas DataFrames.
- VaccineAdministrationData is the only data structure that has multiple records for each date. This is because the
  original data table contains - in multiple rows - the daily number of inoculated doses of vaccine for each gender, age
  group and supplier. These counts are provided both for first and second doses. The pipeline renames the age group
  labels in order to consider broader age groups (e.g. from “30-40” to “30-60”), then it pivots the table in order to
  provide, for each day, the total number of inoculated doses for each age group, both for first and second doses.
- Joins all table according to the date index.
- Produces a single count for the total hospitalizations, so that high_intensity and intensive_care can be used to
  compute a fraction of the totals and keeps a single count for RSA active patients.
- Adds a dummy variable to select holidays.
- Computes cumulative counts of administered vaccine doses for each age group and their total, both for first and second
  doses.
- Computes the ratio of the total administered doses to the total population.
- Computes daily variations for the number of deaths.

The pipeline only processes what has not been processed before. It checks the most recent date in the table of curated
data, and reads all records for each entity stored in the MySQL database that have a date higher than the maximum found
in curated data. Therefore, it will not reprocess the whole dataset at each run. It will only process the most recent
unprocessed records. If it is run daily, it will produce a single row to be appended to the table of curated data.

The data schema both for the tables in MySQL and the curated data collection in MongoDB, see the documentation page for
the `data` package.

Usage:

```bash
python data_curation.py [--reprocess-all]
```

The option `-r` or `--reprocess-all` swipes the curated data table and reprocesses all data.

Curated data can be read from the DB by using the following commands:

```python
from data.dao import CuratedDataMongoDao
from data.models import CuratedData

# Get curated data from DB
dao = CuratedDataMongoDao()
data = dao.get_by_date()  # could also include date_from and date_to for filtering dates

# Transform into a pandas DataFrame
df = CuratedData.to_df(data)
```

## Hyperparameter tuning (`hyperparameter_tuning.py`)

Loads curated data and performs cross validation to choose the best hyperparameters for the SARIMAX model.

The hyperparameter tuning process performs a grid search on a predefined parameter space; for each configuration,
cross-validation is used to evaluate a performance measure which is then used to assess the best performing
configuration.

The process is based on a JSON configuration file. Here's an example:

```json
{
  "outputs": [
    "new_cases"
  ],
  "regressors": [
    "stringency_index"
  ],
  "date_from": "2021",
  "p_values": [
    0,
    1,
    2,
    3,
    4,
    5
  ],
  "d_values": [
    1
  ],
  "q_values": [
    0,
    1,
    2,
    3,
    4,
    5
  ],
  "P_values": [
    0,
    1,
    2
  ],
  "D_values": [
    1
  ],
  "Q_values": [
    0,
    1,
    2
  ],
  "m_values": [
    7
  ],
  "t_values": [
    "n"
  ],
  "performance_measure": "rmse",
  "cv_n_splits": 15,
  "cv_max_test_size": 7,
  "parallel": true,
  "debug": false
}
```

- `outputs` and `regressors` select the names of the variables to be used respectively as endogenous or exogenous in the
  model
- `date_from` allows to use a fraction of the data, starting from a date in the format `yyyy-mm-dd`; in this
  example, `2021` corresponds to `2021-01-01`
- the space of hyperparameters is defined by a list of values for each of the SARIMA parameters, e.g. `p_values` for the
  _p_ parameter of the AR (autoregressive) model; _t_ is the trend, which could be one of `('n', 'c', 't', 'ct')` (see
  statsmodels's
  SARIMAX [documentation](https://www.statsmodels.org/dev/generated/statsmodels.tsa.statespace.sarimax.SARIMAX.html)).
- `performance_measure` can be either `rmse` (root mean squared error) or `aic` (Akaike Information Criterion)
- `cv_n_splits` and `cv_max_test_size` regulate the cross-validation as described in `ml` package documentation
- `parallel` set to true uses multiprocessing and computes cross-validation for each configuration in its own process
- `debug` enables warning and errors.

The path to the configuration file must be passed as an argument when launching the script:

```bash
python hyperparameter_tuning.py <path_to_file>
```

The default model configurations are in `pipelines/hyperparameter_tuning_configurations`.

The pipeline works as follows:

- Parse the configuration file.
- Instantiate `CuratedMongoDao` and `HyperparameterTuningResultDao`.
- Use `CuratedMongoDao` to ingest curated data.
- Filter dates according to the configuration parameter `date_from`.
- Fill missing values with the method "forward fill", i.e. replace NAs with the previous known value.
- Filter columns for the output variable and the desired regressors, according to the configuration parameters.
- Create a set of indices to split the data during the cross-validation process (see `ml` package documentation).
- Create a set of possible configurations to be tested, according to the parameter ranges specified in the configuration
  file.
- Launch the `grid_search` method (see documentation in `ml`): this will return a list of dictionaries in the form

```
{
  "cfg": {
    "arima_order": [p, d, q],
    "seasonal_order": [P, D, Q, m],
    "trend": t
    },
  "score_mean": float,
  "score_se": float
}
```

where `cfg` contains the model hyperparameters, whereas `score_mean` and `score_se` are the mean of the performance
score throughout the cross_validation folds and its standard error, respectively.

- An instance of `HyperparameterTuningResult` is created, in which the above list of dictionaries is saved under the
  key `results` together with the content of the configuration with which the pipeline was initialised and some
  operational information such as the elapsed time (see the data schema at the documentation page for the `data`
  package).
- The above object is saved in the MongoDB collection "hyperparameters" through `HyperparameterTuningResultDao`.

## Forecast (`forecast.py`)

Loads curated data and the stored model hyperparameters, trains the corresponding SARIMAX model and produces a
short-term forecast. These forecasts are stored in MongoDB overwriting the existing values, i.e. the collection "
forecasts" has only the most recent forecasts.

The forecasts are produced for the models defined in a JSON configuration file. Here's an example:

```json
[
  {
    "output": "new_cases",
    "regressors": [
      "stringency_index"
    ]
  }
]
```

Different models can be added as different objects in the above list.

For each of the models defined above (in this example only one), the corresponding hyperparameters are pulled from
MongoDB "hyperparameters" collection through `HyperparameterTuningResultDao` in the form of a `HyperparameterResult`
object, from which the best performing configuration is extracted. If successive hyperparameter tuning processes are
performed via the pipeline `hyperparameter_tuning.py`, forecast pipeline uses the most recently added results.

The path to the configuration file must be passed as an argument when launching the script:

```bash
python forecast.py <path_to_file> --steps 7
```

The default models are in `pipelines/forecast_configuration/forecast_models.json`.

`steps` specifies the number of days for the out-of-sample forecast. If omitted, default is 7.

The pipeline works as follows:

- Parse the configuration file.
- Instantiate `CuratedMongoDao`, `HyperparameterTuningResultDao` and `ForecastDao`.
- Use `CuratedMongoDao` to ingest curated data.
- Fill missing values with the method "forward fill", i.e. replace NAs with the previous known value.
- Loop on each of the models specified in the configuration file:
    - Use `HyperparameterTuningResultDao` to find the best performing hyperparameters for the current model.
    - Filter columns for the output variable and the desired regressors.
    - Instanciate the model.
    - Forecast assuming exogenous variables remain constant in the future.
    - Use `ForecastDao` to save forecasts in the MongoDB "forecasts" collection.

The schema for Forecast entity is described in the documentation for `data`. Forecast results can then be read from the
DB by using the following commands:

```python
from data.dao import ForecastMongoDao
from data.models import Forecast

# Get forecasts from DB
dao = ForecastMongoDao()
forecasts = dao.get_by_date("new_cases")  # could also include date_from and date_to for filtering dates

# Transform into a pandas DataFrame
df = Forecast.to_df(forecasts)
```

The analysis of the forecasts, as well as their visualisation, can be performed with the notebook `modelling.ipynb`;
further information and code examples are in the `ml` package documentation.

## Weather pipelines

The collection of weather data has a slightly more complex flow because of the following reasons:

- Weather stations measure local weather conditions, thus they refer to a specific municipality and a specific altitude
  range.
- There is a large number of weather stations; some municipalities have multiple weather stations and some others have
  none; some rule must be found to select the stations to be used.
- In order to find measurements for a given municipality, there must be some kind of virtual weather station that
  returns data for that municipality; this means either selecting a specific station or averaging over multiple
  stations.

The collection of weather data involves therefore the creation of a rule that links stations to municipalities. This
action must only be performed once setting up the database (`db_setup.py`). Further updates to weather data (retrieving
new records) is performed by `data_update.py`.

To do so, `db_setup.py` calls a pipeline named `weather_setup.py` which, in turn,
runs `municipality_weatherstation_linker.py`. The two pipelines are described below.

### Weather setup (`weather_setup.py`)

It works as follows:

- Instantiates DAO classes for Municipality, WeatherStation and MunicipalityWeatherStationLink.
- Uses MunicipalityDao to retrieve all stored municipalities from the MySQL database.
- Uses WeatherStationCollector to collect weather stations information.
- Saves weather stations into MySQL with WeatherStationDao.
- Calls `municipality_weatherstation_linker.py` passing the list of WeatherStation objects.
- The latter returns objects of class MunicipalityWeatherstationLink, which link municipalities to weather stations.
- The links are stored into MySQL using MunicipalityWeatherStationLinkDao.

### Municipality-WeatherStation linker (`municipality_weatherstation_linker.py`)

This pipeline uses a rule to assign a weather station to each municipality, yielding links between the two entities, so
that weather data for each municipality are collected from some assigned stations. Links are stored as
MunicipalityWeatherStationLink objects, which specify municipalities and weather stations as source and target of a
directed link, respectively.

To do so, the geographical coordinates of each weather station have been used in `WeatherStationCollector` to locate in
which municipality they are installed (by performing spatial joins through the geopandas library, see documentation
for `collectors`). Then, for each municipality, a rule is used to select which station to assign. Some municipalities
have more than one weather station, some others have no stations at all.

The default rule is to choose weather stations at an altitude lower than 1600 m and, among those, the one with the
largest number of sensors. Since almost all stations measure the temperature but only some of them have other sensors (
see data schema), those with a large number of sensors are preferred. If there are still more than one station following
these criteria for a given municipality, the station that is installed at the lowest altitude is chosen. This rule is
called "max_sensors_min_elevation". Therefore, for each municipality that has at least one station, the one with maximum
number of sensors and, among those, the one with minimum altitude is assigned.

This pipeline can be re-run to reset links and form new ones according to different rules, if needed. For instance, it
can be extended so that municipalities which have no stations can still "borrow" data from neighbouring stations
installed in a different municipality.

In short, the pipeline performs the following steps:

- The pipeline is called by passing the list of weather stations.
- Stations with zero sensors or an altitude higher than the threshold are discarded.
- Stations are grouped by municipality.
- For each municipality, the stations with the maximum number of sensors and the minimum altitude is selected.
- An edgelist of (source, target) pairs is created; this is used to instanciate MunicipalityWeatherStationLink objects.
- The latter are saved to the MySQL database through the corresponding DAO.