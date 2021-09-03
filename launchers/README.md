# Launchers

This pipelines are the entry point of the deployed application. They run the main pipelines described in the first
documentation page in a predefined way.

These launchers can be run on demand or scheduled to run at specific times (e.g. once a day at midnight, when all data
sources have probabily been updated by data providers).

## Data Update launcher

It launches `pipelines/data_update.py` for updating the sources currently used to predict the number of new COVID-19
infections in Trentino. It corresponds to the command:

```bash
python pipelines/data_update.py --updaters ProvinceData, Holiday, RegionRisk, VaccinesAdministrationData, VaccinesDeliveryData, StringencyIndex
```

## Data Update and Curation launcher

It calls Data Update launcher first, then starts Data Curation (`pipelines/data_curation.py`). It is equivalent to run:

```bash
python launchers/data_update_launcher.py
python pipelines/data_curation.py
```

## Hyperparameter Tuning launcher

It reads the JSON configuration files in `pipelines/hyperparameter_tuning_configurations` and, for each file `cfg.json`,
equivalently calls:

```bash
python pipelines/hyperparameter_tuning.py `pipelines/hyperparameter_tuning_configurations/cfg.json`
```

## Forecast launcher

It reads the JSON configuration files in `pipelines/forecast_configuration` and, for each file `cfg.json`, equivalently
calls:

```bash
python pipelines/forecast.py pipelines/hyperparameter_tuning_configurations/cfg.json --steps 7
```

to produce 7-steps-ahead forecasts.