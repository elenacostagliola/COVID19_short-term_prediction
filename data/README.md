# Data package

This package contains the following modules:
- `models.py`: classes that model entities that are passed from one pipeline to another; collectors return lists of 
instances of the class referring to the entity they are collecting data for; also curated data are modelled by a class 
  defined in this module.
- `dao.py`: Data Access Objects (DAO) used as interface between the classes that model the entities and the database;
interaction with the database is therefore always performed through data access objects.
- `mysql_wrapper.py` and `mongo_wrapper.py`: wrapper classes for interacting with MySQL through the python connector,
and with MongoDB through pymongo, respectively. Their methods are used by DAOs. Credentials are pulled from the
configuration package.

The folder `sql_scripts` contains the file `init_tables.sql` that is run upon DB initialisation (DB Setup pipeline) to 
create tables in the MySQL database.

## Models

Model classes define attributes for each entity. The file therefore defines the **data schema**.

Each class has the following methods (static methods):
 - from_repr: from a list of dictionaries to a list of instances of the class.
 - to_repr: from a list of instances to a list of dictionaries.
 - from_df: from a pandas DataFrame to a list of instances of the class.
 - to_df: from a list of instances to a pandas DataFrame.

The transformations to and from pandas DataFrames are widely used in pipelines, as most of the data transformations are
performed using pandas.

**Example:** each collector returns a list of instances of these classes; for the entity named "StringencyIndex", for instance,
we have:

```python
collector = StringencyIndexCollector()
results = collector.search(...)
```

results is a list of instances of the class `StringencyIndex`. In order to transform this into a pandas DataFrame:

```python
from data.models import StringencyIndex
df = StringencyIndex.to_df(results)
```

## DAOs

Data access objects sit between the code and the database. Each entity in models has its DAO class which is used to 
interact with the database. 

Each entity has an abstract class named like the corresponding class name in models and "Dao" as a suffix, e.g. Province --> ProvinceDao.
Abstract DAO classes define the methods that the DAO has (e.g. save, get_by_date).

These methods are then implemented in the "MySql" or "Mongo" DAO implementation, according to the type of database the entity is stored.
E.g. Province --> ProvinceMySqlDao and CuratedData --> CuratedDataMongoDao.

**Example:** save the instances of StringencyIndex returned as `result` in the example above:

```python
from data.dao import StringencyIndexMySqlDao
dao = StringencyIndexMySqlDao()
dao.save(results)
```

In order to get all records in a date range, one should call:

```python
results = dao.get_by_date(date_from, date_to)
```

where `results` is again returned as a list of instances of `StringencyIndex`.


## DB wrappers

The modules `mysql_wrapper.py` and `mongo_wrapper.py` have the objective of making the interaction with the database 
easier through higher abstraction. They provide both low level basic operations (read, insert, update, delete) and more
complex operations (e.g. table initialisation, counting objects, executing bulk operations).

### MySQL wrapper usage

```python
from data.mysql_wrapper import MySqlDB
db = MySqlDB()
```

Once instantiated, the connection to the DB does not exist. Before performing each operation by calling the instance 
methods, connection must be established by calling the `connect` method.

```python
db.connect()
db.call_method() # call one of the existing methods
```

The connection must be closed with the `close` method. One can conveniently use the syntax:

```python
with db.connect():
    db.call_method() # call one of the existing methods
```

that automatically closes the connection.

### MongoDB wrapper usage

```python
from data.mongo_wrapper import MongoDB
db = MongoDB()
```

Once instantiated, the first operation performed on the database will establish a connection to it. Since this process
may take some time, do not destroy and re-instantiate the wrapper at each operation.

```python
db.call_method() # call one of the existing methods
```


# Data Schema

- **Province**
  - `name`: str, name of the Italian Province (in this case - PAT).
  - `code`: int, official Italian National Institute of Statistics (ISTAT) code of the Province.
  - `population`: int, number of population.
  



- **ProvinceData**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00".
  - `name`: str, name of the Italian Province (in this case - PAT). References the name in Province(`name`).
  - `cases`: int, total number of covid cases from March 3rd 2020.
  - `new_cases`: int, daily variation of covid cases.
  - `active`: int, daily count of positive people.
  - `recovered`: int, daily count of revovered people.
  - `deaths`: int, total number of covid related deaths from March 3rd 2020.
  - `quarantined`: int, daily count of people in home isolation.
  - `hospitalized_infectious_diseases`: int, daily count of hospitalized people in infectious diseases department.
  - `hospitalized_high_intensity`: int, daily count of hospitalized people in high intensity department.
  - `hospitalized_intensive_care`: int, daily count of hospitalized people in intensive care department.
  - `discharged`: int, daily count of discharged hospitalized people.
  - `active_rsa`: int, daily count of cases in RSA (Residenza Sanitaria Assistenziale).
  - `active_nursing_homes`: int, daily count of cases in nursing homes.
  - `active_int_struct`: int, daily count of cases in intermediate structures.
  - `active_rsa_total`: int, total number of cases in RSA from March 3rd 2020.
  



- **Municipality**
  - `code`: int, official Italian National Institute of Statistics (ISTAT) code of the Municipality.
  - `name`: str, name of Trentino's Municipality.
  - `province`: str, name of the Italian Province (in this case - PAT). References the name in Province(`name`).
  - `lat`: float, latitude.
  - `lon`: float, longitude.
  - `population`: int, number of population.
  



- **MunicipalityData**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00".
  - `code`: int, official Italian National Institute of Statistics (ISTAT) code of the Municipality. References the name in Municipality (`code`).
  - `cases`: int, total number of covid cases by Municipality from March 3rd 2020.
  - `recovered`: int, total number of recovered people by Municipality from March 3rd 2020.
  - `deaths`: int, total number of covid related deaths by Municipality from March 3rd 2020.
  - `discharged`: int, total number of covid related discharged by Municipality from March 3rd 2020.
  



- **RegionRisk**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00".
  - `region`: str, name of the Italian Region (in this case - PAT). References the name in Province(`name`).
  - `risk`: str, categorical variable referred to the provincial epidemiological scenario identified by 4 colours - white, green, yellow, red.
  



- **Holiday**
  - `holiday`: name of Italian National Holiday.
  - `start`: timestamp, Holiday's starting date.
  - `end`: timestamp, Holiday's starting date.
  



- **VaccinesAdministrationData**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00". 
  - `region`: str, name of the Italian Region (in this case - PAT). References the name in Province(`name`).
  - `supplier`: str, vaccine suppliers - Janssen, Moderna, Pfizer/BioNTech, Vaxzevria (AstraZeneca).
  - `age_group`: str, age groups of vaccinated people - 20-29, 30-39, 40-49, 50-59, 60-69.
  - `male`: int, daily number of male vaccinated people.
  - `female`: int, daily number of female vaccinated people.
  - `first_dose`: int, daily number of first doses administered.
  - `second_dose`: int, daily number of second doses administered.
  



- **VaccinesDeliveryData**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00". 
  - `region`: str, name of the Italian Region (in this case - PAT). References the name in Province(`name`).
  - `supplier`: str, vaccine suppliers - Janssen, Moderna, Pfizer/BioNTech, Vaxzevria (AstraZeneca).
  - `n_doses`: int, daily number of vaccines' delivered doses.
  



- **WeatherStation**
  - `station_id`: str, id of the weather station.
  - `name`: str, name of the weather station.
  - `lat`: float, latitude.
  - `lon`: float, longitude.
  - `elev`: int, weather station's vertical distance above mean sea level.
  - `mun_code`: int, official Italian National Institute of Statistics (ISTAT) code of the Municipality.
  - `n_sensors`: int, number of eather station's sensor.
  - `precipitation_available`: bool, availability of precipitation sensor.
  - `temperature_available`: bool, availability of temperature sensor.
  - `humidity_available`: bool, availability of humidity sensor.
  - `wind_speed_available`: bool, availability of wind speed sensor.
  - `atmospheric_pressure_available`: bool, availability of atmospheric pressure sensor.
  - `solar_rad_available`: bool, availability of solar radiation sensor.
  



- **WeatherData**
  - `station_id`: str, id of the weather station. References the name in WeatherStation (`station_id`).
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00". 
  - `precipitation_total`: float, daily total precipitation recorded by the station.
  - `temperature_mean`: float, average daily temperature recorded by the station.
  - `humidity_mean`: float, average daily humidity recorded by the station.
  - `wind_speed_mean`: float, average daily wind speed recorded by the station.
  - `atmospheric_pressure_mean`: float, average daily atmospheric pressure recorded by the station.
  - `solar_rad_total`: float, daily total solar radiation recorded by the station.




- **MunicipalityWeatherStationLink**
  - `municipality_code`: int, official Italian National Institute of Statistics (ISTAT) code of the Municipality. References the name in Municipality (`code`).
  - `station_id`: str, id of the weather station. References the name in WeatherStation (`station_id`).
 



- **StringencyIndex**
  - `date`: timestamp, in format "YYYY-MM-DD 00:00:00". 
  - `value`: float.
  
