CREATE TABLE IF NOT EXISTS Province
(
    `name`       varchar(100) not null,
    `code`       smallint,
    `population` int,
    PRIMARY KEY (`name`)
);


CREATE TABLE IF NOT EXISTS ProvinceData
(
    `date`                             timestamp,
    `name`                             varchar(100) not null,
    `cases`                            int,
    `new_cases`                        int,
    `active`                           int,
    `recovered`                        int,
    `deaths`                           int,
    `quarantined`                      int,
    `hospitalized_infectious_diseases` int,
    `hospitalized_high_intensity`      int,
    `hospitalized_intensive_care`      int,
    `discharged`                       int,
    `active_rsa`                       int,
    `active_nursing_homes`             int,
    `active_int_struct`                int,
    `active_rsa_total`                 int,
    PRIMARY KEY (`date`),
    FOREIGN KEY (`name`) REFERENCES Province (`name`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS Municipality
(
    `code`       smallint,
    `name`       varchar(100) not null,
    `province`   varchar(100) not null,
    `lat`        float,
    `lon`        float,
    `population` int,
    PRIMARY KEY (`code`),
    FOREIGN KEY (`province`) REFERENCES Province (`name`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS MunicipalityData
(
    `date`       timestamp,
    `code`       smallint,
    `cases`      int,
    `recovered`  int,
    `deaths`     int,
    `discharged` int,
    INDEX (`date`),
    FOREIGN KEY (`code`) REFERENCES Municipality (`code`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS RegionRisk
(
    `date`   timestamp,
    `region` varchar(100) not null,
    `risk`   varchar(100) not null,
    PRIMARY KEY (`date`),
    FOREIGN KEY (`region`) REFERENCES Province (`name`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS Holiday
(
    `holiday` varchar(100) not null,
    `start`   timestamp,
    `end`     timestamp,
    INDEX (`start`, `end`)
);

CREATE TABLE IF NOT EXISTS VaccinesAdministrationData
(
    `date`        timestamp,
    `region`      varchar(100) not null,
    `supplier`    varchar(100) not null,
    `age_group`   varchar(100) not null,
    `male`        int,
    `female`      int,
    `first_dose`  int,
    `second_dose` int,
    INDEX (`date`),
    FOREIGN KEY (`region`) REFERENCES Province (`name`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS VaccinesDeliveryData
(
    `date`     timestamp,
    `region`   varchar(100) not null,
    `supplier` varchar(100) not null,
    `n_doses`  int,
    INDEX (`date`),
    FOREIGN KEY (`region`) REFERENCES Province (`name`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS WeatherStation
(
    `station_id`                     varchar(6)  not null,
    `name`                           varchar(50) not null,
    `lat`                            float       not null,
    `lon`                            float       not null,
    `elev`                           smallint    not null,
    `mun_code`                       smallint    not null,
    `n_sensors`                      smallint    not null,
    `precipitation_available`        bool        not null,
    `temperature_available`          bool        not null,
    `humidity_available`             bool        not null,
    `wind_speed_available`           bool        not null,
    `atmospheric_pressure_available` bool        not null,
    `solar_rad_available`            bool        not null,
    PRIMARY KEY (`station_id`)
);


CREATE TABLE IF NOT EXISTS WeatherData
(
    `station_id`                varchar(6) not null,
    `date`                      timestamp  not null,
    `precipitation_total`       float,
    `temperature_mean`          float,
    `humidity_mean`             float,
    `wind_speed_mean`           float,
    `atmospheric_pressure_mean` float,
    `solar_rad_total`           float,
    INDEX (`station_id`, `date`),
    FOREIGN KEY (`station_id`) REFERENCES WeatherStation (`station_id`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS MunicipalityWeatherStationLink
(
    `municipality_code` smallint   not null,
    `station_id`        varchar(6) not null,
    FOREIGN KEY (`municipality_code`) REFERENCES Municipality (`code`) ON DELETE CASCADE,
    FOREIGN KEY (`station_id`) REFERENCES WeatherStation (`station_id`) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS StringencyIndex
(
    `date`  timestamp,
    `value` float,
    INDEX (`date`)
);