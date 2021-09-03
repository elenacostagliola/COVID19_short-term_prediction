import argparse
import sys, os
import time
from collections import OrderedDict
import pandas as pd
from pymongo.errors import ConfigurationError

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from collectors import validation_utils
from data.models import *
from data.dao import *


def main(reprocess_all=False):
    # # Ingestion stage

    print("Connecting to DB")
    success = False
    n_retries = 0
    while n_retries <= 3:
        try:
            cddao = CuratedDataMongoDao()
            success = True
            break
        except ConfigurationError:
            n_retries += 1
            print(f"Connection attempt {n_retries} failed. Retrying in 3 s...")
            time.sleep(3)

    if not success:
        print("Connection to DB failed.")
        exit()

    # Define some column names
    doses_columns = ['first_doses_ag0',
                     'first_doses_ag1',
                     'first_doses_ag2',
                     'second_doses_ag0',
                     'second_doses_ag1',
                     'second_doses_ag2',
                     'first_doses',
                     'second_doses']
    variations = ["deaths"]

    # Read last element in curated_data table
    empty_table = False
    if not reprocess_all:
        last_record = CuratedData.to_df(cddao.get_most_recent_record())

        if len(last_record) == 0:
            empty_table = True

    if not empty_table and not reprocess_all:

        # Get latest date
        latest_date = last_record["date"].values[0]

        # Get latest counts
        doses_counter = dict(((c, last_record[c].values[0]) for c in doses_columns))
        prev_counter = dict(((c, last_record[c].values[0]) for c in variations))
    else:
        latest_date = None
        doses_counter = dict(((c, 0) for c in doses_columns))
        prev_counter = dict(((c, 0) for c in variations))

    # If reprocessing, clear previous curated data
    if reprocess_all:
        print("Deleting previous data")
        cddao.clear()

    # Search date range (None=lower or upper default bound)
    date_from = latest_date + pd.Timedelta(days=1) if latest_date is not None else None
    date_to = None
    date_from, date_to = validation_utils.validate_dates(date_from, date_to)

    print(f"Processing data from {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")

    # Read static information tables
    print("Retrieving population from Province")
    pat_population = ProvinceMySqlDao().get_population("PAT")

    # Read data tables
    avail_sources = 4  # Do not count Holiday
    print("Retrieving ProvinceData")
    df_provdata = ProvinceData.to_df(ProvinceDataMySqlDao().get_by_date(date_from, date_to))
    print("Retrieving RegionRisk")
    df_risk = RegionRisk.to_df(RegionRiskMySqlDao().get_by_date(date_from, date_to))
    print("Retrieving VaccinesAdministrationData")
    df_vaxadm = VaccinesAdministrationData.to_df(VaccinesAdministrationDataMySqlDao().get_by_date(date_from, date_to))
    print("Retrieving Holiday")
    df_holiday = Holiday.to_df(HolidayMySqlDao().get_by_date(date_from, date_to))
    print("Retrieving StringencyIndex")
    df_stringency = StringencyIndex.to_df(StringencyIndexMySqlDao().get_by_date(date_from, date_to))
    # print("Retrieving average temperature from WeatherData")
    # df_temperature = WeatherData.to_df(WeatherDataMySqlDao().get_average_temperature(date_from, date_to))

    # Check: proceed only if all data are available
    sources = 0
    if len(df_provdata) > 0:
        sources += 1
    if len(df_risk) > 0:
        sources += 1
    if len(df_vaxadm) > 0:
        sources += 1
    if len(df_stringency) > 0:
        sources += 1
    # if len(df_temperature) > 0:
    #     sources += 1

    if sources != avail_sources:
        print("No data to be processed")
        exit()
    else:
        print("Integrating and cleaning data")

    # # Data manipulation
    #
    # ## Vaccine data: relabel age groups and pivot

    def relabel_age_group(g):
        labels = [0, 1, 2]
        g = g.strip()
        if g == "90+":
            return labels[2]
        age_range = g.split("-")
        if int(age_range[1]) < 30:
            return labels[0]
        elif int(age_range[0]) >= 60:
            return labels[2]
        elif int(age_range[0]) >= 30 and int(age_range[1]) < 60:
            return labels[1]

    df_vaxadm = df_vaxadm.drop(columns=["supplier", "region", "male", "female"])
    df_vaxadm.age_group = df_vaxadm.age_group.apply(relabel_age_group)

    df_vax = df_vaxadm.pivot_table(index="administration_date", values=["first_dose", "second_dose"],
                                   columns="age_group",
                                   aggfunc='sum', fill_value=0)
    df_vax["new_first_doses"] = df_vax["first_dose"].sum(axis=1)
    df_vax["new_second_doses"] = df_vax["second_dose"].sum(axis=1)
    df_vax.columns = ["new_first_doses_ag0", "new_first_doses_ag1", "new_first_doses_ag2",
                      "new_second_doses_ag0", "new_second_doses_ag1", "new_second_doses_ag2",
                      "new_first_doses", "new_second_doses"]
    df_vax = df_vax.reset_index(drop=False).rename(columns={"administration_date": "date"})

    # ## Join all tables

    tables_to_join = [df_provdata, df_risk, df_stringency, df_vax]

    # Set date indexes
    for d in tables_to_join:
        d.set_index("date", inplace=True)

    # Join tables
    df = tables_to_join[0]
    for d in tables_to_join[1:]:
        df = df.join(d)

    # ## Fix columns
    df = df.drop(columns=["name", "region"]).rename(columns={"value": "stringency_index"})

    # ## Add holiday dummy variable
    if len(df_holiday) > 0:
        df_holiday["date_range"] = df_holiday.apply(
            lambda row: pd.date_range(start=row["start"], end=row["end"] - pd.DateOffset(days=1), freq="D"), axis=1)
        new_idx = df_holiday["date_range"].iloc[0]
        for i in range(1, len(df_holiday)):
            new_idx = new_idx.union(df_holiday["date_range"].iloc[i])
        df = df.join(pd.DataFrame(index=new_idx, columns=["holiday"]).fillna(True))
        df["holiday"] = df["holiday"].fillna(False)
    else:
        df["holiday"] = False

    # ## Handle NAs in vaccine new doses
    # Fill NA with 0
    doses_mask = ["doses" in c for c in df.columns]
    df.loc[:, doses_mask] = df.loc[:, doses_mask].fillna(0).astype(int)

    # # Create custom metrics
    #
    # ### Single count for hospitalised
    #
    # Removes hospitalized_infectious_deseases so that high_intensity and intensive_care can be used to compute a fraction of the totals
    hospitalized_mask = ["hospitalized" in c for c in df.columns]
    df["hospitalized"] = df.loc[:, hospitalized_mask].sum(axis=1)
    df = df.drop(columns=["hospitalized_infectious_diseases"])

    # ### Single count for RSA active patients
    df = df.drop(columns=['active_rsa', 'active_nursing_homes', 'active_int_struct'])

    # ### Vaccines: create cumulative counts and fraction of population
    #
    # Cumulative sums must be computed by increasing the current number (the pipeline will work with just the new data to be inserted, thus cumulative sums must start from there)
    # Initialise counter with previous counts
    doses_counter = OrderedDict(doses_counter)
    actual_doses_values = list(doses_counter.values())

    # Create a copy of new_doses values
    replaced_values = df.loc[:, ["new_" + c for c in doses_columns]].values

    # Add counts to first row
    replaced_values[0] = [replaced_values[0][i] + actual_doses_values[i] for i in range(len(doses_counter))]
    df[doses_columns] = replaced_values

    # Compute cumulative sum
    df[doses_columns] = df[doses_columns].cumsum()

    # Compute fraction of population
    df[["vaccinated_population", "fully_vaccinated_population"]] = df[["first_doses", "second_doses"]] / pat_population

    # ### Compute daily variations

    # Initialise counter with previous variations (assume it comes in as a dict)
    prev_counter = OrderedDict(prev_counter)
    prev_counter_values = list(prev_counter.values())

    # Compute variations in first row
    replaced_values = df[variations].diff().values
    replaced_values[0] = [df[variations].values[0][i] - prev_counter_values[i] for i in range(len(prev_counter))]
    df[["new_" + v for v in variations]] = replaced_values
    df[["new_" + v for v in variations]] = df[["new_" + v for v in variations]].astype(int)

    # # Save
    print(f"Saving {len(df)} records")
    data = CuratedData.from_df(df.reset_index(drop=False))
    cddao.save(data)

    print("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data validation and integration pipeline.')
    parser.add_argument('-r', '--reprocess-all', dest="reprocess_all", action="store_true",
                        help='clear current curated data table and reprocess all data, otherwise append new data only')
    args = parser.parse_args()

    main(reprocess_all=args.reprocess_all)
