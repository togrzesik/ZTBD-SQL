# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import sqlalchemy
# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory
from sqlalchemy import create_engine


all_filenames = []
import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        all_filenames.append(os.path.join(dirname, filename))

# You can write up to 5GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All" 
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current session

def build_city_lookup():
    """
    First 12 rows contain information about cities and no temperture data.
    Extract the rows and transform into useful lookup table
    """
    city_lookup = pd.read_csv('daily_temperature_1000_cities_1980_2020.csv', nrows=12).T
    city_lookup.columns = city_lookup.iloc[0]
    city_lookup = city_lookup.iloc[1:,:]
    city_lookup["lat"] = city_lookup.lat.astype(np.float)
    city_lookup["lng"] = city_lookup.lng.astype(np.float)
    city_lookup["lng"] = city_lookup.lng.astype(np.float)


    return city_lookup


cities=build_city_lookup()
engine = create_engine("postgresql://postgres:31080@localhost/postgres")
print(build_city_lookup())


def build_reduced_city_lookup():
    """
    Convert city_lookup types and assert data is valid
    """
    city_lookup = build_city_lookup()

    # First 12 rows are information about the cities
    city_lookup = build_city_lookup()
    city_lookup = city_lookup[["city", "lat", "lng", "country", "population"]]
    city_lookup.loc[:,"population"] = city_lookup.population.fillna(0).astype(float).astype(np.uint)

    assert len(city_lookup) == len(city_lookup.drop_duplicates())
    non_null = city_lookup[["city", "country", "lat", "lng"]]
    assert len(non_null[non_null.isnull().T.any()]) == 0, non_null[non_null.isnull().T.any()]

    assert city_lookup.loc[(city_lookup.lat < -90) | (city_lookup.lat > 90), "lat"].count() == 0
    assert city_lookup.loc[(city_lookup.lng < -180) | (city_lookup.lng > 180), "lng"].count() == 0

    return city_lookup

city_lookup = build_reduced_city_lookup()
print(city_lookup.info())
city_lookup

def city_by_name(city_lookup, city_name:str):
    """Lookup col of city and call city_by_index"""
    city_col  = city_lookup[city_lookup["city"]==city_name]
    city_index = int(city_col.index[0])

    return city_by_index(city_index), city_col

def city_by_index(city_col:int):
    """Read only one col from the csv that contains the city we are interested in"""
    city_data = pd.read_csv('daily_temperature_1000_cities_1980_2020.csv', skiprows=12, usecols=[0, city_col + 1], index_col=0, parse_dates=True, cache_dates=False).iloc[:, 0]

    return city_data

# summary = f"""This data contains daily temperatures for {len(city_lookup)} cities coving a population of at least {city_lookup["population"].sum():,} and {len(city_lookup["country"].unique())} countries. The first recorded day is {sample_city.index.min().strftime('%d %B, %Y')} and the last {sample_city.index.max().strftime('%d %B, %Y')}."""
# print(summary)


city_lookup.to_sql('cities', con=engine)
for i in range(1000):
    sample_city = city_by_index(i)
    city_ids = [i] * len(sample_city)
    sample_city = sample_city.to_frame()
    sample_city['city_id'] = city_ids
    sample_city.columns = ['value', 'city_id']
    sample_city.to_sql('meas', if_exists='append', con=engine)
    print(sample_city)


#for i in range(1000):
#    city_by_index(i);

#for index, record in enumerate(cities):
#    print(index, record)

#    print("Insert INTO CITY VALUES ({}, {}, \'{}\') ")
#    print("Insert INTO Measurement VALUES ({}, {}, \'{}\') ")

