# Downloading the data using modified code described in Python_PreReq_Notebook from the tutorial notebooks.
from urllib.request import urlretrieve
import os

# Import Meteostat library and dependencies
from datetime import datetime
from meteostat import Hourly, Point
import pandas as pd

# from the current `tute_1` directory, go back two levels to the `MAST30034` directory
output_relative_dir = './data/'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)
    
# now, for each type of data set we will need, we will create the paths
for target_dir in (['tlc_data/', 'weather_data/']): # taxi_zones should already exist
    if not os.path.exists(output_relative_dir + target_dir):
        os.makedirs(output_relative_dir + target_dir)
        for sub_dir in ['landing', 'raw', 'curated', 'development']:
            if not os.path.exists(output_relative_dir + target_dir + sub_dir):
                os.makedirs(output_relative_dir + target_dir + sub_dir)

YEARS = ['2023', '2024']
MONTHS = range(1, 7)

# this is the URL template as of 07/2023
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"#year-month.parquet

# data output directory is `data/tlc_data/`
tlc_output_dir = output_relative_dir + 'tlc_data/landing/'

for year in YEARS:
    for month in MONTHS:
        # 0-fill i.e 1 -> 01, 2 -> 02, etc
        month = str(month).zfill(2) 
        print(f"Begin month {month}, {year}")
        
        # generate url
        url = f'{URL_TEMPLATE}{year}-{month}.parquet'
        # generate output location and filename
        output_dir = f"{tlc_output_dir}/{year}-{month}.parquet"
        # download
        
        urlretrieve(url, output_dir)
        print(f"Downloaded month {month}")

# Set point for New York
new_york = Point(40.715, -74.007)

# Set time period
start = datetime(2023, 1, 1)
end = datetime(2023, 6, 30, 23, 59)

# Get hourly data
data = Hourly(new_york, start, end)
data = data.fetch()

year1_df = pd.DataFrame(data)

year1_df['time'] = year1_df.index
year1_df.reset_index(drop=True, inplace=True)

year1_df.to_csv(output_relative_dir + 'weather_data/landing/2023_weather.csv', index=False)

# Set point for New York
new_york = Point(40.715, -74.007)

# Set time period
start = datetime(2024, 1, 1)
end = datetime(2024, 6, 30, 23, 59)

# Get hourly data
data = Hourly(new_york, start, end)
data = data.fetch()

year2_df = pd.DataFrame(data)

year2_df['time'] = year2_df.index
year2_df.reset_index(drop=True, inplace=True)

year2_df.to_csv(output_relative_dir + 'weather_data/landing/2024_weather.csv', index=False)