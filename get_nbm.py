import os
import requests
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from datetime import datetime, timedelta

hr_interval = 6

timedict = {1:'PoT01', 3:'PoT03', 6:'PoT06', 12:'PoT12'}

vardict = {1:'tstm01', 3:'tstm03', 6:'tstm06', 12:'tstm12'}

obsdir = r'\\AR-NV-Main\ARHusers$\David.Levin\My Documents\_David\NBMProbEval\LightningData'

tstmdir = os.path.join(obsdir, timedict[hr_interval])

nbmdir = r'C:\Users\David.Levin\NBMProbThunder\NBMData'

outdir = os.path.join(nbmdir, timedict[hr_interval])

obsfile = 'GLD_AK_20-23.csv'

DOMAIN = 'alaska'

fcst_time_step = 24

client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

bucket = 'noaa-nbm-pds'

df = pd.read_csv(os.path.join(obsdir, obsfile))

# Combine Date and Time columns into a single datetime column
df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

# Function to download .tif files from the S3 URL
def download_aws(bucket, key, output_path, fileout):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        #client.download_file(bucket, key, output_path)
        with open(os.path.join(output_path, fileout), 'wb') as f:
            client.download_fileobj(bucket, key, f)
        print(f"Downloaded {key} to {output_path}")
    except Exception as e:
        print(f"Failed to download {key}: {e}")

# Function to download .tif files from the S3 URL
def download_tif_file(url, output_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {output_path}")
    else:
        print(f"Failed to download {url}")

# Function to determine the blend version based on the date
def get_blend_version(date):
    if date < datetime(2023, 1, 17, 12):
        return 'blendv4.0'
    elif datetime(2023, 1, 17, 12) <= date <= datetime(2024, 5, 16, 12):
        return 'blendv4.1'
    else:
        return 'blendv4.2'

# Function to process each observation file
def process_observation_file(file_path, timestep):
    # Extract the valid date and time from the file name
    file_name = os.path.basename(file_path)
    valid_date_str = file_name.split('_')[2]
    valid_time_str = file_name.split('_')[3].split('.')[0]
    valid_datetime_str = f"{valid_date_str}_{valid_time_str}"
    valid_time = datetime.strptime(valid_datetime_str, '%Y%m%d_%H')

    # Calculate the forecast start time (24 hours earlier)
    forecast_start_time = valid_time - timedelta(hours=timestep)

    # Check to see what blend we need
    blend = get_blend_version(forecast_start_time)

    forecast_year = forecast_start_time.year
    forecast_month = forecast_start_time.month
    forecast_day = forecast_start_time.day
    forecast_hour = forecast_start_time.hour

    # Format the runtime and validtime strings
    runtimestring = forecast_start_time.strftime('%Y-%m-%dT%H:%M')
    validtimestring = valid_time.strftime('%Y-%m-%dT%H:%M')
    runtimestring_path = forecast_start_time.strftime('%Y-%m-%dT%H-%M')
    validtimestring_path = valid_time.strftime('%Y-%m-%dT%H-%M')
    return forecast_year, forecast_month, forecast_day, forecast_hour, runtimestring, validtimestring, blend, runtimestring_path, validtimestring_path


for fl in os.listdir(tstmdir):
    forecast_year, forecast_month, forecast_day, forecast_hour, runtimestring, validtimestring, blend_version, runtimestring_path, validtimestring_path = process_observation_file(fl, fcst_time_step)
    # Construct the URL
    #tifurl = f'https://noaa-nbm-pds.s3.amazonaws.com/{blend_version}/alaska/{forecast_year:04d}/{forecast_month:02d}/{forecast_day:02d}/{forecast_hour:02d}00/{vardict[hr_interval]}/{blend_version}_alaska_tstm06_{runtimestring}_{validtimestring}.tif'
    bucketdir = f'{blend_version}/alaska/{forecast_year:04d}/{forecast_month:02d}/{forecast_day:02d}/{forecast_hour:02d}00/{vardict[hr_interval]}/'    
    tiffile = f'{blend_version}_alaska_{vardict[hr_interval]}_{runtimestring}_{validtimestring}.tif'
    awsfile = f'{bucketdir}{tiffile}'
    #print(f'Bucket dir is: {bucketdir} and tif file is: {tiffile}')
    print(f'Bucket key is: {awsfile}')
    # Define the output path
    #output_path = os.path.join(outdir, f'blend_alaska_{vardict[hr_interval]}_{runtimestring_path}_{validtimestring_path}_{fcst_time_step:03d}.tif')
    outfile = f'blend_alaska_{vardict[hr_interval]}_{runtimestring_path}_{validtimestring_path}_{fcst_time_step:03d}.tif'
    # Download the .tif file
    #download_tif_file(tifurl, output_path)
    download_aws(bucket, awsfile, outdir, outfile)
    

