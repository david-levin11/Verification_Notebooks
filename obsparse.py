import os
import pandas as pd
from datetime import datetime, timedelta


def delete_files(indir):
    for fl in os.listdir(indir):
        os.remove(os.path.join(indir, fl))
        print(f'Removed: {fl} from {indir}')

hr_interval = 6

timedict = {1:'PoT01', 3:'PoT03', 6:'PoT06', 12:'PoT12'}

obsdir = r'\\AR-NV-Main\ARHusers$\David.Levin\My Documents\_David\NBMProbEval\LightningData'

outdir = os.path.join(obsdir, timedict[hr_interval])

obsfile = 'GLD_AK_20-23.csv'

delete_files(outdir)

df = pd.read_csv(os.path.join(obsdir, obsfile))

# Combine Date and Time columns into a single datetime column
df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

# Sort the dataframe by datetime
df = df.sort_values(by='datetime')

# Define the start and end of the period you are interested in
start_datetime = df['datetime'].min()
end_datetime = df['datetime'].max()

# Align start_datetime to the nearest previous even hour
start_datetime = start_datetime.replace(minute=0, second=0, microsecond=0)
if start_datetime.hour % hr_interval != 0:
    start_datetime -= timedelta(hours=start_datetime.hour % hr_interval)

# Initialize the start of the first 6-hour interval
interval_start = start_datetime

# Iterate through the dataframe in 6-hour intervals
while interval_start <= end_datetime:
    interval_end = interval_start + timedelta(hours=hr_interval)
    
    # Filter the dataframe for the current 6-hour interval
    df_interval = df[(df['datetime'] >= interval_start) & (df['datetime'] < interval_end)]
    
    # Check if the filtered dataframe is not empty
    if not df_interval.empty:
        # Generate the filename with the UTC hour at the end
        filename = f"data_valid_{interval_end.strftime('%Y%m%d_%H')}.csv"
        # Save the filtered dataframe to a CSV file
        df_interval.to_csv(os.path.join(outdir,filename), index=False)
        print(f"Saved {filename} to {outdir}")
    
    # Move to the next 6-hour interval
    interval_start = interval_end