import os
import pandas as pd
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point
from rasterio import open as rio_open
from rasterio.features import dataset_features
from shapely.ops import unary_union
from pyproj import Transformer
import numpy as np
from scipy.spatial import cKDTree

hr_interval = 6

timedict = {1:'PoT01', 3:'PoT03', 6:'PoT06', 12:'PoT12'}

vardict = {1:'tstm01', 3:'tstm03', 6:'tstm06', 12:'tstm12'}

obsdir = r'\\AR-NV-Main\ARHusers$\David.Levin\My Documents\_David\NBMProbEval\LightningData'

tstmdir = os.path.join(obsdir, timedict[hr_interval])

nbmdir = r'C:\Users\David.Levin\NBMProbThunder\NBMData'

tifdir = os.path.join(nbmdir, timedict[hr_interval])


def build_valid_dates(flpath):
    valid_dates = []
    for fl in os.listdir(flpath):
        rawdate = fl.split('.')[0].split('_')[4]
        dt = (datetime.strptime(rawdate,'%Y-%m-%dT%H-%M')).strftime('%Y%m%d_%H')
        valid_dates.append(dt)
    return valid_dates


# Function to extract valid time from the first directory's filenames
def extract_valid_time_obs(filename):
    try:
        parts = filename.split('_')
        date_str = parts[2] + '_' + parts[3].split('.')[0]
        return datetime.strptime(date_str, '%Y%m%d_%H')
    except (IndexError, ValueError):
        return None

# Function to extract valid time from the second directory's filenames
def extract_valid_time_nbm(filename):
    try:
        parts = filename.split('_')
        date_str = parts[5].replace('-', ':')
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
    except (IndexError, ValueError):
        return None
    
# List files in the first directory
files_obs = os.listdir(tstmdir)

# List files in the second directory
files_nbm = os.listdir(tifdir)

# Extract valid times and filenames from the second directory
valid_times_nbm = {extract_valid_time_nbm(f): f for f in files_nbm if extract_valid_time_nbm(f) is not None}


print(valid_times_nbm)

# for item in os.listdir(tstmdir):
#     if item == 'data_valid_20210711_06.csv':
#         # Load the lightning strikes data
#         lightning_data = pd.read_csv(os.path.join(tstmdir, item))
#         lightning_data['datetime'] = pd.to_datetime(lightning_data['datetime'])
#         lightning_gdf = gpd.GeoDataFrame(lightning_data, geometry=gpd.points_from_xy(lightning_data.Lon, lightning_data.Lat))

#         # Load the gridded probability data
#         with rio_open('path_to_probability_data.tif') as src:
#             probability_data = src.read(1)
#             transform = src.transform

#         # Get grid points with probability > 0
#         rows, cols = np.where(probability_data > 0)
#         probability_points = [Point(transform * (col, row)) for row, col in zip(rows, cols)]

#         # Convert to GeoDataFrame
#         probability_gdf = gpd.GeoDataFrame(geometry=probability_points)

#         # Convert coordinates to lat/lon if necessary (assuming the tif is in a different CRS)
#         # Assuming the .tif is in EPSG:3857 and we want to convert to EPSG:4326 (lat/lon)
#         transformer = Transformer.from_crs("epsg:3857", "epsg:4326", always_xy=True)
#         probability_gdf['geometry'] = probability_gdf['geometry'].apply(lambda geom: Point(transformer.transform(geom.x, geom.y)))

#         # Define the 40km radius buffer
#         buffer_radius = 40000  # in meters

#         # Buffer the grid points by 40km
#         probability_gdf['geometry'] = probability_gdf.buffer(buffer_radius)

#         # Spatial join to find hits
#         hits = gpd.sjoin(lightning_gdf, probability_gdf, op='within')

#         # Results
#         print(f"Number of hits: {len(hits)}")
#         print(hits)
