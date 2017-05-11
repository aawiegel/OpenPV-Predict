# -*- coding: utf-8 -*-
"""
Created on Wed May 10 19:55:07 2017

@author: Aaron
"""

# Script to add daily insolation data to solar data
# Runs just below the hourly limit on API calls (672 versus 1000)
# So can only be run once an hour

import pandas as pd
import numpy as np
import requests

# Create OS independent file name

subdir = os.path.join(os.path.curdir, "data")

solar_path = os.path.join(subdir, "combined_data.csv")

output_csv = os.path.join(subdir, "irradiance_data.csv")

# Read file
solar_data = pd.read_csv(solar_path, dtype = {'fips' : str})

# Read api key
with open('nrelapi.txt') as file:
    nrel_key = file.read()

# API url
solar_url = "https://developer.nrel.gov/api/solar/solar_resource/v1.json"

# Initialize empty arrays

fips_arr = np.array([])
dni = np.array([])
ghi = np.array([])
tilt = np.array([])

for fips, lat, lon in zip(solar_data["fips"].values, 
                          solar_data["latitude"].values,
                          solar_data["longitude"].values):
    
    # Generate parameters for API request
    payload = {"api_key" : nrel_key, "lat" : lat, "lon" : lon}
    
    # Get request
    solar_req = requests.get(solar_url, params = payload)
    
    # Convert to json to dictionary
    solar_json = solar_req.json()
    
    # Add entries to numpy arrays
    fips_arr = np.append(fips_arr, fips)
    dni = np.append(dni, solar_json["outputs"]["avg_dni"]["annual"])
    ghi = np.append(ghi, solar_json["outputs"]["avg_ghi"]["annual"])
    tilt = np.append(tilt, solar_json["outputs"]["avg_lat_tilt"]["annual"])
    
irradiance_data = pd.DataFrame({"fips" : fips_arr, "Direct Irradiance" : dni,
                          "Horizontal Irradiance" : ghi,
                          "Tilt Irradiance" : tilt})
    
irradiance_data.to_csv(output_csv)

combined_data = solar_data.merge(irradiance_data, on='fips', left_index=True)
combined_data.to_csv(solar_path)
    
    