# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 13:00:52 2017

@author: aawie
"""

import pandas as pd
import numpy as np
import requests
import zipfile
import os



# OS-independent Subdirecotry

subdir = os.path.join(os.path.curdir, "data")

# Create OS-independent file names
election_csv = os.path.join(subdir, "2016_US_County_Level_Presidential_Results.csv")

censusgazette = os.path.join(subdir, "Gaz_counties_national")

output_csv = os.path.join(subdir, "combined_data.csv")

# Get API keys

with open('censusapi.txt') as file:
    census_key = file.read()
    
with open('nrelapi.txt') as file:
    nrel_key = file.read()



# Check if tab deliminated txt file alaready exists, and unzip if it does not
if not os.path.isfile(censusgazette+".txt"):
    with zipfile.ZipFile(censusgazette+".zip", "r") as zip_ref:
        zip_ref.extractall(subdir)

# Load census fips and lat/long
fips = pd.read_table(censusgazette+".txt", dtype = {'GEOID' : str},
                     skip_blank_lines=True, encoding = "ISO-8859-1")
                  
# Remove white space    
fips.columns = fips.columns.str.strip()
    
# Strip 'county' and 'borough' from county names
fips['NAME'] = fips['NAME'].str.replace(' County| Borough| Parish| Municipio', '')


# Combine county and state names

fips['combined'] = fips['USPS'] + '-' + fips['NAME']

# Create fips dictionary

fips_dict = dict(zip(fips['combined'], fips['GEOID']))

# Create latitude and longitude dictionaries

long_dict = dict(zip(fips['GEOID'], fips['INTPTLONG']))
lat_dict = dict(zip(fips['GEOID'], fips['INTPTLAT']))

# Special code for Alaska (not many entries in PV data anyway)
fips_dict['AK-Alaska'] = '02013'

# Create NREL API call for PV data
nrel_url = "https://developer.nrel.gov/api/solar/open_pv/installs/rankings"

# Create empty dataframe

PVdata = pd.DataFrame()

for state in fips['USPS'].unique():
        
    # Create API request
    nrel_params = {'api_key' : nrel_key, 'state' : state}
    nrel_request = requests.get(nrel_url, params=nrel_params)
    
    # Load as dictionary/list
    nrel_json = nrel_request.json()
    
    # Load result of API call into temporary dataframe
    temp_frame = pd.DataFrame(nrel_json['result'])
    
    # Rename columns
    temp_frame.rename(columns={'cap' : 'capacity', 'name' : 'county'}, inplace=True)
    
    # Replace 0s in cost with weighted average for state
    temp_frame2 = temp_frame[temp_frame['cost'] > 0]
    avg_cost = (temp_frame2['cost'] * temp_frame2['capacity']).sum() / temp_frame2['capacity'].sum()
    temp_frame.loc[temp_frame['cost'] == 0, 'cost'] = avg_cost
    
    
    # Add column for state
    temp_frame['state'] = state
    
    # concatenate with previous dataframe
    PVdata = pd.concat([PVdata, temp_frame])
    
    
# Remove data where installation type has no county

PVdata.loc[PVdata['state'] == 'AK', 'county'] = 'Alaska'
PVdata = PVdata[PVdata['county'].isnull() == False]


# Fix misspelled county names
PVdata['county'] = PVdata['county'].str.replace('Doa Ana|Doña Ana|Do̱a Ana', 'Dona Ana')
PVdata['county'] = PVdata['county'].str.replace('St ', 'St. ')
PVdata['county'] = PVdata['county'].str.replace('Baltimore City', 'Baltimore city')
PVdata['county'] = PVdata['county'].str.replace('St. Louis City', 'St. Louis city')
PVdata['county'] = PVdata['county'].str.replace('Newport News', 'Newport News city')
PVdata['county'] = PVdata['county'].str.replace('Norfolk', 'Norfolk city')
PVdata['county'] = PVdata['county'].str.replace('La Salle', 'LaSalle')
PVdata['county'] = PVdata['county'].str.replace('Fond Du Lac', 'Fond du Lac')
PVdata['county'] = PVdata['county'].str.replace('DC', 'District of Columbia')


PVdata.loc[PVdata['state'] == 'TX', 'county'] = PVdata.loc[PVdata['state'] == 'TX', 'county'].str.replace('De Witt', 'DeWitt')
PVdata.loc[PVdata['state'] == 'MA', 'county'] = PVdata.loc[PVdata['state'] == 'MA', 'county'].str.replace('Norfolk city', 'Norfolk')
PVdata.loc[PVdata['state'] == 'DE', 'county'] = PVdata.loc[PVdata['state'] == 'DE', 'county'].str.replace('Butler', 'Kent')

# Combine county and state names and assign fips ID
PVdata['combined'] = PVdata['state'] + '-' + PVdata['county']

PVdata['fips'] = PVdata['combined'].map(fips_dict)
PVdata['longitude'] = PVdata['fips'].map(long_dict)
PVdata['latitude'] = PVdata['fips'].map(lat_dict)



election_results = pd.read_csv(election_csv, dtype = {'combined_fips' : str})

election_results.rename(columns={'combined_fips' : 'fips'}, inplace=True)

election_results['fips'] = election_results['fips'].str.zfill(5)

# Create Census API request
acs_url = "https://api.census.gov/data/2015/acs1"
payload = {"get" : "B01001_001E,B01002_001E,B01001A_001E,B19013_001E,B25077_001E,B15003_022E,B08006_003E",
           "for" : "county:*",
           "key" : census_key}

# GET census data

census_req = requests.get(acs_url, params=payload)

census = pd.read_json(census_req.text)

census = census.drop(0)

census.columns = ['Population', 'Median Age', 'White Population', 
                  'Median Household Income', 'Median House Price', 
                  'Bachelors Degree', 'Drive Alone', 'State', 'County']

census['fips'] = census['State'] + census['County']


combined_data = PVdata.merge(election_results, on = 'fips', left_index=True)
combined_data2 = combined_data.merge(census, on = 'fips', left_index=True)

combined_data2.to_csv(output_csv)


