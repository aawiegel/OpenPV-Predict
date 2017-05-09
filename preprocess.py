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
openpv_zip = os.path.join(subdir, "openpv_all.zip")
openpv_csv = os.path.join(subdir, "openpv_all.csv")
censusfips_csv = os.path.join(subdir, "fips_national_county.txt")
election_csv = os.path.join(subdir, "2016_US_County_Level_Presidential_Results.csv")

output_csv = os.path.join(subdir, "combined_data.csv")

with open('censusapi.txt') as file:
    census_key = file.read()

# Create API request
acs_url = "https://api.census.gov/data/2015/acs1"
payload = {"get" : "B01001_001E,B01002_001E,B01001A_001E,B19013_001E,B25077_001E,B15003_022E,B08006_003E",
           "for" : "county:*",
           "key" : census_key}



# Check if CSV file already exists, and unzip it if it does not
if not os.path.isfile(openpv_csv):
    with zipfile.ZipFile(openpv_zip, "r") as zip_ref:
        zip_ref.extractall(subdir)

# Load census fips
fips = pd.read_csv(censusfips_csv, header=None, 
                   names = ["state", "stateid", "countyid", "countyname", "fips code type"],
                   dtype = {'state' : str, "stateid" : str, "countyid" : str})

# Strip 'county' and 'borough' from county names
fips['countyname'] = fips['countyname'].str.replace(' County| Borough| Parish| Municipio', '')


# Combine county and state names

fips['combined'] = fips['state'] + '-' + fips['countyname']
fips['combinedid'] = fips['stateid'] + fips['countyid']

# Create fips dictionary

fips_dict = dict(zip(fips['combined'], fips['combinedid']))

# Special code for Alaska (not many entries in PV data anyway)
fips_dict['AK-Alaska'] = '02013'


# Load PV data        
PVdata = pd.read_csv(openpv_csv, 
                     usecols=[0, 1, 2, 3, 4, 6, 7, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
                     dtype = {'zipcode' : str})


    
# Remove data where installation type is unavailable, non-residential, has no county,
# or where the PV capacity is clearly misentered (A 5 acre solar farm has about ~1MW capacity)

PVdata = PVdata[PVdata['install_type'].isnull() == False]
PVdata = PVdata[PVdata['install_type'].str.contains('^[Rr]esidential')]
PVdata.loc[PVdata['state'] == 'AK', 'county'] = 'Alaska'
PVdata = PVdata[PVdata['county'].isnull() == False]
PVdata = PVdata[PVdata['size_kw'] < 1E3]

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

PVdata_agg = PVdata.groupby('combined').agg(np.sum)
PVdata_agg.reset_index(inplace=True)

PVdata_agg['fips'] = PVdata_agg['combined'].map(fips_dict)

election_results = pd.read_csv(election_csv, dtype = {'combined_fips' : str})

election_results['fips'] = election_results['combined_fips'].str.zfill(5)


# GET census data

r = requests.get(acs_url, params=payload)

census = pd.read_json(r.text)

census = census.drop(0)

census.columns = ['Population', 'Median Age', 'White Population', 
                  'Median Household Income', 'Median House Price', 
                  'Bachelors Degree', 'Drive Alone', 'State', 'County']

census['fips'] = census['State'] + census['County']


combined_data = PVdata_agg.merge(election_results, on = 'fips')
combined_data = combined_data.merge(census, on = 'fips')

combined_data.to_csv(output_csv)


