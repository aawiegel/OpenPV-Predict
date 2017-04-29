# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 13:00:52 2017

@author: aawie
"""

import pandas as pd
import zipfile
import os

# OS-independent Subdirecotry

subdir = os.path.join(os.path.curdir, "data")

# Create OS-independent file names
openpv_zip = os.path.join(subdir, "openpv_all.zip")
openpv_csv = os.path.join(subdir, "openpv_all.csv")

# Check if CSV file already exists, and unzip it if it does not
if not os.path.isfile(openpv_csv):
    with zipfile.ZipFile(openpv_zip, "r") as zip_ref:
        zip_ref.extractall(subdir)

# Load PV data        
PVdata = pd.read_csv(openpv_csv, usecols=[0, 1, 2, 3, 4, 6, 7, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29])
    
# Remove data where installation type is unavailable or non-residential
# OR where the PV capacity is clearly misentered (A 5 acre solar farm has about ~1MW capacity)

PVdata = PVdata[PVdata['install_type'].isnull() == False]
PVdata = PVdata[PVdata['install_type'].str.contains('^[R|r]esidential')]
PVdata = PVdata[PVdata['size_kw'] < 1E3]