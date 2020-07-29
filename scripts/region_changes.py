#!/usr/bin/env python

##
# Downloads an excel file from The Norwegian Mapping Authority for all the changes in Norwgian regions from 2019/2020
# Changes upper case to title case and saves files as csv and pickle.
#
# Author: Lasse Lambrechts <lasse.lambrechts@bt.no>
#
# Data source: https://www.kartverket.no/kommunereform/tekniske-endringer/kommune--og-regionsendringer-2020/
#
import os

import pandas as pd

PROJECT_DIRECTORY = os.path.realpath(os.path.curdir)

df = pd.read_excel("https://www.kartverket.no/globalassets/kommunereform/fylker-kommuner-2019-2020-alle.xlsx", converters={
    'Fylkesnr. 2019': '{:0>2}'.format, 'Kommunenr. 2019': '{:0>4}'.format, 'Fylkesnr. 2020': '{:0>2}'.format, 'Kommunenr. 2020': '{:0>4}'.format})
df["Fylkesnavn 2019"] = df["Fylkesnavn 2019"].str.title().replace("Og", "og", regex=True)
df["Kommunenavn 2019"] = df["Kommunenavn 2019"].str.title().replace("Og", "og", regex=True)
df["Fylkesnavn 2020"] = df["Fylkesnavn 2020"].str.title().replace("Og", "og", regex=True)
df["Kommunenavn 2020"] = df["Kommunenavn 2020"].str.title().replace("Og", "og", regex=True)

df.to_csv(os.path.join(PROJECT_DIRECTORY, "data/csv/", "norwegian_regions_changes.csv"), index=False)
df.to_pickle(os.path.join(PROJECT_DIRECTORY, "data/pkl/", "norwegian_regions_changes.pkl"))
