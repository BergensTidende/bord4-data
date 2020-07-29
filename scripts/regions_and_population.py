#!/usr/bin/env python

##
# Downloads population data from the national statistical institute of Norway (SSB)
# Fills in county and muncipality ids and names for districts and municipalities.
# Saves files as csv and pickle.
#
# Author: Lasse Lambrechts <lasse.lambrechts@bt.no>
#
# Data sources: https://data.ssb.no/api/v0/no/table/10826/, https://data.ssb.no/api/v0/no/table/07459
#

import os
import requests

from datetime import date
from pyjstat import pyjstat
import pandas as pd

PROJECT_DIRECTORY = os.path.realpath(os.path.curdir)
YEAR = date.today().year

def get_population_districts():
    ##
    # Get the population data from the four largest cities in Norway
    #

    POST_URL = "https://data.ssb.no/api/v0/no/table/10826/"

    json_q = {"query":[{"code":"Region","selection":{"filter":"item","values":["030100aa", "030101a","030102a","030103a","030104a","030105a","030106a","030107a","030108a","030109a","030110a","030111a","030112a","030113a","030114a","030115a","030116a","030117a","030199a","110300a", "110301","110302","110303","110304","110305","110306","110307","110308","110309","110399","460100","460101","460102","460103","460104","460105","460106","460107","460108","460199","500100","500101","500102","500103","500104","500199"]}},{"code":"Alder","selection":{"filter":"vs:AlleAldre00B","values":[]}},{"code":"Tid","selection":{"filter":"item","values":["2020"]}}],"response":{"format":"json-stat2"}}

    res = requests.post(POST_URL, json=json_q)
    ssb_data = pyjstat.Dataset.read(res.text)

    df = ssb_data.write('dataframe')
    df_id = ssb_data.write('dataframe', naming='id')
    df['region_id'] = df_id['Region']

    return df

def get_population_counties():
    ##
    # Get the population data for Norwegian countiues
    #

    POST_URL = 'https://data.ssb.no/api/v0/no/table/07459'

    json_q = {"query":[{"code":"Region","selection":{"filter":"agg:KommFylker","values":["F-30","F-03","F-34","F-38","F-42","F-11","F-46","F-15","F-50","F-18","F-54","F-21","F-22","F-23"]}},{"code":"Alder","selection":{"filter":"vs:AlleAldre00B","values":[]}}],"response":{"format":"json-stat2"}}

    res = requests.post(POST_URL, json=json_q)
    ssb_data = pyjstat.Dataset.read(res.text)

    df = ssb_data.write('dataframe')
    df_id = ssb_data.write('dataframe', naming='id')
    df['region_id'] = df_id['Region']

    return df

def get_population_municipalities():
    ##
    # Get the population data for Norwegian municipalities
    #

    POST_URL = 'https://data.ssb.no/api/v0/no/table/07459'

    json_q = {"query":[{"code":"Region","selection":{"filter":"agg:KommSummer","values":["K-3001","K-3002","K-3003","K-3004","K-3005","K-3006","K-3007","K-3011","K-3012","K-3013","K-3014","K-3015","K-3016","K-3017","K-3018","K-3019","K-3020","K-3021","K-3022","K-3023","K-3024","K-3025","K-3026","K-3027","K-3028","K-3029","K-3030","K-3031","K-3032","K-3033","K-3034","K-3035","K-3036","K-3037","K-3038","K-3039","K-3040","K-3041","K-3042","K-3043","K-3044","K-3045","K-3046","K-3047","K-3048","K-3049","K-3050","K-3051","K-3052","K-3053","K-3054","K-0301","K-3401","K-3403","K-3405","K-3407","K-3411","K-3412","K-3413","K-3414","K-3415","K-3416","K-3417","K-3418","K-3419","K-3420","K-3421","K-3422","K-3423","K-3424","K-3425","K-3426","K-3427","K-3428","K-3429","K-3430","K-3431","K-3432","K-3433","K-3434","K-3435","K-3436","K-3437","K-3438","K-3439","K-3440","K-3441","K-3442","K-3443","K-3446","K-3447","K-3448","K-3449","K-3450","K-3451","K-3452","K-3453","K-3454","K-3801","K-3802","K-3803","K-3804","K-3805","K-3806","K-3807","K-3808","K-3811","K-3812","K-3813","K-3814","K-3815","K-3816","K-3817","K-3818","K-3819","K-3820","K-3821","K-3822","K-3823","K-3824","K-3825","K-4201","K-4202","K-4203","K-4204","K-4205","K-4206","K-4207","K-4211","K-4212","K-4213","K-4214","K-4215","K-4216","K-4217","K-4218","K-4219","K-4220","K-4221","K-4222","K-4223","K-4224","K-4225","K-4226","K-4227","K-4228","K-1101","K-1103","K-1106","K-1108","K-1111","K-1112","K-1114","K-1119","K-1120","K-1121","K-1122","K-1124","K-1127","K-1130","K-1133","K-1134","K-1135","K-1144","K-1145","K-1146","K-1149","K-1151","K-1160","K-4601","K-4602","K-4611","K-4612","K-4613","K-4614","K-4615","K-4616","K-4617","K-4618","K-4619","K-4620","K-4621","K-4622","K-4623","K-4624","K-4625","K-4626","K-4627","K-4628","K-4629","K-4630","K-4631","K-4632","K-4633","K-4634","K-4635","K-4636","K-4637","K-4638","K-4639","K-4640","K-4641","K-4642","K-4643","K-4644","K-4645","K-4646","K-4647","K-4648","K-4649","K-4650","K-4651","K-1505","K-1506","K-1507","K-1511","K-1514","K-1515","K-1516","K-1517","K-1520","K-1525","K-1528","K-1531","K-1532","K-1535","K-1539","K-1547","K-1554","K-1557","K-1560","K-1563","K-1566","K-1573","K-1576","K-1577","K-1578","K-1579","K-5001","K-5006","K-5007","K-5014","K-5020","K-5021","K-5022","K-5025","K-5026","K-5027","K-5028","K-5029","K-5031","K-5032","K-5033","K-5034","K-5035","K-5036","K-5037","K-5038","K-5041","K-5042","K-5043","K-5044","K-5045","K-5046","K-5047","K-5049","K-5052","K-5053","K-5054","K-5055","K-5056","K-5057","K-5058","K-5059","K-5060","K-5061","K-1804","K-1806","K-1811","K-1812","K-1813","K-1815","K-1816","K-1818","K-1820","K-1822","K-1824","K-1825","K-1826","K-1827","K-1828","K-1832","K-1833","K-1834","K-1835","K-1836","K-1837","K-1838","K-1839","K-1840","K-1841","K-1845","K-1848","K-1851","K-1853","K-1856","K-1857","K-1859","K-1860","K-1865","K-1866","K-1867","K-1868","K-1870","K-1871","K-1874","K-1875","K-5401","K-5402","K-5403","K-5404","K-5405","K-5406","K-5411","K-5412","K-5413","K-5414","K-5415","K-5416","K-5417","K-5418","K-5419","K-5420","K-5421","K-5422","K-5423","K-5424","K-5425","K-5426","K-5427","K-5428","K-5429","K-5430","K-5432","K-5433","K-5434","K-5435","K-5436","K-5437","K-5438","K-5439","K-5440","K-5441","K-5442","K-5443","K-5444","K-21-22","K-23","K-Rest"]}},{"code":"Alder","selection":{"filter":"vs:AlleAldre00B","values":[]}},{"code":"Tid","selection":{"filter":"item","values":["2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020"]}}],"response":{"format":"json-stat2"}}

    res = requests.post(POST_URL, json=json_q)
    ssb_data = pyjstat.Dataset.read(res.text)

    df = ssb_data.write('dataframe')
    df_id = ssb_data.write('dataframe', naming='id')
    df['region_id'] = df_id['Region']

    return df

def normalize_dataframe(df):
    ##
    # Normalizes dataframes to common format
    #

    _df = df.copy()

    _df["region"] = _df.region.str.strip()
    _df["region_id"] = _df.region_id.str.strip()
    _df["ssb_id"] = _df.region_id
    _df["ssb"] = _df.region

    _df.region_id = _df.region_id.replace({
        "K-": "",
        "F-": "",
        "aa": "",
        "a": ""
    }, regex=True)

    _df.region = _df.region.replace({
        'Trøndelag - Trööndelage': 'Trøndelag',
        'Troms og Finnmark - Romsa ja Finnmárku': 'Troms og Finnmark',
        'Unjárga - Nesseby': 'Nesseby',
        'Deatnu - Tana': 'Tana',
        'Kárásjohka - Karasjok': 'Karasjok',
        'Porsanger - Porsángu - Porsanki': 'Porsanger',
        'Guovdageaidnu - Kautokeino': 'Kautokeino',
        'Gáivuotna - Kåfjord - Kaivuono': 'Kåfjord',
        'Gáivuotna - Kåfjord' : 'Kåfjord',
        'Storfjord - Omasvuotna - Omasvuono': 'Storfjord',
        'Loabák - Lavangen': 'Lavangen',
        'Raarvihke - Røyrvik': 'Røyrvik',
        'Snåase - Snåsa': 'Snåsa',
        'Aarborte - Hattfjelldal': 'Hattfjelldal',
        'Fauske - Fuossko': 'Fauske',
        'Hamarøy - Hábmer': 'Hamarøy',
        'Divtasvuodna - Tysfjord': 'Tysfjord',
        'Divtasvuodna Tysfjord': 'Tysfjord',
        'Sortland - Suortá': 'Sortland',
        'Harstad - Hárstták': 'Harstad',
        'Nordreisa - Ráisa - Raisi': 'Nordreisa',
        'Deatnu-Tana': 'Tana',
        'Unjárga-Nesseby': 'Nesseby'
    })

    _df["district_id"] =  _df.region_id  if len(_df.region_id[0]) == 6 else ""
    _df["district"] = _df.region  if len(_df.region_id[0]) == 6 else ""
    _df["municipality_id"] = _df.region_id.str[0:4] if (len(_df.region_id[0]) > 2)  else ""
    _df["municipality"] = _df.region  if len(_df.region_id[0]) == 4 else ""
    _df["county_id"] = _df.region_id.str[0:2]
    _df["county"] =  _df.region  if len(_df.region_id[0]) == 2 else ""

    # Fix Rest for county_id and Jan Mayen/Svalbard combined
    _df.loc[_df.region_id == "21-22", "municipality_id"] = "21-22"
    _df.loc[_df.region_id == "21-22", "county_id"] = ""
    _df.loc[_df.region_id == "Rest", "county_id"] = "Rest"
    _df.loc[_df.region_id == "Rest", "county"] = "Delte kommuner og uoppgitt"

    _df = _df[["region_id", "region", "ssb_id", "ssb", "district_id", "district", "municipality_id", "municipality", "county_id", "county", "år", "value"]]
    _df.columns = ["region_id", "region", "ssb_id", "ssb", "district_id", "district", "municipality_id", "municipality", "county_id", "county", "year", "population"]

    # Correct datatypes
    _df = _df.astype(str)
    _df[["year", "population"]] = _df[["year", "population"]].astype(int)

    return _df

def get_population():
    ##
    # Get population from different regions and combine them to one dataframe
    # Fill inn missing names for counties and municipalities
    #

    df_counties = normalize_dataframe(get_population_counties())
    df_municipalities = normalize_dataframe(get_population_municipalities())
    df_districts = normalize_dataframe(get_population_districts())

    # Append all three to one dataframe
    df = df_districts.append(df_municipalities, ignore_index=True)
    df = df.append(df_counties, ignore_index=True)

    df_unique_counties = df[df.county != ""][["county_id", "county"]].drop_duplicates().copy()
    df_unique_municipalities = df[df.municipality != ""][["municipality_id", "municipality"]].drop_duplicates().copy()

    df = df.merge(df_unique_municipalities, on="municipality_id", how="left")
    df = df.drop("municipality_x", axis=1).rename(columns={'municipality_y':'municipality'})
    df = df.merge(df_unique_counties, on="county_id", how="left")
    df = df.drop("county_x", axis=1).rename(columns={'county_y':'county'})
    df = df[["region_id", "region", "ssb_id", "ssb", "district_id", "district", "municipality_id", "municipality", "county_id", "county", "year", "population"]]

    return df

##
# Save the dataframe in different versions and filetypes
#

df = get_population()

df.to_csv(os.path.join(PROJECT_DIRECTORY, "data/csv/", "norwegian_population.csv"), index=False)
df.to_pickle(os.path.join(PROJECT_DIRECTORY, "data/pkl/", "norwegian_population.pkl"))

filename = "norwegian_population_" + str(YEAR)
df[df.year == YEAR].to_csv(os.path.join(PROJECT_DIRECTORY, "data/csv/", filename + ".csv"), index=False)
df[df.year == YEAR].to_pickle(os.path.join(PROJECT_DIRECTORY, "data/pkl/", filename + ".pkl"))

df = df[df.year == YEAR].drop(["year", "population"], axis=1)
df.to_csv(os.path.join(PROJECT_DIRECTORY, "data/csv/", "norwegian_regions.csv"), index=False)
df.to_pickle(os.path.join(PROJECT_DIRECTORY, "data/pkl/", "norwegian_regions.pkl"))
