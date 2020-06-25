import os
import csv
import requests
import pandas as pd
import numpy as np

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Generate a CSV of US County 2019 estimated populations for joining to other data. Subtract counties or portions of counties that are tallied with city areas (New York City and Kansas City)'

    CENSUS_POP_ESTIMATES_LOCAL = os.path.join(settings.BASE_DIR, 'data', 'co-est2019-alldata.csv')

    POP_ESTIMATES_EXPORTS_PATH = os.path.join(settings.BASE_DIR, 'data', 'county_pops_2019.csv')

    EXTRANEOUS_GEOGRAPHIES = [
        {
            'state_name': 'New York',
            'misc_geo_name': 'New York City',
            'fake_fips': 'nyc',
            'pop_estimate_2019': 8336817,
            'counties_to_deduct': [
                {'fips': '36061', 'name': 'New York', 'pct': 1},
                {'fips': '36047', 'name': 'Kings', 'pct': 1},
                {'fips': '36081', 'name': 'Queens', 'pct': 1},
                {'fips': '36005', 'name': 'Bronx', 'pct': 1},
                {'fips': '36085', 'name': 'Richmond', 'pct': 1},
            ]
        }, {
            'state_name': 'Missouri',
            'misc_geo_name': 'Kansas City',
            'fake_fips': 'mci',
            'pop_estimate_2019': 498558, # 491,918
            'counties_to_deduct': [  # From 2018 Subcounty Resident Population Estimates (https://www.census.gov/data/tables/time-series/demo/popest/2010s-total-cities-and-towns.html)
                {'fips': '29037', 'name': 'Cass', 'pct': 0.0019}, # 'pop': 201}, co: 104954
                {'fips': '29047', 'name': 'Clay', 'pct': 0.5133}, # 'pop': 126460}, co: 246365
                {'fips': '29095', 'name': 'Jackson', 'pct': 0.4509}, # 'pop': 315801}, co: 700307
                {'fips': '29165', 'name': 'Platte', 'pct': 0.4802}, # 'pop': 49456} co: 102985
            ]
        }
    ]

    def handle(self, *args, **options):
        pop_estimates_all_df = pd.read_csv(self.CENSUS_POP_ESTIMATES_LOCAL, dtype={'STATE': object, 'COUNTY': object}, encoding='latin-1')

        county_pops_df = pop_estimates_all_df.loc[pop_estimates_all_df['SUMLEV'] == 50][[
            'STATE',
            'COUNTY',
            'STNAME',
            'CTYNAME',
            'POPESTIMATE2019',
        ]]
        county_pops_df['full_fips'] = county_pops_df['STATE'] + county_pops_df['COUNTY']
        print(county_pops_df)

        # total_nyc_pop = county_pops_df.loc[(county_pops_df['STNAME'] == 'New York') & (county_pops_df['CTYNAME'].isin(['New York County', 'Kings County', 'Queens County', 'Bronx County', 'Richmond County']))]
        # print(total_nyc_pop['POPESTIMATE2019'].sum())
        county_pops_df = county_pops_df.rename(columns={'STNAME': 'state_name', 'CTYNAME': 'county_name'})

        # For weird counties, reduce populations to get rid of cities that are counted separately (mostly KC)
        for g in self.EXTRANEOUS_GEOGRAPHIES:
            for cd in g['counties_to_deduct']:
                county_pops_df.loc[
                    county_pops_df['full_fips'] == cd['fips'], 'fixed_pop'\
                ] = county_pops_df.loc[
                    county_pops_df['full_fips'] == cd['fips'], 'POPESTIMATE2019'
                ].apply(lambda x: int(x * (1 - cd['pct'])))

        print(county_pops_df[county_pops_df['fixed_pop'] > 0])

        # Coalesce to use amended pop if present
        county_pops_df['pop_2019'] = county_pops_df.fixed_pop.combine_first(county_pops_df.POPESTIMATE2019)

        # Add populations for the weird cities themselves
        county_pops_df = county_pops_df.append(pd.DataFrame([{
            'state_name': g['state_name'],
            'county_name': g['misc_geo_name'],
            'full_fips': g['fake_fips'],
            'pop_2019': g['pop_estimate_2019']
        } for g in self.EXTRANEOUS_GEOGRAPHIES]))

        county_pops_out_df = county_pops_df[[
            'state_name',
            'county_name',
            'full_fips',
            'pop_2019',
        ]]

        print(county_pops_out_df)
        county_pops_out_df.to_csv(self.POP_ESTIMATES_EXPORTS_PATH, index=False)
