import os
import csv
import requests
import pandas as pd
import numpy as np

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Grab the latest NYT timeseries data (https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv) and join it to get centroids of counties (with some extra cities)'

    NYT_COUNTY_TIMESERIES_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    NYT_COUNTY_TIMESERIES_LOCAL = os.path.join(settings.BASE_DIR, 'data', 'nyt-us-counties.csv')
    COUNTY_CENTROIDS_PATH = os.path.join(settings.BASE_DIR, 'data', 'us_county_centroids_tl_2019_4326.csv')
    POP_ESTIMATES_PATH = os.path.join(settings.BASE_DIR, 'data', 'county_pops_2019.csv')

    STATE_TIMESERIES_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'state_cases_from_100_timeseries.csv')
    TIMESERIES_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'national_cases_deaths_by_county_timeseries.csv')
    LATEST_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'national_cases_deaths_by_county_latest.csv')

    EXTRANEOUS_GEOGRAPHIES = [
        {
            'misc_geo_name': 'New York City',
            'fake_fips': 'nyc',
            'latitude': 40.7128,
            'longitude': -74.0060
        }, {
            'misc_geo_name': 'Kansas City',
            'fake_fips': 'mci',
            'latitude': 39.0997,
            'longitude': -94.5786
        }, {
            'misc_geo_name': 'Doña Ana',
            'fake_fips': '35013',
            'latitude': 32.4858,
            'longitude': -106.7235
        }
    ]

    def download_nyt_data(self):
        print('Downloading NYT timeseries csv...')

        r = requests.get(self.NYT_COUNTY_TIMESERIES_URL)

        with open(self.NYT_COUNTY_TIMESERIES_LOCAL, 'wb') as f:
            f.write(r.content)
            return self.NYT_COUNTY_TIMESERIES_LOCAL
        return False

    def build_emerging_counties(self, df):
        '''Output last 2 weeks of data for counties with the highest average percent change in cases over the last week'''
        counties = df['fips'].unique()
        print(counties)
        # df['daily_pct_change'] =

    def handle(self, *args, **options):

        nyt_csv = self.download_nyt_data()
        if nyt_csv:

            nyt_timeseries_df = pd.read_csv(self.NYT_COUNTY_TIMESERIES_LOCAL, dtype={'fips': object})

            county_centroids_df = pd.read_csv(self.COUNTY_CENTROIDS_PATH, dtype={'STATEFP': object, 'COUNTYFP': object})
            county_centroids_df['full_fips'] = county_centroids_df['STATEFP'] + county_centroids_df['COUNTYFP']

            df_merged = nyt_timeseries_df.merge(
                county_centroids_df,
                how="left",
                left_on="fips",
                right_on="full_fips"
            )

            # Add lat/lng for places that aren't counties or got missed somehow else
            df_merged = df_merged.merge(
                pd.DataFrame(self.EXTRANEOUS_GEOGRAPHIES),
                how="left",
                left_on="county",
                right_on="misc_geo_name"
            )

            # Combine lat/lng, preferring our manual settings in case there are exceptions
            df_merged['latitude_coalesced'] = df_merged.latitude.combine_first(df_merged.INTPTLAT)
            df_merged['longitude_coalesced'] = df_merged.longitude.combine_first(df_merged.INTPTLON)

            df_merged['latitude_coalesced'] = df_merged['latitude_coalesced'].round(4)
            df_merged['longitude_coalesced'] = df_merged['longitude_coalesced'].round(4)
            df_merged['fips'] = df_merged.fips.combine_first(df_merged.fake_fips)

            # Join to population data created by build_us_county_populations.py (which needs to be run manually one time)
            county_pops_df = pd.read_csv(self.POP_ESTIMATES_PATH, dtype={'full_fips': object})
            df_merged = df_merged.merge(
                county_pops_df,
                how="left",
                left_on="fips",
                right_on="full_fips"
            )

            df_merged['cases_p_100k'] = round(100000 * (df_merged['cases'] / df_merged['pop_2019']), 1)
            df_merged['deaths_p_100k'] = round(100000 * (df_merged['deaths'] / df_merged['pop_2019']), 1)

            df_subset = df_merged[[
                'date',
                'fips',
                'state',
                'county',
                'cases',
                'deaths',
                'pop_2019',
                'cases_p_100k',
                'deaths_p_100k',
                'latitude_coalesced',
                'longitude_coalesced',
            ]]
            df_subset.rename(columns={'latitude_coalesced': 'latitude', 'longitude_coalesced': 'longitude'}, inplace=True)

            print('Making state timeseries...')

            df_bystate = df_subset[[
                'date',
                'state',
                'cases',
                'deaths',
                # 'pop_2019'
            ]].groupby(['state', 'date']).agg('sum').reset_index()

            # Can't do state per capita this way because not all counties are included
            # df_bystate['cases_p_100k'] = round(100000 * (df_bystate['cases'] / df_bystate['pop_2019']), 3)
            # df_bystate['deaths_p_100k'] = round(100000 * (df_bystate['deaths'] / df_bystate['pop_2019']), 3)

            df_bystate_100_plus = df_bystate[df_bystate['cases'] >= 100].sort_values(['state', 'date'])
            df_bystate_100_plus['day_counter'] = df_bystate_100_plus.groupby(['state']).cumcount()
            print(df_bystate_100_plus)
            df_bystate_100_plus.to_csv(self.STATE_TIMESERIES_EXPORT_PATH, index=False)

            print('Exporting national timeseries...')
            df_subset.to_csv(self.TIMESERIES_EXPORT_PATH, index=False)

            # Now let's get the latest date for each county
            latest_dates = df_subset[['county', 'date']].groupby(['county']).agg({'date': 'max'})
            latest_df = latest_dates.merge(
                df_subset,
                how="left",
                on=['county', 'date']
            )

            print('Exporting latest observations...')
            latest_df[[
                'fips',
                'state',
                'county',
                'cases',
                'deaths',
                'pop_2019',
                'cases_p_100k',
                'deaths_p_100k',
                'latitude',
                'longitude',
                'date',
            ]].to_csv(self.LATEST_EXPORT_PATH, index=False)

            self.build_emerging_counties(df_subset)
