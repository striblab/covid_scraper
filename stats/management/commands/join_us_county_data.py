import os
import csv
import geojson
import requests
import pandas as pd
import numpy as np
# import datetime
from datetime import datetime, timedelta

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
    LATEST_JSON_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'national_cases_deaths_by_county_latest.json')
    MIDWEST_EMERGING_COUNTIES_PATH = os.path.join(settings.BASE_DIR, 'exports', 'midwest_emerging_counties.csv')
    MIDWEST_EMERGING_COUNTIES_WIDE_PATH = os.path.join(settings.BASE_DIR, 'exports', 'midwest_emerging_counties_wide.csv')

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
        print('Building emerging hotspots data ...')

        target_states_df = df[df['state'].isin(['Minnesota', 'Wisconsin', 'Iowa', 'North Dakota', 'South Dakota', 'Nebraska', 'Illinois', 'Michigan', 'Indiana'])]

        counties = target_states_df['fips'].unique()
        out_df = pd.DataFrame()

        df['date'] = pd.to_datetime(df['date'])

        for c in counties:
            county_timeseries = df[df['fips'] == c].sort_values('date')

            total_cases = county_timeseries['cases'].max()
            if total_cases > 20:

                county_timeseries['cases_daily_pct_change'] = county_timeseries['cases'].pct_change() * 100
                county_timeseries['deaths_daily_pct_change'] = county_timeseries['deaths'].pct_change() * 100

                county_max_date = county_timeseries['date'].max()
                county_last_7 = county_timeseries[county_timeseries['date'] > county_max_date - timedelta(days=7)]

                # print(county_last_7['cases_daily_pct_change'])
                county_cases_weekly_pct_change = county_last_7[county_last_7['cases_daily_pct_change'] != np.inf]['cases_daily_pct_change'].mean()
                county_deaths_weekly_pct_change = county_last_7[county_last_7['deaths_daily_pct_change'] != np.inf]['deaths_daily_pct_change'].mean()

                county_timeseries['cases_weekly_pct_change'] = county_cases_weekly_pct_change
                county_timeseries['deaths_weekly_pct_change'] = county_deaths_weekly_pct_change

                out_df = out_df.append(county_timeseries)

        out_df = out_df.sort_values('cases_weekly_pct_change', ascending=False)

        worst_100_cutoff = out_df['cases_weekly_pct_change'].unique()[:100]
        worst_100_df = out_df[out_df['cases_weekly_pct_change'] >= min(worst_100_cutoff)]

        worst_100_df['date_only'] = worst_100_df['date'].dt.date

        # Add image links
        worst_100_df['map'] = worst_100_df['fips'].apply(lambda x: '![](https://static.startribune.com/news/projects/all/2020-covid-scraper/locator_maps/county_{}.png)'.format(x))

        # Floats to int
        # worst_100_df.cases = worst_100_df.cases.astype(int)
        # print(worst_100_df.cases.dtype)
        postal_codes = pd.DataFrame([
            {'state': 'Minnesota', 'abbrev': 'MN'},
            {'state': 'Wisconsin', 'abbrev': 'WI'},
            {'state': 'Iowa', 'abbrev': 'IA'},
            {'state': 'Illinois', 'abbrev': 'IL'},
            {'state': 'Michigan', 'abbrev': 'MI'},
            {'state': 'North Dakota', 'abbrev': 'ND'},
            {'state': 'South Dakota', 'abbrev': 'SD'},
            {'state': 'Nebraska', 'abbrev': 'NE'},
            {'state': 'Indiana', 'abbrev': 'IN'},
        ])

        worst_100_df = worst_100_df.merge(
            postal_codes,
            how="left",
            on='state'
        )
        worst_100_df['county_state'] = worst_100_df[['county', 'abbrev']].apply(lambda x: ' Co, '.join(x[x.notnull()]), axis = 1)

        # TODO: Clip to last two weeks of data
        # worst_100_df = worst_100_df[worst_100_df['date'] >= (datetime.today() - timedelta(days=15))]
        worst_100_df.to_csv(self.MIDWEST_EMERGING_COUNTIES_PATH, index=False)

        # Now go wide...

        worst_100_pivot = worst_100_df[worst_100_df['date'] >= '2020-03-01'][[
            'fips',
            'date_only',
            'cases'
        ]].pivot(
            index='fips',
            columns='date_only',
            values='cases'
        ).reset_index().add_prefix('cases_')
        worst_100_pivot = worst_100_pivot.rename(columns={'cases_fips': 'fips'})
        # print(worst_100_pivot)

        # case_cols = [col for col in worst_100_pivot.columns if 'cases_' in col]
        # worst_100_pivot[case_cols] = worst_100_pivot[case_cols].fillna(pd.NA)
        # worst_100_pivot[case_cols] = worst_100_pivot[case_cols].astype(int)

        latest_dates = worst_100_df[['fips', 'date']].groupby(['fips']).agg({'date': 'max'})
        worst_100_max_df = worst_100_df.merge(
            latest_dates,
            how="right",
            on=['fips', 'date']
        )

        worst_100_pivot = worst_100_max_df.merge(
            worst_100_pivot,
            on='fips'
        ).drop(columns={'date_only'})

        # print(worst_100_pivot)
        worst_100_pivot.to_csv(self.MIDWEST_EMERGING_COUNTIES_WIDE_PATH, index=False)

    def df_to_geojson(self, df):
        features = []
        insert_features = lambda x: features.append(
            geojson.Feature(geometry=geojson.Point((x["longitude"],
                                                    x["latitude"])),
                            properties={k: v for k, v in x.items() if k not in ['longtitude', 'latitude']}
                        ))
        df.dropna().apply(insert_features, axis=1)

        with open(self.LATEST_JSON_EXPORT_PATH, 'w', encoding='utf8') as fp:
            geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True, ensure_ascii=False)

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
                'STATEFP',
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
            df_subset.rename(columns={'STATEFP': 'state_fips', 'latitude_coalesced': 'latitude', 'longitude_coalesced': 'longitude'}, inplace=True)

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
            # print(df_bystate_100_plus)
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
            out_df = latest_df[[
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
            ]]

            out_df.to_csv(self.LATEST_EXPORT_PATH, index=False)
            self.df_to_geojson(out_df)

            # with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_statewide_timeseries.json'), 'w') as jsonfile:
            #     jsonfile.write(json.dumps(rows))

            self.build_emerging_counties(df_subset)
