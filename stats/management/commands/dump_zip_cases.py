import re
import os
import json
import datetime
import pandas as pd
import geopandas as gpd
import numpy as np

from django.conf import settings
from django.core.management.base import BaseCommand

from stats.models import ZipCasesDate


class Command(BaseCommand):
    help = 'Load weekly cases by zip code spreadsheet from MDH. This is very basic, and most tabulation is handled on dumping.'

    DEMOGRAPHICS_FILE = os.path.join(settings.BASE_DIR, 'data', 'acs_2018_5yr_zip_demo.csv')

    OUTFILE = os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_zip_timeseries.csv')

    def handle(self, *args, **options):
        print('Retriving DB case data...')
        zips_df = pd.DataFrame(list(ZipCasesDate.objects.all().order_by('data_date', 'zip').values()))

        # Temp set -1 values to None to do difference calculations correctly (igoring cases where we can't calc)
        zips_df['cases_cumulative_calc'] = zips_df['cases_cumulative'].replace(-1, np.nan)
        zips_df['cases_weekly_change'] = zips_df.groupby('zip')['cases_cumulative_calc'].transform(lambda x: x.diff())
        zips_df['cases_weekly_pct_chg'] = (zips_df['cases_weekly_change'] / (zips_df['cases_cumulative_calc'] - zips_df['cases_weekly_change'])).round(3)

        zips_df.drop(columns=['id', 'import_date'])

        print('Merging with ACS demographics...')
        demo_df = pd.read_csv(self.DEMOGRAPHICS_FILE, dtype={'zip': object, 'bool_metro': bool})
        zips_df_merged = zips_df.merge(demo_df, how="left", on="zip")
        zips_df_merged['pct_nonwhite'] = zips_df_merged['pct_nonwhite'].round(3)
        zips_df_merged['pct_black'] = zips_df_merged['pct_black'].round(3)
        zips_df_merged['pct_latinx'] = zips_df_merged['pct_latinx'].round(3)
        zips_df_merged['pct_asian'] = zips_df_merged['pct_asian'].round(3)
        zips_df_merged['pct_nativeamer'] = zips_df_merged['pct_nativeamer'].round(3)
        zips_df_merged['cases_weekly_change'] = zips_df_merged['cases_weekly_change']
        zips_df_merged['cases_per_1k'] = (zips_df_merged['cases_cumulative_calc'] / (zips_df_merged['pop_total'] / 1000)).round(1)
        zips_df_merged['cases_weekly_per_1k'] = (zips_df_merged['cases_weekly_change'] / (zips_df_merged['pop_total'] / 1000)).round(1)
        zips_df_merged.rename(columns={'cases_cumulative': 'cases_total'}, inplace=True)

        print('Filtering out zips with 0 population...')
        zips_df_merged = zips_df_merged[
            (zips_df_merged['pop_total'] > 0)
            | (zips_df_merged['zip'] == 'Missing')
        ]

        print('Exporting CSV...')
        zips_df_merged[[
            'data_date',
            'zip',
            'cases_total',
            'cases_per_1k',
            'cases_weekly_change',
            'cases_weekly_per_1k',
            'cases_weekly_pct_chg',
            'pop_total',
            'pct_nonwhite',
            'pct_black',
            'pct_latinx',
            'pct_asian',
            'pct_nativeamer',
            'largest_minority',
            'bool_metro',
        ]].to_csv(self.OUTFILE, index=False)
