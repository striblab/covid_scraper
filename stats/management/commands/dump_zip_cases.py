import re
import os
import json
import datetime
import pandas as pd
import numpy as np

from django.conf import settings
from django.core.management.base import BaseCommand

from stats.models import ZipCasesDate


class Command(BaseCommand):
    help = 'Load weekly cases by zip code spreadsheet from MDH. This is very basic, and most tabulation is handled on dumping.'

    OUTFILE = os.path.join(settings.BASE_DIR, 'exports', 'mn_zip_timeseries.csv')
    DEMOGRAPHICS_FILE = os.path.join(settings.BASE_DIR, 'data', 'acs_2018_5yr_zip_demo.csv')

    def handle(self, *args, **options):
        zips_df = pd.DataFrame(list(ZipCasesDate.objects.all().order_by('data_date', 'zip').values()))

        # Temp set -1 values to None to do difference calculations correctly (igoring cases where we can't calc)
        zips_df['cases_cumulative_calc'] = zips_df['cases_cumulative'].replace(-1, np.nan)
        zips_df['cases_weekly_change'] = zips_df.groupby('zip')['cases_cumulative_calc'].transform(lambda x: x.diff())
        zips_df['cases_weekly_change_rolling'] = zips_df.groupby('zip')['cases_weekly_change'].transform(lambda x: x.rolling(7, 1).mean())
        zips_df['cases_weekly_pct_chg'] = zips_df.groupby('zip')['cases_weekly_change'].transform(lambda x: x.rolling(7, 1).mean())

        # Merge with demographics
        demo_df = pd.read_csv(self.DEMOGRAPHICS_FILE, dtype={'zip': object})
        zips_df_merged = zips_df.merge(demo_df, how="left", on="zip")
        zips_df_merged['cases_per_1k'] = zips_df_merged['cases_cumulative_calc'] / (zips_df_merged['pop_total'] / 1000).round(1)
        print(zips_df_merged)

        zips_df_merged[[
            'data_date',
            'zip',
            'cases_cumulative',
            'cases_per_1k',
            'cases_weekly_change',
            'cases_weekly_change_rolling',
            'pop_total',
            'pct_nonwhite'
        ]].to_csv(self.OUTFILE, index=False)

        # Massage into json timeseries
        zips = zips_df_merged['zip'].unique()

        zip_records = {}
        for zip in zips:
            zip_dates = zips_df_merged[zips_df_merged['zip'] == zip]
            dates = {d['data_date'].strftime('%Y-%m-%d'): {
                'cases_total': d['cases_cumulative'],
                'cases_per_1k': d['cases_per_1k'],
                'cases_weekly_change': d['cases_weekly_change']
            } for d in zip_dates.to_dict('records')}
            zip_records[zip] = {
                'pop_total': zip_dates.pop_total.iloc[0],
                'pct_nonwhite': round(zip_dates.pct_nonwhite.iloc[0], 3),
                'dates': dates
            }

        # Merge with topojson spatial boundaries
        with open("covid_scraper/data/shp/mn_zcta.topojson", 'r') as topojson_file:
            zcta_topojson = json.loads(topojson_file.read())

            for zcta in zcta_topojson['objects']['data']['geometries']:
                try:
                    zcta['properties']['pop_total'] = zip_records[zcta['properties']['zip']]['pop_total']
                    zcta['properties']['pct_nonwhite'] = zip_records[zcta['properties']['zip']]['pct_nonwhite']
                    zcta['properties']['dates'] = zip_records[zcta['properties']['zip']]['dates']
                except:
                    zcta['properties']['pop_total'] = None
                    zcta['properties']['pct_nonwhite'] = None
                    zcta['properties']['dates'] = None

            with open("covid_scraper/exports/mn_zcta_with_data.topojson", 'w') as out_topojson:
                out_topojson.write(json.dumps(zcta_topojson))
                out_topojson.close()
