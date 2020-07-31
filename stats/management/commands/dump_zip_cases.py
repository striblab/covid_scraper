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

    OUTFILE = os.path.join(settings.BASE_DIR, 'exports', 'mn_zip_timeseries.csv')
    OUTGEOJSON = os.path.join(settings.BASE_DIR, 'exports', 'mn_zcta_with_data.geojson')
    OUTMBTILES = os.path.join(settings.BASE_DIR, 'exports', 'mn_zcta_with_data.mbtiles')

    def handle(self, *args, **options):
        print('Retriving DB case data...')
        zips_df = pd.DataFrame(list(ZipCasesDate.objects.all().order_by('data_date', 'zip').values()))

        # Temp set -1 values to None to do difference calculations correctly (igoring cases where we can't calc)
        zips_df['cases_cumulative_calc'] = zips_df['cases_cumulative'].replace(-1, np.nan)
        zips_df['cases_weekly_change'] = zips_df.groupby('zip')['cases_cumulative_calc'].transform(lambda x: x.diff())
        zips_df['cases_weekly_pct_chg'] = zips_df.groupby('zip')['cases_weekly_change'].transform(lambda x: x.rolling(7, 1).mean()).round(1)

        zips_df.drop(columns=['id', 'import_date'])

        print('Merging with ACS demographics...')
        demo_df = pd.read_csv(self.DEMOGRAPHICS_FILE, dtype={'zip': object})
        zips_df_merged = zips_df.merge(demo_df, how="left", on="zip")
        zips_df_merged['pct_nonwhite'] = zips_df_merged['pct_nonwhite'].round(3)
        zips_df_merged['cases_weekly_change'] = zips_df_merged['cases_weekly_change'].fillna(-1)
        zips_df_merged['cases_per_1k'] = (zips_df_merged['cases_cumulative_calc'] / (zips_df_merged['pop_total'] / 1000)).round(1).fillna(-1)
        zips_df_merged.rename(columns={'cases_cumulative': 'cases_total'}, inplace=True)
        # print(zips_df_merged)

        print('Exporting CSV...')
        zips_df_merged[[
            'data_date',
            'zip',
            'cases_total',
            'cases_per_1k',
            'cases_weekly_change',
            # 'cases_weekly_change_rolling',
            'pop_total',
            'pct_nonwhite'
        ]].to_csv(self.OUTFILE, index=False)

        # Massage into json timeseries
        # zips = zips_df_merged['zip'].unique()
        #
        # zip_records = {}
        # for zip in zips:
        #     zip_dates = zips_df_merged[zips_df_merged['zip'] == zip]
        #     dates = {d['data_date'].strftime('%Y-%m-%d'): {
        #         'cases_total': d['cases_cumulative'],
        #         'cases_per_1k': d['cases_per_1k'],
        #         'cases_weekly_change': d['cases_weekly_change']
        #     } for d in zip_dates.to_dict('records')}
        #     zip_records[zip] = {
        #         'pop_total': zip_dates.pop_total.iloc[0],
        #         'pct_nonwhite': round(zip_dates.pct_nonwhite.iloc[0], 3),
        #         'dates': dates
        #     }

        print('Merging with spatial data...')
        max_data_date = zips_df_merged.data_date.max()

        # Do this the geopandas way
        zcta_boundaries = gpd.read_file('covid_scraper/data/shp/zip_code_tabulation_areas_4326/zip_code_tabulation_areas_4326.shp', dtype={'ZCTA5CE10': str})

        mn_zcta_with_data = zcta_boundaries[['ZCTA5CE10', 'geometry']].merge(
            zips_df_merged[zips_df_merged['data_date'] == max_data_date],
            how="left",
            left_on="ZCTA5CE10",
            right_on="zip"
        )

        mn_zcta_with_data = mn_zcta_with_data[(~mn_zcta_with_data['data_date'].isna()) | (~mn_zcta_with_data['pop_total'].isna())]
        mn_zcta_with_data['data_date'] = pd.to_datetime(mn_zcta_with_data['data_date']).dt.strftime('%Y-%m-%d')

        print('Exporting GeoJSON...')
        mn_zcta_with_data.to_file(self.OUTGEOJSON, driver='GeoJSON')

        print('Exporting MBTiles...')
        os.system('tippecanoe -o {} -Z 4 -z 13 {} --force'.format(self.OUTMBTILES, self.OUTGEOJSON))
