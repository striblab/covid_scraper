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

    OUTGEOJSON = os.path.join(settings.BASE_DIR, 'exports', 'mn_zctas.geojson')
    OUTMBTILES = os.path.join(settings.BASE_DIR, 'exports', 'mn_zctas.mbtiles')

    CENTROID_OUTGEOJSON = os.path.join(settings.BASE_DIR, 'exports', 'mn_zctas_centroids.geojson')
    CENTROID_OUTMBTILES = os.path.join(settings.BASE_DIR, 'exports', 'mn_zctas_centroids.mbtiles')

    def handle(self, *args, **options):

        print('Loading boundary shapefile ...')
        zcta_boundaries = gpd.read_file('covid_scraper/data/shp/zip_code_tabulation_areas_4326/zip_code_tabulation_areas_4326.shp', dtype={'ZCTA5CE10': str})

        print('Loading ACS demographics...')
        demo_df = pd.read_csv(self.DEMOGRAPHICS_FILE, dtype={'zip': object, 'bool_metro': bool})
        zips_map_merged = zcta_boundaries.merge(demo_df, how="left", left_on="ZCTA5CE10", right_on="zip")

        # Drop 0 pops
        zips_map_merged = zips_map_merged[
            (~zips_map_merged['pop_total'].isna())
            & (zips_map_merged['pop_total'] > 0)
        ]

        # Temp: Drop zips not tracked by MDH
        zips_map_merged = zips_map_merged[~zips_map_merged['zip'].isin(['51360', '57030', '57026', '58225', '57068'])]

        zips_map_merged['pct_nonwhite'] = zips_map_merged['pct_nonwhite'].round(3)
        zips_map_merged['pct_black'] = zips_map_merged['pct_black'].round(3)
        zips_map_merged['pct_latinx'] = zips_map_merged['pct_latinx'].round(3)
        zips_map_merged['pct_asian'] = zips_map_merged['pct_asian'].round(3)
        zips_map_merged['pct_nativeamer'] = zips_map_merged['pct_nativeamer'].round(3)

        zips_map_merged = zips_map_merged[[
            'zip',
            'pop_total',
            'pct_nonwhite',
            'pct_black',
            'pct_latinx',
            'pct_asian',
            'pct_nativeamer',
            'largest_minority',
            'bool_metro',
            'geometry'
        ]]
        zips_map_merged['zip'] = zips_map_merged['zip'].astype(int)

        print('Exporting GeoJSON...')
        zips_map_merged.to_file(self.OUTGEOJSON, driver='GeoJSON')

        print('Exporting MBTiles...')
        os.system('tippecanoe -o {} -Z 4 -z 13 {} --force --use-attribute-for-id=zip'.format(self.OUTMBTILES, self.OUTGEOJSON))

        print('Exporting centroid GeoJSON...')
        zips_map_merged['geom_utm'] = zips_map_merged['geometry'].to_crs(26915)
        zips_map_merged['geometry'] = zips_map_merged['geom_utm'].centroid.to_crs(4326)
        zips_map_merged.drop(columns=['geom_utm'], inplace=True)
        zips_map_merged.to_file(self.CENTROID_OUTGEOJSON, driver='GeoJSON')

        print('Exporting centroid MBTiles...')
        os.system('tippecanoe -o {} -Z 4 -z 13 {} --force --use-attribute-for-id=zip'.format(self.CENTROID_OUTMBTILES, self.CENTROID_OUTGEOJSON))
