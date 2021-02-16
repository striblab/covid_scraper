import os
import csv

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County


class Command(BaseCommand):
    help = 'One-time import to update county data with ACS 2019 5-year'

    def handle(self, *args, **options):
        with open(os.path.join(settings.BASE_DIR, 'imports', 'acs_2019_5yr_county_pop.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                print(row['name'])
                small_fips = row['geoid'].replace('05000US', '')

                try:
                    county_match = County.objects.get(fips=small_fips)
                    # print(county_match.name)
                    county_match.pop_2019 = row['B01003001']
                    county_match.save()
                except:
                    print('WARNING: No match found:' + row['name'])
