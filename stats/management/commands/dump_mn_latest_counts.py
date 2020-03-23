import os
import csv

from django.conf import settings

from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'Check for new or updated results by date from Minnesota Department of Health table: https://www.health.state.mn.us/diseases/coronavirus/situation.html'


    def handle(self, *args, **options):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_positive_tests_by_county.csv'), 'w') as csvfile:
            # fieldnames = ['first_name', 'last_name']
            fieldnames = ['county_fips', 'county_name', 'total_positive_tests', 'latitude', 'longitude']
            # COUNTY ID	COUNTY	COUNTA of COUNTY ID	COUNTUNIQUE of COMMUNITY TRANSMISSION	COUNTA of FATALITIES	MAX of LAT	MAX of LONG
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for c in County.objects.all().order_by('name'):
                latest_observation = CountyTestDate.objects.filter(county=c).order_by('-scrape_date').first()
                if latest_observation:
                    row = {
                        'county_fips': c.fips,
                        'county_name': c.name,
                        'total_positive_tests': latest_observation.case_count,
                        'latitude': c.latitude,
                        'longitude': c.longitude,
                    }

                    writer.writerow(row)
