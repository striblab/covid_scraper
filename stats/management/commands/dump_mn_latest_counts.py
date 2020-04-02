import os
import csv

from django.conf import settings

from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'Dump a CSV of the latest cumulative count of statewide and county-by-county data.'

    def dump_county_latest(self):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_positive_tests_by_county.csv'), 'w') as csvfile:
            # fieldnames = ['first_name', 'last_name']
            fieldnames = ['county_fips', 'county_name', 'total_positive_tests', 'total_deaths', 'latitude', 'longitude']
            # COUNTY ID	COUNTY	COUNTA of COUNTY ID	COUNTUNIQUE of COMMUNITY TRANSMISSION	COUNTA of FATALITIES	MAX of LAT	MAX of LONG
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            msg_output = '*Latest numbers from MPH:*\n\n'

            updated_total = 0

            for c in County.objects.all().order_by('name'):
                latest_observation = CountyTestDate.objects.filter(county=c).order_by('-scrape_date').first()
                if latest_observation:

                    updated_total += latest_observation.cumulative_count

                    row = {
                        'county_fips': c.fips,
                        'county_name': c.name,
                        'total_positive_tests': latest_observation.cumulative_count,
                        'total_deaths': latest_observation.cumulative_deaths,
                        'latitude': c.latitude,
                        'longitude': c.longitude,
                    }

                    writer.writerow(row)


    def dump_state_latest(self):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_latest.csv'), 'w') as csvfile:
            # fieldnames = ['first_name', 'last_name']
            fieldnames = ['total_positive_tests', 'total_completed_tests', 'total_completed_mdh', 'total_completed_private', 'total_hospitalized', 'currently_hospitalized', 'currently_in_icu', 'total_statewide_deaths', 'total_statewide_recoveries', 'last_update']

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            latest = StatewideTotalDate.objects.all().order_by('-last_update').first()
            writer.writerow({
                'total_positive_tests': latest.cumulative_positive_tests,
                'total_completed_tests': latest.cumulative_completed_tests,
                'total_completed_mdh': latest.cumulative_completed_mdh,
                'total_completed_private': latest.cumulative_completed_private,
                'total_hospitalized': latest.cumulative_hospitalized,
                'currently_hospitalized': latest.currently_hospitalized,
                'currently_in_icu': latest.currently_in_icu,
                'total_statewide_deaths': latest.cumulative_statewide_deaths,
                'total_statewide_recoveries': latest.cumulative_statewide_recoveries,
                'last_update': latest.last_update,
            })

    def handle(self, *args, **options):
        self.dump_county_latest()
        self.dump_state_latest()
