import os
import csv

from django.conf import settings

from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, CurrentTotal
from stats.utils import slack_latest


class Command(BaseCommand):
    help = 'Dump a CSV of the latest cumulative count of county-by-county positive tests.'

    def handle(self, *args, **options):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_positive_tests_by_county.csv'), 'w') as csvfile:
            # fieldnames = ['first_name', 'last_name']
            fieldnames = ['county_fips', 'county_name', 'total_positive_tests', 'total_deaths', 'latitude', 'longitude']
            # COUNTY ID	COUNTY	COUNTA of COUNTY ID	COUNTUNIQUE of COMMUNITY TRANSMISSION	COUNTA of FATALITIES	MAX of LAT	MAX of LONG
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            msg_output = '*Latest numbers from MPH:*\n\n'

            updated_total = 0
            statewide_daily_new_cases = 0

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

                    statewide_daily_new_cases += latest_observation.daily_count

                    # Slack lastest results
                    change_text = ''
                    if latest_observation.daily_count != 0:
                        optional_plus = '+'
                        if latest_observation.daily_count < 0:
                            optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE *** '
                        elif latest_observation.daily_count == latest_observation.cumulative_count:
                            optional_plus = ':heavy_plus_sign: NEW COUNTY '

                        change_text = ' (:point_right: {}{} today)'.format(optional_plus, latest_observation.daily_count)

                    print('{}: {}{}\n'.format(c.name, latest_observation.cumulative_count, change_text))
                    msg_output = msg_output + '{}: {}{}\n'.format(c.name, latest_observation.cumulative_count, change_text)

            previous_total = CurrentTotal.objects.all().first()
            if not previous_total:
                previous_total = CurrentTotal(count=0)
            if updated_total != previous_total.count:
                slack_header = '*{} new county cases announced today statewide.*\n\n'.format(statewide_daily_new_cases)
                slack_latest(slack_header + msg_output)
                previous_total.count = updated_total
                previous_total.save()
            else:
                slack_latest('Scraper update: No changes detected.')
