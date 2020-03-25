import os
import csv
import json
import requests

from django.conf import settings

from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'Dump a CSV of the latest cumulative count of county-by-county positive tests.'

    def slack_latest(self, text):
        # endpoint = 'https://hooks.slack.com/services/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'  # robot-dojo
        endpoint = 'https://hooks.slack.com/services/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'  # covid-tracking
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
        }
        payload = {
            # 'text': text,
            'blocks': [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": text
                    }
                }
            ]
        }
        print(payload)
        r = requests.post(endpoint, data=json.dumps(payload), headers=headers)

    def handle(self, *args, **options):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_positive_tests_by_county.csv'), 'w') as csvfile:
            # fieldnames = ['first_name', 'last_name']
            fieldnames = ['county_fips', 'county_name', 'total_positive_tests', 'latitude', 'longitude']
            # COUNTY ID	COUNTY	COUNTA of COUNTY ID	COUNTUNIQUE of COMMUNITY TRANSMISSION	COUNTA of FATALITIES	MAX of LAT	MAX of LONG
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            msg_output = '*Latest numbers from MPH:*\n\n'

            for c in County.objects.all().order_by('name'):
                latest_observation = CountyTestDate.objects.filter(county=c).order_by('-scrape_date').first()
                if latest_observation:
                    row = {
                        'county_fips': c.fips,
                        'county_name': c.name,
                        'total_positive_tests': latest_observation.cumulative_count,
                        'latitude': c.latitude,
                        'longitude': c.longitude,
                    }

                    writer.writerow(row)

                    # Slack lastest results
                    change_text = ''
                    if latest_observation.daily_count != 0:
                        optional_plus = '+'
                        if latest_observation.daily_count < 0:
                            optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE *** '

                        change_text = ' (:point_right: {}{} today)'.format(optional_plus, latest_observation.daily_count)

                    print('{}: {}{}\n'.format(c.name, latest_observation.cumulative_count, change_text))
                    msg_output = msg_output + '{}: {}{}\n'.format(c.name, latest_observation.cumulative_count, change_text)

            self.slack_latest(msg_output)
