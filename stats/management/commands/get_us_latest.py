import os
import csv
import codecs
import json
import pytz
from pytz import timezone
import requests
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Grab the latest national figures from Covid Tracking Project'

    NYT_ENDPOINT = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/live/us.csv'
    CTP_ENDPOINT = 'https://covidtracking.com/api/v1/us/current.json'

    US_LATEST_EXPORT_PATH_NYT = os.path.join(settings.BASE_DIR, 'exports', 'us_latest_nyt.csv')
    US_LATEST_EXPORT_PATH_CTP = os.path.join(settings.BASE_DIR, 'exports', 'us_latest_ctp.csv')

    def build_from_ctp(self):
        r = requests.get(self.CTP_ENDPOINT)
        if r.status_code == requests.codes.ok:
            raw_json = r.json()[0]
            death_total = raw_json['death']
            utc = pytz.utc
            central = timezone('US/Central')

            # CTP messed up their timestamps to include 24, which doesn't exist in python. Logic check here:
            if '24' in raw_json['lastModified']:
                raw_json['lastModified'] = raw_json['lastModified'][0:11] + '00:00:00Z'

            last_update = utc.localize(datetime.strptime(raw_json['lastModified'], '%Y-%m-%dT%H:%M:%S%fZ')).astimezone(central).strftime('%Y-%m-%d %H:%M')

            out_dict = {
                'death_total': f'{death_total:,}',
                'last_update': last_update
            }
            return out_dict
        return False

    def build_from_nyt(self):
        r = requests.get(self.NYT_ENDPOINT, stream=True)

        nyt_csv = csv.DictReader(codecs.iterdecode(r.iter_lines(), 'utf-8'))
        for row in nyt_csv:
            last_line = row

        cases_total = int(last_line['cases'])
        deaths_total = int(last_line['deaths'])

        out_dict = {
            'death_total': f'{deaths_total:,}',
            'case_total': f'{cases_total:,}',
            'last_update': last_line['date']
        }
        return out_dict

    def test_and_save(self, out_dict, export_path):
        if out_dict:
            # Test that you get an actual integer before overwriting
            out_int = int(out_dict['death_total'].replace(',', ''))
            if out_int > 0:
                print('Output looks normal, Updating U.S. latest file to {}'.format(export_path))
                with open(export_path, 'w') as csvfile:
                    fieldnames = out_dict.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow(out_dict)

                return True
        return False

    def handle(self, *args, **options):
        out_dict = self.build_from_nyt()
        bool_saved = self.test_and_save(out_dict, self.US_LATEST_EXPORT_PATH_NYT)

        out_dict = self.build_from_ctp()
        bool_saved = self.test_and_save(out_dict, self.US_LATEST_EXPORT_PATH_CTP)
