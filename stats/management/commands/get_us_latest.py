import os
import json
import pytz
from pytz import timezone
import requests
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Grab the latest national figures from Covid Tracking Project'

    ENDPOINT = 'https://covidtracking.com/api/v1/us/current.json'
    US_LATEST_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'ctp_us_latest.json')

    def handle(self, *args, **options):
        r = requests.get(self.ENDPOINT)
        if r.status_code == requests.codes.ok:
            raw_json = r.json()[0]
            death_total = raw_json['death']
            utc = pytz.utc
            central = timezone('US/Central')
            last_update = utc.localize(datetime.strptime(raw_json['lastModified'], '%Y-%m-%dT%H:%M:%S.%fZ')).astimezone(central).strftime('%Y-%m-%d %H:%M')

            out_dict = {
                'death_total': f'{death_total:,}',
                'last_update': last_update
            }

            with open(self.US_LATEST_EXPORT_PATH, 'w') as jsonfile:
                jsonfile.write(json.dumps(out_dict))

            print(out_dict)

        else:
            return False
