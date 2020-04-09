import os
import csv
import requests
import datetime

from django.core.management.base import BaseCommand
from stats.utils import slack_latest

from django.conf import settings


class Command(BaseCommand):
    help = 'Check for new or updated results from the Minnesota "dashboard": https://mn.gov/covid19/data/response.jsp'

    def get_csv(self):
        r = requests.get('https://mn.gov/covid19/assets/StateofMNResponseDashboardCSV_tcm1148-427143.csv')
        if r.status_code == requests.codes.ok:

            # Save a copy of CSV
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            outpath = os.path.join(settings.BASE_DIR, 'exports', 'dashboard', 'dashboard_{}.csv').format(now)
            with open(outpath, 'w') as csv_file:
                csv_file.write(r.text)
                csv_file.close()
        else:
            slack_latest('WARNING: Dashboard CSV scraper error. Not proceeding.', '#robot-dojo')

    def handle(self, *args, **options):
        self.get_csv()
