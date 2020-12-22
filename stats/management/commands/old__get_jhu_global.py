import os
import csv
import json
import urllib.request
import codecs
import pandas as pd
import requests

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Grab the latest global country-by-country figures from Johns Hopkins, calculate current totals'

    JHU_DEATHS_ENDPOINT = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'

    JHU_CASES_ENDPOINT = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'

    GLOBAL_LATEST_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'global_latest_ghu.json')


    def get_ghu_csv(self, endpoint):
        ftpstream = urllib.request.urlopen(endpoint)
        csvfile = csv.DictReader(codecs.iterdecode(ftpstream, 'utf-8'))
        return csvfile

    def get_sum_of_last(self, csv_obj):
        ''' get sum of values in last column'''
        df = pd.DataFrame(csv_obj)
        most_recent_date = df.columns[-1]
        most_recent_values = df.iloc[:,-1]

        return {'sum': most_recent_values.astype('int').sum(), 'date': most_recent_date}

    def handle(self, *args, **options):
        print('Getting latest Johns Hopkins global figures ...')

        deaths = self.get_ghu_csv(self.JHU_DEATHS_ENDPOINT)
        total_deaths = self.get_sum_of_last(deaths)

        cases = self.get_ghu_csv(self.JHU_CASES_ENDPOINT)
        total_cases = self.get_sum_of_last(cases)

        out_dict = {
            'cases_total': int(total_cases['sum']),
            'cases_updated': total_cases['date'],
            'deaths_total': int(total_deaths['sum']),
            'deaths_updated': total_deaths['date'],
        }

        with open(self.GLOBAL_LATEST_EXPORT_PATH, 'w') as outfile:
            outfile.write(json.dumps(out_dict))
            outfile.close()
