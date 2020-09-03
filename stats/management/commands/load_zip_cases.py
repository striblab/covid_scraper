import re
import datetime
import os
import pandas as pd

from django.conf import settings
from django.core.management.base import BaseCommand

from stats.models import ZipCasesDate


class Command(BaseCommand):
    help = 'Load weekly cases by zip code spreadsheet from MDH. This is very basic, and most tabulation is handled on dumping.'

    IMPORT_FOLDER = os.path.join(settings.BASE_DIR, 'imports', 'zip_cases')

    # FILE_PATH = os.path.join(IMPORT_FOLDER, 'COVID_ZIP_8.3.2020.xlsx')
    FILE_PATH = os.path.join(IMPORT_FOLDER, 'covid_zip_20200903.csv')

    def code_cases(self, cases_raw):
        '''Handle <=5 cases, set them to -1'''
        if cases_raw == '<=5':
            return -1
        return int(cases_raw)

    def catch_missing(self, zip_raw):
        if zip_raw == 'Missing/Unknown':
            return 'Missing'
        else:
            return zip_raw

    def handle(self, *args, **options):
        # data_date_parser = re.search('(\d+)\.(\d+)\.(\d+)\.xlsx', self.FILE_PATH)
        # data_date = datetime.date(int(data_date_parser.group(3)), int(data_date_parser.group(1)), int(data_date_parser.group(2)))
        data_date_parser = re.search('covid_zip_(\d{4})(\d{2})(\d{2})\.csv', self.FILE_PATH)
        data_date = datetime.date(int(data_date_parser.group(1)), int(data_date_parser.group(2)), int(data_date_parser.group(3)))

        # Delete old records from this data date
        ZipCasesDate.objects.filter(data_date=data_date).delete()

        # in_df = pd.read_excel(self.FILE_PATH)
        in_df = pd.read_csv(self.FILE_PATH)
        in_df.rename(columns={'Cases': 'CASES', 'ZIP': 'ZIPCODE'}, inplace=True)
        in_df['data_date'] = data_date
        in_df['cases_cumulative'] = in_df['CASES'].apply(lambda x: self.code_cases(x))
        in_df['ZIPCODE'] = in_df['ZIPCODE'].apply(lambda x: self.catch_missing(x))
        in_df.rename(columns={'ZIPCODE': 'zip'}, inplace=True)
        in_df.drop(columns=['CASES'], inplace=True)

        df_records = in_df.to_dict('records')

        model_instances = [ZipCasesDate(**record) for record in df_records]

        ZipCasesDate.objects.bulk_create(model_instances)
