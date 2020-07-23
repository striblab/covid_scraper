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

    FILE_PATH = os.path.join(IMPORT_FOLDER, 'COVID_ZIP_07.20.2020.xlsx')

    def code_cases(self, cases_raw):
        '''Handle <=5 cases, set them to -1'''
        if cases_raw == '<=5':
            return -1
        return int(cases_raw)

    def handle(self, *args, **options):
        data_date_parser = re.search('(\d+)\.(\d+)\.(\d+)\.xlsx', self.FILE_PATH)
        data_date = datetime.date(int(data_date_parser.group(3)), int(data_date_parser.group(1)), int(data_date_parser.group(2)))

        # Delete old records from this data date
        ZipCasesDate.objects.filter(data_date=data_date).delete()

        in_df = pd.read_excel(self.FILE_PATH)
        in_df['data_date'] = data_date
        in_df['cases_cumulative'] = in_df['Cases'].apply(lambda x: self.code_cases(x))
        in_df.rename(columns={'GEOG_UNIT': 'zip'}, inplace=True)
        in_df.drop(columns=['Cases'], inplace=True)

        df_records = in_df.to_dict('records')

        model_instances = [ZipCasesDate(**record) for record in df_records]

        ZipCasesDate.objects.bulk_create(model_instances)
