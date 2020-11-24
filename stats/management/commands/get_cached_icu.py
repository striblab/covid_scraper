import os
import csv
import datetime
import boto3
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand

from stats.models import StatewideHospitalizationsDate
from stats.utils import get_matching_s3_cached_html, find_filename_date_matchs, get_s3_file_contents

class Command(BaseCommand):
    help = 'Find the first date that each care home appears on the situation page'

    S3_HTML_BUCKET = 'static.startribune.com'
    S3_HTML_PATH = settings.S3_EXPORT_PREFIX

    def full_table_parser(self, table):
        ''' should work on multiple columns '''
        # table = soup.find("th", text=find_str).find_parent("table")
        rows = table.find_all("tr")
        num_rows = len(rows)
        data_rows = []
        for k, row in enumerate(rows):
            # print(row)
            if k == 0:
                first_row = row.find_all("th")
                col_names = [' '.join(th.text.split()).replace('<br>', ' ') for th in first_row]
                # print(col_names)
            else:
                data_row = {}
                cells = row.find_all(["th", "td"])
                if len(cells) > 0:  # Filter out bad TRs
                    for k, c in enumerate(col_names):
                        # print(cells[k].text)

                        data_row[c] = cells[k].text
                    data_rows.append(data_row)

        return data_rows

    def parse_comma_int(self, input):
        if input.strip() in ['-', '-\xa0\xa0']:
            return None
        return int(input.replace(',', ''))

    def parse_date(self, input):
        try:
            return datetime.datetime.strptime('{}/2020'.format(input), '%m/%d/%Y').date()
        except:
            if input == 'Admitted on or before 3/5':
                return datetime.datetime.strptime('3/5/2020'.format(input), '%m/%d/%Y').date()
            return None

    def add_statewide_hospitalizations_timeseries(self, hosp_timeseries):
        ''' Adapted from main scraper '''
        print('Parsing statewide hospitalizations timeseries...')

        if len(hosp_timeseries) > 0:

            # Remove old records from today
            # existing_date_records = StatewideHospitalizationsDate.objects.filter(scrape_date=update_date)
            # print('Removing {} records of hospitalizations timeseries data'.format(existing_today_records.count()))
            # existing_today_records.delete()
            hosp_objs = []
            for c in hosp_timeseries:
                if c['scrape_date'] <= datetime.date(2020, 9, 23):
                    std = StatewideHospitalizationsDate(
                        reported_date=c['reported_date'],
                        total_hospitalizations=c['total_hospitalizations'],
                        total_icu_admissions=c['total_icu_admissions'],
                        update_date=c['scrape_date'],
                        scrape_date=c['scrape_date'],
                    )
                    hosp_objs.append(std)

            print('Adding {} records of hospitalizations timeseries data'.format(len(hosp_objs)))
            StatewideHospitalizationsDate.objects.bulk_create(hosp_objs)

    EARLY_DATA = [
      {"reported_date": "3/19", "total_hospitalizations": 7, "total_icu_admissions": 1},
      {"reported_date": "3/20", "total_hospitalizations": 7, "total_icu_admissions": 2},
      {"reported_date": "3/21", "total_hospitalizations": 12, "total_icu_admissions": 5},
      {"reported_date": "3/22", "total_hospitalizations": 12, "total_icu_admissions": 5},
      {"reported_date": "3/23", "total_hospitalizations": 21, "total_icu_admissions": 5},
      {"reported_date": "3/24", "total_hospitalizations": 21, "total_icu_admissions": 7},
      {"reported_date": "3/25", "total_hospitalizations": 35, "total_icu_admissions": 12},
      {"reported_date": "3/26", "total_hospitalizations": 41, "total_icu_admissions": 13},
      {"reported_date": "3/27", "total_hospitalizations": 51, "total_icu_admissions": 17},
      {"reported_date": "3/28", "total_hospitalizations": 57, "total_icu_admissions": 17},
      {"reported_date": "3/29", "total_hospitalizations": 75, "total_icu_admissions": 24},
      {"reported_date": "3/30", "total_hospitalizations": 92, "total_icu_admissions": 25},
      {"reported_date": "3/31", "total_hospitalizations": 112, "total_icu_admissions": 32},
      {"reported_date": "4/1", "total_hospitalizations": 122, "total_icu_admissions": 40},
      {"reported_date": "4/2", "total_hospitalizations": 138, "total_icu_admissions": 49},
      {"reported_date": "4/3", "total_hospitalizations": 156, "total_icu_admissions": 62},
      {"reported_date": "4/4", "total_hospitalizations": 180, "total_icu_admissions": 69},
      {"reported_date": "4/5", "total_hospitalizations": 202, "total_icu_admissions": 77},
      {"reported_date": "4/6", "total_hospitalizations": 223, "total_icu_admissions": 90},
      {"reported_date": "4/7", "total_hospitalizations": 242, "total_icu_admissions": 100},
      {"reported_date": "4/8", "total_hospitalizations": 271, "total_icu_admissions": 105},
      {"reported_date": "4/9", "total_hospitalizations": 293, "total_icu_admissions": 119},
      {"reported_date": "4/10", "total_hospitalizations": 317, "total_icu_admissions": 131}
    ]

    def handle(self, *args, **options):

        icu_dates = []

        # First get dates from before tables existed on MDH site. Sourced from situation_2020-04-10_1003.html
        early_data_scrape_dates = [self.parse_date(d['reported_date']) for d in self.EARLY_DATA]
        for sd in early_data_scrape_dates:
            for d in self.EARLY_DATA:
                reported_date = self.parse_date(d['reported_date'])

                if reported_date <= sd:
                    icu_dates.append({
                        'reported_date': reported_date,
                        'scrape_date': sd,
                        'total_icu_admissions': d['total_icu_admissions'],
                        'total_hospitalizations': d['total_hospitalizations']
                    })

        # Now move on to the cached files.
        session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        s3 = session.client('s3')

        matching_files = get_matching_s3_cached_html(self.S3_HTML_BUCKET, self.S3_HTML_PATH, s3)
        last_files_of_day = find_filename_date_matchs(matching_files)

        for f in last_files_of_day:
            soup = BeautifulSoup(get_s3_file_contents(f, self.S3_HTML_BUCKET, s3), 'html.parser')

            hosp_table = None
            hosp_div = soup.find('div', id='hosp')
            if hosp_div:
                hosp_table = hosp_div.find('table')
                if hosp_table:
                    print(f['scrape_date'])

                    table_data = self.full_table_parser(hosp_table)
                    for row in table_data:
                        if f['scrape_date'] <= datetime.date(2020, 9, 23):

                            if 'Date reported' in row:
                                reported_date = self.parse_date(row['Date reported'])
                            elif 'Date' in row:
                                reported_date = self.parse_date(row['Date'])
                            else:
                                print(row)
                                reported_date = 'HEY SOMETHING IS WRONG'

                            # if f['scrape_date'] != reported_date:
                            #     print(f['scrape_date'], reported_date)
                            #     print('DATE MISMATCH.')

                            # print(row)
                            # if 'Cases admitted to a hospital' in row:
                            #     new_hospitalizations = self.parse_comma_int(c['Cases admitted to a hospital'])
                            # elif 'Hospitalized, not in ICU (daily)' in row:
                            #     new_hospitalizations = self.parse_comma_int(row['Hospitalized, not in ICU (daily)']) + self.parse_comma_int(row['Hospitalized in ICU (daily)'])
                            # else:
                            #     print(row)
                            #     new_hospitalizations = 'HEY SOMETHING IS WRONG'

                            if 'Total ICU hospitalizations' in row:
                                total_icu_admissions = self.parse_comma_int(row['Total ICU hospitalizations'])
                            elif 'Total ICU hospitalizations (cumulative)' in row:
                                total_icu_admissions = self.parse_comma_int(row['Total ICU hospitalizations (cumulative)'])
                            else:
                                print(row)
                                total_icu_admissions = 'HEY SOMETHING IS WRONG'

                            if 'Total hospitalizations' in row:
                                total_hospitalizations = self.parse_comma_int(row['Total hospitalizations'])
                            elif 'Total hospitalizations (cumulative)' in row:
                                total_hospitalizations = self.parse_comma_int(row['Total hospitalizations (cumulative)'])
                            else:
                                print(row)
                                total_hospitalizations = 'HEY SOMETHING IS WRONG'

                            print(reported_date, total_icu_admissions, total_hospitalizations)
                            icu_dates.append({
                                'reported_date': reported_date,
                                'scrape_date': f['scrape_date'],
                                'total_icu_admissions': total_icu_admissions,
                                'total_hospitalizations': total_hospitalizations,
                            })

        print(icu_dates)
        self.add_statewide_hospitalizations_timeseries(icu_dates)
