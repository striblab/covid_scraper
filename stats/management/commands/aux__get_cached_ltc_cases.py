import os
import re
import csv
import boto3
import pandas as pd
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand

from stats.utils import get_matching_s3_cached_html, find_filename_date_matchs, get_s3_file_contents

class Command(BaseCommand):
    help = 'Get daily figures for daily death residence type'

    S3_HTML_BUCKET = 'static.startribune.com'
    S3_HTML_PATH = 'news/projects/all/2021-covid-scraper/raw'

    def handle(self, *args, **options):
        session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        s3 = session.client('s3')

        matching_files = get_matching_s3_cached_html(self.S3_HTML_BUCKET, self.S3_HTML_PATH, s3)
        noon_files = find_filename_date_matchs(matching_files, None)

        records = []

        for f in noon_files:
            print(f)
            soup = BeautifulSoup(get_s3_file_contents(f, self.S3_HTML_BUCKET, s3), 'html.parser')

            # total_new_deaths = None
            # total_new_deaths_section = soup.find('span', text='Newly reported deaths')
            # if total_new_deaths_section:
            #     total_new_deaths = int(total_new_deaths_section.find_parent('td').find('strong').text)
            #     print(total_new_deaths)

            homes_table = soup.find('table', id='restable')
            if not homes_table:
                print('Table not found.')
                # homes_th = soup.find('th', text=re.compile("Residence type.*"))
                # if homes_th:
                #     homes_table = homes_th.find_parent('tr').find_parent('table')
            else:

                for row in homes_table.find_all('tr')[1:]:
                    # cells = row.find_all(['td'])
                    try:
                        facility_type = row.find('th').text
                        total_cases_count = int(row.find('td').text.strip().replace(',', ''))
                        print(facility_type, total_cases_count)
                        if total_cases_count:
                            record = {
                                'date': f['scrape_date'],
                                'facility_type': facility_type,
                                'total_cases_count': total_cases_count
                            }

                            records.append(record)
                            # if total_new_deaths:
                            #     death_pct = round(new_death_count / total_new_deaths, 4)
                            # else:
                            #     death_pct = None
                            # record = {
                            #     'date': f['scrape_date'],
                            #     'facility_type': facility_type,
                            #     'death_count': new_death_count,
                            #     'death_pct': death_pct
                            # }
                            #
                            # records.append(record)
                    except:
                        raise
                        print('Error for {}'.format(f['scrape_date']))
                        pass

        df = pd.DataFrame(records)
        # print(df.head())

        df.to_csv('covid_scraper/exports/total_cases_by_residence_type.csv', index=False)
