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
    help = 'Get daily figures for cases and deaths by race/ethnicity'

    S3_HTML_BUCKET = 'static.startribune.com'
    S3_HTML_PATH = settings.S3_EXPORT_PREFIX

    def parse_comma_int(self, input_str):
        if input_str == '-':
            return '-'
        else:
            return int(input_str.replace(',', ''))

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
            soup = BeautifulSoup(get_s3_file_contents(f, self.S3_HTML_BUCKET, s3), 'html.parser')

            # total_new_deaths = None
            # total_new_deaths_section = soup.find('span', text='Newly reported deaths')
            # if total_new_deaths_section:
            #     total_new_deaths = int(total_new_deaths_section.find_parent('td').find('strong').text)
            #     print(total_new_deaths)

            race_table = None
            race_table = soup.find('table', id='raceethtable')

            if race_table:
                print(f)
                race_eth_switch = 'race'
                for row in race_table.find_all('tr')[1:]:
                    label = row.find_all(['th'])
                    if len(label) > 1:
                        race_eth_switch = 'eth'  # Do nothing with this row other than switch to ethnicity
                    else:
                        cells = row.find_all(['td'])
                        try:
                            # print(cells)
                            race_eth = label[0].text
                            if race_eth_switch == 'eth' and race_eth == 'Unknown/missing':
                                race_eth = 'Unknown/missing (ethnicity)'

                            case_count = self.parse_comma_int(cells[0].text)
                            death_count = self.parse_comma_int(cells[1].text)

                            record = {
                                'date': f['scrape_date'],
                                'race_eth': race_eth,
                                'case_count': case_count,
                                'death_count': death_count,
                            }
                            # print(record)

                            records.append(record)
                        except:
                            raise
                            print('Error for {}'.format(f['scrape_date']))
                            pass

        df = pd.DataFrame(records)
        df['cases_change'] = df.groupby('race_eth')['case_count'].transform(lambda x: x.diff())
        df['deaths_change'] = df.groupby('race_eth')['death_count'].transform(lambda x: x.diff())
        df['deaths_change_rolling'] = df.groupby('race_eth')['deaths_change'].transform(lambda x: x.rolling(7, 2).mean())
        print(df)

        df.to_csv('covid_scraper/exports/cases_deaths_by_race.csv', index=False)
