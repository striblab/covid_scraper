import os
import csv
import boto3
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand

from stats.utils import get_matching_s3_cached_html, find_filename_date_matchs, get_s3_file_contents

class Command(BaseCommand):
    help = 'Find the first date that each care home appears on the situation page'

    S3_HTML_BUCKET = 'static.startribune.com'
    S3_HTML_PATH = settings.S3_EXPORT_PREFIX

    def handle(self, *args, **options):
        session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        s3 = session.client('s3')

        matching_files = get_matching_s3_cached_html(self.S3_HTML_BUCKET, self.S3_HTML_PATH, s3)
        noon_files = find_filename_date_matchs(matching_files, 12)

        facilities = {}

        for f in noon_files:
            soup = BeautifulSoup(get_s3_file_contents(f, self.S3_HTML_BUCKET, s3), 'html.parser')

            homes_table = None
            homes_th = soup.find('th', text='Facility')
            if homes_th:
                homes_table = homes_th.find_parent('table')
                for row in homes_table.find_all('tr')[1:]:
                    cells = row.find_all(['th', 'td'])
                    # print(cells)
                    facility_name = cells[1].text
                    county = cells[0].text

                    home_lookup = {
                        'name': facility_name.strip(),
                        'county': county.strip(),
                    }
                    print(home_lookup)
                    hashed_lookup = hash(frozenset(home_lookup.items()))
                    if hashed_lookup not in facilities.keys():
                        facilities[hashed_lookup] = {'name': facility_name, 'county': county, 'dates': [f['scrape_date']]}
                    else:
                        facilities[hashed_lookup]['dates'].append(f['scrape_date'])

        final_facilities = [item for key, item in facilities.items()]
        for f in final_facilities:
            f['min_date'] = min(f['dates'])
            f['max_date'] = max(f['dates'])
            f['dates'] = [d.strftime('%Y-%m-%d') for d in f['dates']]

        print(final_facilities)
        with open(os.path.join(settings.BASE_DIR, 'exports', 'care_facilities_dates.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=final_facilities[0].keys())
            writer.writeheader()
            writer.writerows(final_facilities)
