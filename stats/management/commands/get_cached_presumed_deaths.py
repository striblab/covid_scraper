import re
import boto3
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand

from stats.utils import get_matching_s3_cached_html, find_filename_date_matchs, get_s3_file_contents

class Command(BaseCommand):
    help = 'Find "probable deaths" in cached html data'

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

        for f in noon_files:
            soup = BeautifulSoup(get_s3_file_contents(f, self.S3_HTML_BUCKET, s3), 'html.parser')
            probables = soup.find(string=re.compile("Probable COVID-19 Deaths.*:"))
            print(f['scrape_date'], probables)
