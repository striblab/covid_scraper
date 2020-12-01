import os
from datetime import datetime
import csv

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County, Death


class Command(BaseCommand):
    help = 'One-time import of existing data that was manually compiled'

    def tf(self, input):
        if input == 'Y':
            return True
        elif input == 'N':
            return False
        return None

    def int_or_null(self, input):
        try:
            return int(input)
        except:
            return None

    def load_deaths_data(self):
        with open(os.path.join(settings.BASE_DIR, 'data', 'mn-covid-tracker-deaths-manual-entry.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                print(row['COUNTY'])

                death = Death(
                    scrape_date=datetime.strptime(row['DATE'], '%m/%d/%Y').date(),
                    age_group=row['AGE RANGE'],
                    actual_age=self.int_or_null(row['ACTUAL AGE']),
                    county=County.objects.get(name=row['COUNTY']),
                    bool_ltc=self.tf(row['LONG-TERM CARE?'])
                )
                death.save()

    def handle(self, *args, **options):
        self.load_deaths_data()
