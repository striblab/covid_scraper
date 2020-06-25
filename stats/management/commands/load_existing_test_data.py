import os
from datetime import datetime
import csv

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'One-time import of existing data that was manually compiled'

    def load_county_case_data(self):
        with open(os.path.join(settings.BASE_DIR, 'data', 'mn-covid-tracker-manual-entry.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                # print(row)
                if row['COUNTY'] != '':
                    if row['DATE'] not in records.keys():
                        records[row['DATE']] = {
                            row['COUNTY']: 1
                        }

                    elif row['COUNTY'] not in records[row['DATE']].keys():
                        records[row['DATE']][row['COUNTY']] = 1

                    else:
                        records[row['DATE']][row['COUNTY']] += 1

            print(records)

            for text_date, counties in records.items():
                parsed_date = datetime.strptime(text_date, '%m/%d/%Y').date()
                for county, daily_count in counties.items():
                    county_obj = County.objects.get(name__iexact=county)
                    # Get existing total to add to
                    previous_total_obj = CountyTestDate.objects.filter(county=county_obj, scrape_date__lt=parsed_date).order_by('-scrape_date').first()
                    if previous_total_obj:
                        print('{} + {}'.format(previous_total_obj.cumulative_count, daily_count))
                        new_total = previous_total_obj.cumulative_count + daily_count
                    else:
                        new_total = daily_count
                        print(daily_count)

                    print(parsed_date, county, new_total)
                    obj, created = CountyTestDate.objects.update_or_create(
                        county=county_obj,
                        scrape_date=parsed_date,
                        defaults={'daily_count': daily_count, 'cumulative_count': new_total}
                    )

    def load_statewide_historical_data(self):
        # hospitalizations
        with open(os.path.join(settings.BASE_DIR, 'data', 'hospitalizations-manual-entry.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                if 'Date' in row.keys():
                    parsed_date = datetime.strptime(row['Date'], '%m/%d/%Y').date()
                    # print(row)
                    defaults = {}
                    if row['Cumulative total hospitalizations'] != '':
                        defaults['cumulative_hospitalized'] = int(row['Cumulative total hospitalizations'])

                        if row['Currently hospitalized'] != '':
                            defaults['currently_hospitalized'] = int(row['Currently hospitalized'])

                        if row['ICU'] != '':
                            defaults['currently_in_icu'] = int(row['ICU'])

                        if row['Recoveries'] != '':
                            defaults['cumulative_statewide_recoveries'] = int(row['Recoveries'])

                        if row['Deaths'] != '':
                            defaults['cumulative_statewide_deaths'] = int(row['Deaths'])

                        obj, created = StatewideTotalDate.objects.update_or_create(
                            scrape_date=parsed_date,
                            defaults=defaults
                        )

        # tests
        with open(os.path.join(settings.BASE_DIR, 'data', 'tests-statewide-manual-entry.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                if 'DATE' in row.keys():
                    parsed_date = datetime.strptime(row['DATE'], '%m/%d/%Y').date()
                    # print(row)
                    defaults = {}

                    if row['TOTAL/CUMULATIVE TESTS'] != '':
                        defaults['cumulative_completed_tests'] = int(row['TOTAL/CUMULATIVE TESTS'])

                    if row['cumulative positives'] != '':
                        defaults['cumulative_positive_tests'] = int(row['cumulative positives'])

                    obj, created = StatewideTotalDate.objects.update_or_create(
                        scrape_date=parsed_date,
                        defaults=defaults
                    )

    def handle(self, *args, **options):
        # self.load_county_case_data()
        self.load_statewide_historical_data()
