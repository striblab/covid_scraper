import os
import csv
import datetime

from django.conf import settings
from django.db.models import Sum
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'Calculate change per day to export cumulative and daily counts'

    def handle(self, *args, **options):

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_timeseries.csv'), 'w') as csvfile:
            fieldnames = ['date', 'total_positive_tests', 'new_positive_tests', 'total_hospitalized', 'currently_hospitalized', 'currently_in_icu', 'total_statewide_deaths', 'new_statewide_deaths', 'total_statewide_recoveries', 'total_completed_tests', 'new_completed_tests']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            previous_total_cases = 0
            previous_total_deaths = 0
            previous_total_tests = 0
            for record in StatewideTotalDate.objects.filter(cumulative_positive_tests__gt=0).order_by('scrape_date'):
                new_cases = record.cumulative_positive_tests - previous_total_cases
                previous_total_cases = record.cumulative_positive_tests

                new_deaths = record.cumulative_statewide_deaths - previous_total_deaths
                previous_total_deaths = record.cumulative_statewide_deaths

                new_tests = record.cumulative_completed_tests - previous_total_tests
                previous_total_tests = record.cumulative_completed_tests

                if record.scrape_date == datetime.date.today() and new_cases == 0:
                    pass  # Ignore if there's no new results for today
                else:

                    row = {
                        'date': record.scrape_date.strftime('%Y-%m-%d'),
                        'total_positive_tests': record.cumulative_positive_tests,
                        'new_positive_tests': new_cases,
                        'total_hospitalized': record.cumulative_hospitalized,
                        'currently_hospitalized': record.currently_hospitalized,
                        'currently_in_icu': record.currently_in_icu,
                        'total_statewide_deaths': record.cumulative_statewide_deaths,
                        'new_statewide_deaths': new_deaths,
                        'total_statewide_recoveries': record.cumulative_statewide_recoveries,
                        'total_completed_tests': record.cumulative_completed_tests,
                        'new_completed_tests': new_tests,
                    }
                    writer.writerow(row)
