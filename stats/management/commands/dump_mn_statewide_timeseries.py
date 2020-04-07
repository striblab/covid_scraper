import os
import csv

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

        # with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_timeseries.csv'), 'w') as csvfile:
        #     fieldnames = ['date', 'total_positive_tests', 'new_positive_tests']
        #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #     writer.writeheader()
        #
        #     previous_total = 0
        #     for date in CountyTestDate.objects.all().order_by('scrape_date').values('scrape_date').distinct():
        #         # Get the totals for each county as of that date. The latest observation may be an earlier date.
        #         county_totals = 0
        #         for c in County.objects.all():
        #             latest_county_observation = CountyTestDate.objects.filter(county=c, scrape_date__lte=date['scrape_date']).order_by('-scrape_date').first()
        #             if latest_county_observation:
        #                 county_totals += latest_county_observation.cumulative_count
        #
        #         new_cases = county_totals - previous_total
        #         previous_total = county_totals
        #
        #
        #         row = {
        #             'date': date['scrape_date'].strftime('%Y-%m-%d'),
        #             'total_positive_tests': county_totals,
        #             'new_positive_tests': new_cases
        #         }
        #         writer.writerow(row)
