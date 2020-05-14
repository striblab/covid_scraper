import os
import csv
import datetime
from datetime import timedelta

from django.conf import settings
from django.db.models import Min, Max
from django.core.management.base import BaseCommand
from stats.models import StatewideTotalDate, StatewideCasesBySampleDate, StatewideTestsDate


class Command(BaseCommand):
    help = 'Calculate change per day to export cumulative and daily counts'

    def handle(self, *args, **options):

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_timeseries.csv'), 'w') as csvfile:
            fieldnames = ['date', 'total_positive_tests', 'new_positive_tests', 'removed_cases', 'total_hospitalized', 'currently_hospitalized', 'currently_in_icu', 'total_statewide_deaths', 'new_statewide_deaths', 'total_statewide_recoveries', 'total_completed_tests', 'new_completed_tests']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Cases: Get max scrape date for each sample date
            cases_timeseries_values = {}
            cases_reported_dates = StatewideCasesBySampleDate.objects.all().values_list('sample_date', flat=True).distinct()
            for t in cases_reported_dates:
                latest_record = StatewideCasesBySampleDate.objects.filter(sample_date=t).values().latest('scrape_date')
                cases_timeseries_values[t] = latest_record
            # print(cases_timeseries_values)

            # Tests: Get max scrape date for each real date
            tests_timeseries_values = {}
            tests_reported_dates = StatewideTestsDate.objects.all().values_list('reported_date', flat=True).distinct()
            for t in tests_reported_dates:
                latest_record = StatewideTestsDate.objects.filter(reported_date=t).values().latest('scrape_date')
                tests_timeseries_values[t] = latest_record
            # print(tests_timeseries_values)

            # Topline records: get all by date
            topline_timeseries_values = {}
            for s in StatewideTotalDate.objects.all().values():
                topline_timeseries_values[s['scrape_date']] = s
            # print(topline_timeseries_values)

            min_date = StatewideCasesBySampleDate.objects.all().aggregate(min_date=Min('sample_date'))['min_date']
            max_date = StatewideTotalDate.objects.filter(cumulative_positive_tests__gt=0).aggregate(max_date=Max('scrape_date'))['max_date']
            current_date = min_date

            previous_total_cases = 0
            previous_total_deaths = 0
            previous_total_tests = 0
            # Go through all dates and check for either timeseries or, failing that, topline data
            while current_date <= max_date:
                # print(current_date)
                topline_data = topline_timeseries_values[current_date]

                if topline_data['new_deaths'] == 0:
                    new_deaths = topline_data['cumulative_statewide_deaths'] - previous_total_deaths
                else:
                    new_deaths = topline_data['new_deaths']
                previous_total_deaths = topline_data['cumulative_statewide_deaths']

                if current_date in cases_timeseries_values:
                    # print('timeseries')
                    cr = cases_timeseries_values[current_date]
                    new_cases = cr['new_cases']
                    total_cases = cr['total_cases']
                    previous_total_cases = total_cases
                else:
                    # This will usually just be today's values because no samples have come back yet
                    new_cases = 0
                    # removed_cases = topline_data['removed_cases']
                    total_cases = topline_data['cumulative_positive_tests']

                if current_date - timedelta(days=1) in tests_timeseries_values:
                    tr = tests_timeseries_values[current_date - timedelta(days=1)]
                    new_tests = tr['new_state_tests'] + tr['new_external_tests']
                    total_tests = tr['total_tests']
                    # print('using shifted mdh timeseries')
                elif current_date in topline_timeseries_values:
                    tr = topline_timeseries_values[current_date]

                    new_tests = tr['cumulative_completed_tests'] - previous_total_tests
                    total_tests = tr['cumulative_completed_tests']

                else:
                    new_tests = 0
                    total_tests = previous_total_tests

                previous_total_tests = total_tests

                row = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'total_positive_tests': total_cases,
                    'new_positive_tests': new_cases,
                    'removed_cases': topline_data['removed_cases'],
                    'total_hospitalized': topline_data['cumulative_hospitalized'],
                    'currently_hospitalized': topline_data['currently_hospitalized'],
                    'currently_in_icu': topline_data['currently_in_icu'],
                    'total_statewide_deaths': topline_data['cumulative_statewide_deaths'],
                    'new_statewide_deaths': new_deaths,
                    'total_statewide_recoveries': topline_data['cumulative_statewide_recoveries'],
                    'total_completed_tests': total_tests,
                    'new_completed_tests': new_tests,
                }
                writer.writerow(row)

                current_date += timedelta(days=1)
