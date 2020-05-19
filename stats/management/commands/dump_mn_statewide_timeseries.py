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
            fieldnames = ['date', 'total_confirmed_cases', 'cases_daily_change', 'cases_newly_reported', 'cases_removed', 'cases_sample_date', 'cases_total_sample_date', 'total_hospitalized', 'currently_hospitalized', 'currently_in_icu', 'total_statewide_deaths', 'new_statewide_deaths', 'total_statewide_recoveries', 'total_completed_tests', 'new_completed_tests']
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

            total_cases_sample_date = 0
            previous_total_deaths = 0
            previous_total_tests = 0
            # Go through all dates and check for either timeseries or, failing that, topline data
            while current_date <= max_date:

                # print(current_date)
                topline_data = topline_timeseries_values[current_date]
                # print(current_date, topline_data['update_date'])
                if current_date < datetime.date.today() or topline_data['update_date'] == datetime.date.today():
                    # Don't output today if an update hasn't run yet today

                    if topline_data['new_deaths'] == 0:
                        new_deaths = topline_data['cumulative_statewide_deaths'] - previous_total_deaths
                    else:
                        new_deaths = topline_data['new_deaths']
                    previous_total_deaths = topline_data['cumulative_statewide_deaths']

                    if current_date in cases_timeseries_values:
                        # print('timeseries')
                        cr = cases_timeseries_values[current_date]
                        new_cases_sample_date = cr['new_cases']
                        total_cases_sample_date = cr['total_cases']
                        # total_cases = cr['total_cases']
                        previous_total_cases_sample_date = total_cases_sample_date
                    else:
                        # This will usually just be today's values because no samples have come back yet
                        new_cases_sample_date = 0
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

#                     cases_daily_change <- Difference between total yesterday and today
# cases_newly_reported <- "new" cases per MDH, should add up to daily change when combined with cases_removed
# cases_removed <- MDH removals
# cases_sample_date <- Data from time series, which will lag by several days

                    row = {
                        'date': current_date.strftime('%Y-%m-%d'),
                        'total_confirmed_cases': total_cases,
                        'cases_daily_change': topline_data['cases_daily_change'],
                        'cases_newly_reported': topline_data['cases_newly_reported'],
                        'cases_removed': topline_data['removed_cases'],
                        'cases_sample_date': new_cases_sample_date,
                        'cases_total_sample_date': total_cases_sample_date,

                        # 'new_positive_tests': new_cases,
                        # 'removed_cases': topline_data['removed_cases'],
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
