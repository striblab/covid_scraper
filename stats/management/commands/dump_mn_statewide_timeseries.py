import os
import csv
import datetime
import pandas as pd
from datetime import timedelta

from django.conf import settings
from django.db.models import Min, Max
from django.core.management.base import BaseCommand
from stats.models import StatewideTotalDate, StatewideCasesBySampleDate, StatewideTestsDate, StatewideDeathsDate, StatewideHospitalizationsDate


class Command(BaseCommand):
    help = 'Calculate change per day to export cumulative and daily counts'

    def handle(self, *args, **options):

        # Cases: Get max scrape date for each sample date
        cases_timeseries_values = {}
        cases_reported_dates = StatewideCasesBySampleDate.objects.all().values_list('sample_date', flat=True).distinct()
        for t in cases_reported_dates:
            latest_record = StatewideCasesBySampleDate.objects.filter(sample_date=t).values().latest('scrape_date')
            cases_timeseries_values[t] = latest_record

        # Tests: Get max scrape date for each real date
        tests_timeseries_values = {}
        tests_reported_dates = StatewideTestsDate.objects.all().values_list('reported_date', flat=True).distinct()
        for t in tests_reported_dates:
            latest_record = StatewideTestsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            tests_timeseries_values[t] = latest_record

        # Hospitalizations: Get max scrape date for each real date
        hosp_timeseries_values = {}
        hosp_reported_dates = StatewideHospitalizationsDate.objects.all().values_list('reported_date', flat=True).distinct()
        for t in hosp_reported_dates:
            latest_record = StatewideHospitalizationsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            hosp_timeseries_values[t] = latest_record

        # Deaths: Get max scrape date for each real date
        deaths_timeseries_values = {}
        deaths_reported_dates = StatewideDeathsDate.objects.all().values_list('reported_date', flat=True).distinct()
        for t in deaths_reported_dates:
            latest_record = StatewideDeathsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            deaths_timeseries_values[t] = latest_record

        # Topline records: get all by date
        topline_timeseries_values = {}
        topline_data = StatewideTotalDate.objects.all().values()

        for s in topline_data:
            topline_timeseries_values[s['scrape_date']] = s

        min_date = StatewideCasesBySampleDate.objects.all().aggregate(min_date=Min('sample_date'))['min_date']
        max_date = StatewideTotalDate.objects.filter(cumulative_positive_tests__gt=0).aggregate(max_date=Max('scrape_date'))['max_date']
        current_date = min_date

        total_cases_sample_date = 0
        previous_total_deaths = 0
        previous_total_tests = 0

        total_hospitalizations = 0
        total_icu_admissions = 0
        rows = []
        # Go through all dates and check for either timeseries or, failing that, topline data
        while current_date <= max_date:

            # print(current_date)
            topline_data = topline_timeseries_values[current_date]
            # print(current_date, topline_data['update_date'])
            if current_date < datetime.date.today() or topline_data['update_date'] == datetime.date.today():
                # Don't output today if an update hasn't run yet today
                if current_date in deaths_timeseries_values:
                    death_data = deaths_timeseries_values[current_date]
                    if not death_data['new_deaths']:
                        new_deaths = 0  # Translate nulls, which come on "no report" dates like holidays, to a 0 for output/rolling avg purposes
                    else:
                        new_deaths = death_data['new_deaths']
                else:
                    new_deaths = 0

                previous_total_deaths = topline_data['cumulative_statewide_deaths']

                if current_date in cases_timeseries_values:
                    cr = cases_timeseries_values[current_date]
                    new_cases_sample_date = cr['new_cases']
                    total_cases_sample_date = cr['total_cases']
                else:
                    # This will usually just be today's values because no samples have come back yet
                    new_cases_sample_date = 0

                if current_date in hosp_timeseries_values:
                    # print('timeseries')
                    cr = hosp_timeseries_values[current_date]
                    new_hosp_admissions = cr['new_hosp_admissions']
                    new_icu_admissions = cr['new_icu_admissions']

                else:
                    # This will usually just be today's values because no samples have come back yet
                    new_hosp_admissions = 0
                    new_icu_admissions = 0

                total_cases = topline_data['cumulative_positive_tests']

                if current_date <= datetime.date(2020, 3, 28):  # Temp conditional for old test dates
                    new_tests = 0
                    total_tests = 0
                    daily_pct_positive = None
                else:
                    if current_date - timedelta(days=1) in tests_timeseries_values:
                        tr = tests_timeseries_values[current_date - timedelta(days=1)]
                        new_tests = tr['new_tests']
                        total_tests = tr['total_tests']
                        # print('using shifted mdh timeseries')
                    elif current_date in topline_timeseries_values:
                        tr = topline_timeseries_values[current_date]

                        new_tests = tr['cumulative_completed_tests'] - previous_total_tests
                        # new_tests_rolling = None
                        total_tests = tr['cumulative_completed_tests']

                    else:
                        new_tests = 0
                        total_tests = previous_total_tests

                    if new_tests > 0 and topline_data['cases_daily_change'] is not None:  # Test for null on cases_daily_change e.g. July 4
                        daily_pct_positive = round((topline_data['cases_daily_change']*1.0) / new_tests, 3)
                    else:
                        daily_pct_positive = None

                previous_total_tests = total_tests

                row = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'total_confirmed_cases': topline_data['cumulative_positive_tests'],
                    'cases_daily_change': topline_data['cases_daily_change'],
                    'cases_newly_reported': topline_data['cases_newly_reported'],
                    'cases_removed': topline_data['removed_cases'],
                    'cases_sample_date': new_cases_sample_date,
                    'cases_total_sample_date': total_cases_sample_date,
                    'new_hosp_admissions': new_hosp_admissions,
                    'new_icu_admissions': new_icu_admissions,
                    'hosp_total_daily_change': topline_data['hospitalized_total_daily_change'],
                    'icu_total_daily_change': topline_data['icu_total_daily_change'],
                    'total_hospitalized': topline_data['cumulative_hospitalized'],
                    'total_icu_admissions': topline_data['cumulative_icu'],
                    'currently_hospitalized': topline_data['currently_hospitalized'],
                    'currently_in_icu': topline_data['currently_in_icu'],
                    'total_statewide_deaths': topline_data['cumulative_statewide_deaths'],
                    'new_statewide_deaths': new_deaths,
                    'total_statewide_recoveries': topline_data['cumulative_statewide_recoveries'],
                    'total_completed_tests': None if current_date <= datetime.date(2020, 3, 28) else total_tests,
                    'new_completed_tests': None if current_date <= datetime.date(2020, 3, 28) else new_tests,
                    'daily_pct_positive': daily_pct_positive,
                }
                rows.append(row)

            current_date += timedelta(days=1)

        # Run through pandas to calculate rolling averages.
        ts_df = pd.DataFrame(rows)
        ts_df['daily_pct_positive_rolling'] = ts_df['daily_pct_positive'].rolling(window=7, min_periods=1).mean().round(3)
        ts_df['cases_daily_change_rolling'] = ts_df['cases_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['cases_sample_date_rolling'] = ts_df['cases_sample_date'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['hosp_total_daily_rolling'] = ts_df['hosp_total_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['icu_total_daily_rolling'] = ts_df['icu_total_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_hosp_admissions_rolling'] = ts_df['new_hosp_admissions'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_icu_admissions_rolling'] = ts_df['new_icu_admissions'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_statewide_deaths_rolling'] = ts_df['new_statewide_deaths'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_completed_tests_rolling'] = ts_df['new_completed_tests'].astype('float').rolling(window=7, min_periods=1).mean().round(1)

        # Put in a nice order...
        out_df = ts_df[[
            'date', 'total_confirmed_cases', 'cases_daily_change', 'cases_daily_change_rolling', 'cases_newly_reported', 'cases_removed', 'cases_sample_date', 'cases_sample_date_rolling', 'cases_total_sample_date', 'new_hosp_admissions', 'new_hosp_admissions_rolling', 'new_icu_admissions', 'new_icu_admissions_rolling', 'total_hospitalized', 'total_icu_admissions', 'currently_hospitalized', 'currently_in_icu', 'hosp_total_daily_change', 'hosp_total_daily_rolling', 'icu_total_daily_change', 'icu_total_daily_rolling', 'total_statewide_deaths', 'new_statewide_deaths', 'new_statewide_deaths_rolling', 'total_statewide_recoveries', 'total_completed_tests' , 'new_completed_tests', 'new_completed_tests_rolling', 'daily_pct_positive', 'daily_pct_positive_rolling'
        ]]

        out_df.to_csv(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_timeseries.csv'), index=False)
        out_df.to_json(os.path.join(settings.BASE_DIR, 'exports', 'mn_statewide_timeseries.json'), orient='records')
