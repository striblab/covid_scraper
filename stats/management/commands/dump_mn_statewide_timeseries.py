import os
import csv
import json
import datetime
import pandas as pd
from datetime import timedelta

from django.conf import settings
from django.db.models import Min, Max, Avg, F, RowRange, Window
from django.core.management.base import BaseCommand
# from django.db.models import Avg, F, RowRange, Window
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
        # print(cases_timeseries_values)

        # Tests: Get max scrape date for each real date
        tests_timeseries_values = {}
        tests_reported_dates = StatewideTestsDate.objects.all().values_list('reported_date', flat=True).distinct()
        # test_records = StatewideTestsDate.objects.filter(reported_date__in=tests_reported_dates).order_by('scrape_date')
        # TODO: How to calculate rolling averages
        for t in tests_reported_dates:
            latest_record = StatewideTestsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            tests_timeseries_values[t] = latest_record

        # Hospitalizations: Get max scrape date for each real date
        hosp_timeseries_values = {}
        hosp_reported_dates = StatewideHospitalizationsDate.objects.all().values_list('reported_date', flat=True).distinct()
        for t in hosp_reported_dates:
            latest_record = StatewideHospitalizationsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            hosp_timeseries_values[t] = latest_record

        hosp_totals_values = {}
        hosp_scrape_dates =  StatewideHospitalizationsDate.objects.all().values_list('scrape_date', flat=True).distinct()
        for t in hosp_scrape_dates:
            try:
                total_record = StatewideHospitalizationsDate.objects.get(scrape_date=t, reported_date=None).__dict__
            except:
                total_record = StatewideHospitalizationsDate.objects.filter(scrape_date=t).latest('reported_date').__dict__
            hosp_totals_values[t] = total_record

        # Deaths: Get max scrape date for each real date
        deaths_timeseries_values = {}
        deaths_reported_dates = StatewideDeathsDate.objects.all().values_list('reported_date', flat=True).distinct()
        for t in deaths_reported_dates:
            latest_record = StatewideDeathsDate.objects.filter(reported_date=t).values().latest('scrape_date')
            deaths_timeseries_values[t] = latest_record

        # Topline records: get all by date
        topline_timeseries_values = {}
        topline_data = StatewideTotalDate.objects.all().values()
        # .annotate(
        #     cases_daily_change_rolling=Window(
        #         expression=Avg('cases_daily_change'),
        #         order_by=F('scrape_date').asc(),
        #         frame=RowRange(start=-6,end=0)
        #     )
        # ).annotate(
        #     hosp_total_daily_rolling=Window(
        #         expression=Avg('hospitalized_total_daily_change'),
        #         order_by=F('scrape_date').asc(),
        #         frame=RowRange(start=-6,end=0)
        #     )
        # ).values()
        for s in topline_data:
        # for s in StatewideTotalDate.objects.all().values():
            topline_timeseries_values[s['scrape_date']] = s
        # print(topline_timeseries_values)

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

                # if topline_data['new_deaths'] == 0:
                #     print('dont have deaths')
                #     new_deaths = topline_data['cumulative_statewide_deaths'] - previous_total_deaths
                # else:
                #     print('yes deaths')
                if topline_data['cumulative_statewide_deaths'] > 0:
                    death_data = deaths_timeseries_values[current_date]
                    new_deaths = death_data['new_deaths']
                    new_deaths_rolling = round(death_data['new_deaths_rolling'], 1)
                else:
                    new_deaths = 0
                    new_deaths_rolling = ''

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

                if current_date in hosp_totals_values:
                    ht = hosp_totals_values[current_date]
                    hosp_total_daily_change = ht['total_hospitalizations'] - total_hospitalizations

                    if total_icu_admissions:
                        icu_total_daily_change = ht['total_icu_admissions'] - total_icu_admissions
                    else:
                        icu_total_daily_change = None

                    total_hospitalizations = ht['total_hospitalizations']
                    total_icu_admissions = ht['total_icu_admissions']
                else:
                    hosp_total_daily_change = topline_data['hospitalized_total_daily_change']
                    icu_total_daily_change = None

                    total_hospitalizations = topline_data['cumulative_hospitalized']
                    total_icu_admissions = None

                if current_date in hosp_timeseries_values:
                    # print('timeseries')
                    cr = hosp_timeseries_values[current_date]
                    new_hosp_admissions = cr['new_hosp_admissions']
                    new_icu_admissions = cr['new_icu_admissions']

                    # total_hospitalizations = cr['total_hospitalizations']
                    # total_icu_admissions = cr['total_icu_admissions']

                    # The daily totals don't work quite right because of "missing" dates
                    # try:
                    #     hosp_date_missing = StatewideHospitalizationsDate.objects.get(scrape_date=current_date, reported_date=None)
                    #
                    #     total_hospitalizations = hosp_date_missing.total_hospitalizations
                    #     total_icu_admissions = hosp_date_missing.total_icu_admissions
                    # except:
                    #     total_hospitalizations = cr['total_hospitalizations']
                    #     total_icu_admissions = cr['total_icu_admissions']
                else:
                    # This will usually just be today's values because no samples have come back yet
                    new_hosp_admissions = 0
                    new_icu_admissions = 0

                    # total_hospitalizations = 0
                    # total_icu_admissions = 0

                    # hosp_total_daily_change = 0
                    # icu_total_daily_change = 0

                    # removed_cases = topline_data['removed_cases']
                # Not sure if we need this anymore or not
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
                        new_tests_rolling = None
                        total_tests = tr['cumulative_completed_tests']

                    else:
                        new_tests = 0
                        total_tests = previous_total_tests

                    if new_tests > 0 and topline_data['cases_daily_change'] is not None:  # Test for null on cases_daily_change e.g. July 4
                        daily_pct_positive = round((topline_data['cases_daily_change']*1.0) / new_tests, 3)
                    else:
                        daily_pct_positive = None

                previous_total_tests = total_tests

                if current_date <= datetime.date(2020, 3, 28):
                    new_tests_rolling = None
                elif 'new_tests_rolling' in tr:
                    new_tests_rolling = round(tr['new_tests_rolling'], 1)
                else:
                    new_tests_rolling = None


                row = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'total_confirmed_cases': total_cases,
                    'cases_daily_change': topline_data['cases_daily_change'],
                    'cases_newly_reported': topline_data['cases_newly_reported'],
                    'cases_removed': topline_data['removed_cases'],
                    'cases_sample_date': new_cases_sample_date,
                    'cases_total_sample_date': total_cases_sample_date,
                    'new_hosp_admissions': new_hosp_admissions,
                    'new_icu_admissions': new_icu_admissions,
                    # 'hosp_total_daily_change': topline_data['hospitalized_total_daily_change'],
                    'hosp_total_daily_change': hosp_total_daily_change,
                    'icu_total_daily_change': icu_total_daily_change,

                    # 'total_icu_admissions': total_icu_admissions,
                    'total_hospitalized': total_hospitalizations,
                    'total_icu_admissions': total_icu_admissions,
                    'currently_hospitalized': topline_data['currently_hospitalized'],
                    'currently_in_icu': topline_data['currently_in_icu'],
                    'total_statewide_deaths': topline_data['cumulative_statewide_deaths'],
                    'new_statewide_deaths': new_deaths,
                    'new_statewide_deaths_rolling': new_deaths_rolling,  # Calculated during record creation
                    'total_statewide_recoveries': topline_data['cumulative_statewide_recoveries'],
                    'total_completed_tests': '' if current_date <= datetime.date(2020, 3, 28) else total_tests,
                    'new_completed_tests': '' if current_date <= datetime.date(2020, 3, 28) else new_tests,
                    'new_completed_tests_rolling': new_tests_rolling,
                    'daily_pct_positive': daily_pct_positive,
                }
                rows.append(row)
                # writer.writerow(row)

            current_date += timedelta(days=1)

        # Run through pandas to calculate rolling averages. For some of this it could be done in Django-land, but for positive pct, for example, it's hard to mix and match in a django query. So might as well do them all the same.
        ts_df = pd.DataFrame(rows)
        # ts_df['hosp_total_daily_change'] = ts_df['total_hospitalized'].diff()
        # ts_df['icu_total_daily_change'] = ts_df['total_icu_admissions'].diff()

        ts_df['daily_pct_positive_rolling'] = ts_df['daily_pct_positive'].rolling(window=7, min_periods=1).mean().round(3)
        ts_df['cases_daily_change_rolling'] = ts_df['cases_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['cases_sample_date_rolling'] = ts_df['cases_sample_date'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['hosp_total_daily_rolling'] = ts_df['hosp_total_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['icu_total_daily_rolling'] = ts_df['icu_total_daily_change'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_hosp_admissions_rolling'] = ts_df['new_hosp_admissions'].rolling(window=7, min_periods=1).mean().round(1)
        ts_df['new_icu_admissions_rolling'] = ts_df['new_icu_admissions'].rolling(window=7, min_periods=1).mean().round(1)

        # Put in a nice order...
        out_df = ts_df[[
            'date', 'total_confirmed_cases', 'cases_daily_change', 'cases_daily_change_rolling', 'cases_newly_reported', 'cases_removed', 'cases_sample_date', 'cases_sample_date_rolling', 'cases_total_sample_date', 'new_hosp_admissions', 'new_hosp_admissions_rolling', 'new_icu_admissions', 'new_icu_admissions_rolling', 'total_hospitalized', 'total_icu_admissions', 'currently_hospitalized', 'currently_in_icu', 'hosp_total_daily_change', 'hosp_total_daily_rolling', 'icu_total_daily_change', 'icu_total_daily_rolling', 'total_statewide_deaths', 'new_statewide_deaths', 'new_statewide_deaths_rolling', 'total_statewide_recoveries', 'total_completed_tests' , 'new_completed_tests', 'new_completed_tests_rolling', 'daily_pct_positive', 'daily_pct_positive_rolling'
        ]]

        out_df.to_csv(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_timeseries.csv'), index=False)
        out_df.to_json(os.path.join(settings.BASE_DIR, 'exports', 'mn_statewide_timeseries.json'), orient='records')
