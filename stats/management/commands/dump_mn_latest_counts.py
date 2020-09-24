import os
import re
import csv
import json
import datetime

from django.conf import settings

from django.db.models import Max, Count, Sum
from django.core.management.base import BaseCommand
from stats.models import County, AgeGroupPop, CountyTestDate, StatewideAgeDate, StatewideTotalDate, StatewideDeathsDate, Death


class Command(BaseCommand):
    help = 'Dump a CSV of the latest cumulative count of statewide and county-by-county data.'

    def per_1k(self, numerator, pop):
        return round(float(numerator) / (float(pop) / 1000.0), 2)

    def dump_county_latest(self):
        rows = []
        for c in County.objects.all().order_by('name'):
            latest_observation = CountyTestDate.objects.filter(county=c).order_by('-scrape_date').first()
            if latest_observation:

                row = {
                    'county_fips': c.fips,
                    'county_name': c.name,
                    'total_positive_tests': latest_observation.cumulative_count,
                    'total_deaths': latest_observation.cumulative_deaths,
                    'cases_per_1k': self.per_1k(latest_observation.cumulative_count, c.pop_2019),
                    'deaths_per_1k': self.per_1k(latest_observation.cumulative_deaths, c.pop_2019),
                    'pop_2019': c.pop_2019,
                    'latitude': c.latitude,
                    'longitude': c.longitude,
                }
                rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_positive_tests_by_county.csv'), 'w') as csvfile:
            fieldnames = ['county_fips', 'county_name', 'total_positive_tests', 'total_deaths', 'cases_per_1k', 'deaths_per_1k', 'pop_2019', 'latitude', 'longitude']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_counties_latest.json'), 'w') as jsonfile:
            jsonfile.write(json.dumps(rows))

    def dump_state_latest(self):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_statewide_latest.csv'), 'w') as csvfile:
            fieldnames = [
                'total_confirmed_cases',
                # 'daily_positive_tests',
                # 'daily_removed_tests',
                'cases_daily_change',
                'daily_cases_newly_reported',
                'daily_cases_removed',
                'total_statewide_deaths',
                'daily_statewide_deaths',
                'total_statewide_recoveries',
                'total_completed_tests',
                # 'total_completed_mdh',
                # 'total_completed_private',
                'total_hospitalized',
                'currently_hospitalized',
                'currently_in_icu',
                'last_update',
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            latest = StatewideTotalDate.objects.all().order_by('-scrape_date').first()
            latest_deaths = StatewideDeathsDate.objects.filter(scrape_date=latest.scrape_date).order_by('-reported_date').first()
            writer.writerow({
                'total_confirmed_cases': latest.cumulative_positive_tests,
                'cases_daily_change': latest.cases_daily_change,
                'daily_cases_newly_reported': latest.cases_newly_reported,
                # 'daily_positive_tests': latest.new_cases,
                'daily_cases_removed': latest.removed_cases,
                'total_statewide_deaths': latest_deaths.total_deaths,
                'daily_statewide_deaths': latest_deaths.new_deaths,
                'total_statewide_recoveries': latest.cumulative_statewide_recoveries,
                'total_completed_tests': latest.cumulative_completed_tests,
                # 'total_completed_mdh': latest.cumulative_completed_mdh,
                # 'total_completed_private': latest.cumulative_completed_private,
                'total_hospitalized': latest.cumulative_hospitalized,
                # 'currently_hospitalized': latest.currently_hospitalized,
                # 'currently_in_icu': latest.currently_in_icu,
                'currently_hospitalized': None,
                'currently_in_icu': None,
                'last_update': latest.last_update
            })

    def round_special(self, input_int):
        if input_int > 0 and input_int < 1:
            return -1
        else:
            return round(input_int)

    def build_ages_row(self, age_group, case_count, death_count, total_case_count, total_death_count, pct_state_pop):
        # case_count = group_records.aggregate(Sum('case_count'))['case_count__sum']
        # death_count = group_records.aggregate(Sum('case_count'))['case_count__sum']
        cases_pct = self.round_special(100 * (float(case_count) / float(total_case_count)))
        deaths_pct = self.round_special(100 * (float(death_count) / float(total_death_count)))
        # if row.case_count and not row.cases_pct:
        #     cases_pct = self.round_special(100 * (float(row.case_count) / float(total_case_count)))
        # else:
        #     cases_pct = row.cases_pct
        #
        # if row.death_count and not row.deaths_pct:
        #     death_pct = self.round_special(100 * (float(row.death_count) / float(total_death_count)))
        # else:
        #     death_pct = row.deaths_pct

        return {
            'age_group': age_group,
            'cases': case_count,
            'deaths': death_count,
            'pct_of_cases': cases_pct,
            'pct_of_deaths': deaths_pct,
            'pct_state_pop': pct_state_pop
        }

    def dump_ages_latest(self):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_ages_latest.csv'), 'w') as csvfile:

            fieldnames = [
                'age_group',
                'cases',
                'deaths',
                'pct_of_cases',
                'pct_of_deaths',
                'pct_state_pop'
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            max_date = StatewideAgeDate.objects.aggregate(Max('scrape_date'))['scrape_date__max']
            topline_data = StatewideTotalDate.objects.get(scrape_date=max_date)
            total_case_count = topline_data.cumulative_positive_tests
            total_death_count = topline_data.cumulative_statewide_deaths

            age_groups = AgeGroupPop.objects.all().order_by('pk')
            rows = []
            # age_groups = StatewideAgeDate.objects.filter(scrape_date=datetime.date.today()).order_by('pk')
            for a in age_groups:

                # Need to combine age brackets on data side

                # print(a.age_group)
                print(a.age_group)
                group_records = StatewideAgeDate.objects.filter(
                    age_min__gte=a.age_min,
                    age_max__lte=a.age_max,
                    scrape_date=max_date
                )

                case_count = group_records.aggregate(Sum('case_count'))['case_count__sum']
                death_count = group_records.aggregate(Sum('death_count'))['death_count__sum']

                rows.append(self.build_ages_row(a.age_group, case_count, death_count, total_case_count, total_death_count, a.pct_pop))

                # cases_pct = self.round_special(100 * (float(case_count) / float(total_case_count)))
                # deaths_pct = self.round_special(100 * (float(death_count) / float(total_death_count)))
                #
                # case_count = group_records.aggregate(Sum('case_count'))['case_count__sum']
                # death_count = group_records.aggregate(Sum('case_count'))['case_count__sum']
                #
                # rows.append({
                #     'age_group': a.age_group,
                #     'cases': case_count,
                #     'deaths': death_count,
                #     'pct_of_cases': cases_pct,
                #     'pct_of_deaths': death_pct,
                #     'pct_state_pop': a.pct_pop
                # })

                # rows.append(self.build_ages_row(lr, total_case_count, total_death_count, a.pct_pop))

            missing = StatewideAgeDate.objects.get(age_group='Unknown/missing', scrape_date=max_date)
            # writer.writerow({
            #     'age_group': missing.age_group,
            #     'cases': lr.case_count,
            #     'deaths': lr.death_count,
            #     'pct_of_cases': missing.cases_pct,
            #     'pct_of_deaths': missing.deaths_pct,
            #     'pct_state_pop': 'N/A'
            # })
            rows.append(self.build_ages_row(missing.age_group, missing.case_count, missing.death_count, total_case_count, total_death_count, 'N/A'))

            writer.writerows(rows)

            with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_ages_latest.json'), 'w') as jsonfile:
                jsonfile.write(json.dumps(rows))

    def dump_detailed_death_ages_latest(self):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_death_ages_detailed_latest.csv'), 'w') as csvfile:

            fieldnames = [
                'age_group',
                'num_deaths',
                'pct_of_deaths',
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            total_deaths = Death.objects.all().count()
            age_group_totals = Death.objects.all().values('age_group').annotate(total=Count('pk')).order_by('age_group')
            # print(age_group_totals)

            for ag in age_group_totals:
                ag['age_start_int'] = int(re.match(r'([0-9]+)', ag['age_group']).group(0))

            rows = []
            for ag in sorted(age_group_totals, key = lambda i: i['age_start_int']):
                # print(ag['age_start_int'])
                rows.append({
                    'age_group': ag['age_group'],
                    'num_deaths': ag['total'],
                    'pct_of_deaths': self.round_special(100 * (ag['total'] / total_deaths)),
                })
            writer.writerows(rows)

            with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_death_ages_detailed_latest.json'), 'w') as jsonfile:
                jsonfile.write(json.dumps(rows))

    def handle(self, *args, **options):
        self.dump_county_latest()
        self.dump_state_latest()
        self.dump_ages_latest()
        # self.dump_detailed_death_ages_latest()
