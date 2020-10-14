import os
import csv
import json
import datetime
from datetime import timedelta, date

from django.conf import settings
from django.db.models import Sum, Min, Max, Avg, F, RowRange, ValueRange, Window, Case, When, Value, FloatField
from django.db.models.functions import Lead
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'Export county time series'

    today_statewide_cases = 0  # Default

    def daterange(self, date1, date2):
        for n in range(int ((date2 - date1).days)+1):
            yield date1 + timedelta(n)

    def dump_all_counties_timeseries(self):
        print('Dumping all-county timeseries...')
        start_date = CountyTestDate.objects.aggregate(Min('scrape_date'))['scrape_date__min']
        end_date = datetime.date.today()
        dates = list(self.daterange(start_date, end_date))

        # print(start_date)
        records_by_county = []
        for c in County.objects.all().order_by('name'):
            records = CountyTestDate.objects.filter(county=c).values('scrape_date', 'update_date', 'county__name', 'daily_total_cases', 'cumulative_count', 'daily_deaths', 'cumulative_deaths')
            county_running_total = 0
            # print(records)
            for d in dates:
                if d == datetime.date.today() and self.today_statewide_cases == 0:
                    pass  # Ignore if there's no new results for today
                else:
                    try:
                        county_date_record = [r for r in records if r['scrape_date'] == d][0]
                        county_running_total = county_date_record['cumulative_count']
                    except:
                        # print('missing date', county_running_total, d)
                        county_date_record = {
                            'scrape_date': d,
                            'update_date': d,
                            'county__name': c.name,
                            'daily_total_cases': 0,
                            'cumulative_count': county_running_total,
                            'daily_deaths': 0,
                            'cumulative_deaths': 0
                        }
                    records_by_county.append(county_date_record)

        fieldnames = ['date', 'county', 'daily_cases', 'cumulative_cases', 'daily_deaths', 'cumulative_deaths']
        rows = []
        for r in records_by_county:
            # if not r['update_date'] or r['update_date'] <= datetime.date.today():
            row = {
                'date': r['scrape_date'].strftime('%Y-%m-%d'),
                'county': r['county__name'],
                'daily_cases': r['daily_total_cases'],
                'cumulative_cases': r['cumulative_count'],
                'daily_deaths': r['daily_deaths'],
                'cumulative_deaths': r['cumulative_deaths']
            }
            rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_county_timeseries_all_counties.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def dump_tall_timeseries(self):
        print('Dumping tall county timeseries...')
        fieldnames = ['date', 'county', 'daily_cases', 'cumulative_cases', 'cases_per_1k', 'daily_deaths', 'cumulative_deaths', 'cases_rolling',
'deaths_rolling', 'cases_weekly_chg', 'cases_weekly_per_1k', 'cases_weekly_pct_chg',]
        # 'pct_chg', 'pct_chg_7day',
        rows = []

        # TODO: county by county
        for county in County.objects.all():
            for c in CountyTestDate.objects.filter(county=county).annotate(
                cases_rolling=Window(
                    expression=Avg('daily_total_cases'),
                    order_by=F('scrape_date').asc(),
                    frame=RowRange(start=-6,end=0)
                )
            ).annotate(
                deaths_rolling=Window(
                    expression=Avg('daily_deaths'),
                    order_by=F('scrape_date').asc(),
                    frame=RowRange(start=-6,end=0)
                )
            ).annotate(
                cases_total_weekago=Window(
                    expression=Lead('cumulative_count', offset=7, default=0),
                    order_by=F('scrape_date').desc()
                )
            ).annotate(
                cases_weekly_pct_chg=Case(
                    When(cases_total_weekago__gt=0, then=(F('cumulative_count') - F('cases_total_weekago') * 1.0) / F('cases_total_weekago')),
                    default=Value(0),
                    output_field=FloatField()
                )
            ).values(
                'scrape_date',
                'update_date',
                'county__name',
                'daily_total_cases',
                'cumulative_count',
                'daily_deaths',
                'cumulative_deaths',
                'cases_rolling',
                'deaths_rolling',
                # 'pct_chg',
                # 'pct_chg_7day',
                'cases_total_weekago',
                'cases_weekly_pct_chg',
            ).order_by('scrape_date', 'county__name'):
                # print(c['update_date'], datetime.date.today(), c['update_date'] == datetime.date.today())
                # if not c['update_date'] or c['update_date'] <= datetime.date.today():
                if c['scrape_date'] < datetime.date.today() or c['update_date'] == datetime.date.today():
                #     pass  # Ignore if there's no new results for today
                # else:
                #     # print(c.scrape_date, c.county.name, c.cumulative_count)
                    # print(c['county__name'], c['daily_total_cases'], c['pct_chg'], c['pct_chg_7day'])
                    cases_weekly_change = c['cumulative_count'] - c['cases_total_weekago']
                    if c['cumulative_count'] < 100:
                        cases_weekly_pct_chg = None
                    else:
                        cases_weekly_pct_chg = round(c['cases_weekly_pct_chg'], 3)

                    row = {
                        'date': c['scrape_date'].strftime('%Y-%m-%d'),
                        'county': c['county__name'],
                        'daily_cases': c['daily_total_cases'],
                        'cumulative_cases': c['cumulative_count'],
                        'cases_per_1k': round(c['cumulative_count'] / (county.pop_2019 / 1000), 1),
                        'daily_deaths': c['daily_deaths'],
                        'cumulative_deaths': c['cumulative_deaths'],
                        'cases_rolling': round(c['cases_rolling'], 1),
                        'deaths_rolling': round(c['deaths_rolling'], 2),
                        'cases_weekly_chg': cases_weekly_change,
                        'cases_weekly_per_1k': round(cases_weekly_change / (county.pop_2019 / 1000), 1),
                        'cases_weekly_pct_chg': cases_weekly_pct_chg,
                    }
                    rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_county_timeseries_tall.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        # JSON export: Shorten col names
        skinny_rows = []
        for row in rows:
            skinny_rows.append({
                'date': row['date'],
                'county': row['county'],
                'daily_cases': row['daily_cases'],
                'cases': row['cumulative_cases'],
                'cases_per_1k': row['cases_per_1k'],
                'daily_deaths': row['daily_deaths'],
                'deaths': row['cumulative_deaths'],
                'cases_rolling': row['cases_rolling'],
                'deaths_rolling': row['deaths_rolling'],
                'cases_weekly_chg': row['cases_weekly_chg'],
                'cases_weekly_per_1k': row['cases_weekly_per_1k'],
                'cases_weekly_pct_chg': row['cases_weekly_pct_chg'],
            })

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_county_timeseries.json'), 'w') as jsonfile:
            jsonfile.write(json.dumps(skinny_rows))

    def handle(self, *args, **options):
        today_statewide = StatewideTotalDate.objects.filter(scrape_date=datetime.date.today())
        previous_statewide = StatewideTotalDate.objects.all().values('scrape_date', 'cumulative_positive_tests').order_by('-scrape_date')[1]
        if today_statewide and previous_statewide:
            self.today_statewide_cases = today_statewide[0].cumulative_positive_tests - previous_statewide['cumulative_positive_tests']
            if self.today_statewide_cases > 0:
                print("Updates found for today, including today's date.")
            else:
                print("No new updates found yet for today, ignoring today's date.")

        self.dump_tall_timeseries()
        # self.dump_wide_timeseries()
        self.dump_all_counties_timeseries()
