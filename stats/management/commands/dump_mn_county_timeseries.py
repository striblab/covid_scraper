import os
import csv
import datetime
from datetime import timedelta, date

from django.conf import settings
from django.db.models import Min, Max
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'Export county time series'

    today_statewide_cases = 0  # Default

    def daterange(self, date1, date2):
        for n in range(int ((date2 - date1).days)+1):
            yield date1 + timedelta(n)

    # def dump_wide_timeseries(self):
    #     ''' Currently deprecated '''
    #     print('Dumping wide county timeseries...')
    #     dates = list(self.daterange(CountyTestDate.objects.all().order_by('scrape_date').first().scrape_date, datetime.date.today()))
    #
    #     fieldnames = ['county'] + [d.strftime('%Y-%m-%d') for d in dates]
    #     rows = []
    #
    #     for c in CountyTestDate.objects.all().values('county__name').distinct().order_by('county__name'):
    #         row = {'county': c['county__name']}
    #         for d in dates:
    #             if d == datetime.date.today() and self.today_statewide_cases == 0:
    #                 pass  # Ignore if there's no new results for today
    #             else:
    #                 most_recent_observation = CountyTestDate.objects.filter(county__name=c['county__name'], scrape_date__lte=d).order_by('-scrape_date').first()
    #                 if most_recent_observation:
    #                     row[d.strftime('%Y-%m-%d')] = most_recent_observation.cumulative_count
    #                 else:
    #                     row[d.strftime('%Y-%m-%d')] = 0
    #
    #         rows.append(row)
    #
    #     with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_county_timeseries.csv'), 'w') as csvfile:
    #
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #         writer.writeheader()
    #         writer.writerows(rows)

    def dump_all_counties_timeseries(self):
        print('Dumping all-county timeseries...')
        start_date = CountyTestDate.objects.aggregate(Min('scrape_date'))['scrape_date__min']
        end_date = datetime.date.today()
        dates = list(self.daterange(start_date, end_date))

        # print(start_date)
        records_by_county = []
        for c in County.objects.all().order_by('name'):
            records = CountyTestDate.objects.filter(county=c).values('scrape_date', 'county__name', 'daily_count', 'cumulative_count', 'daily_deaths', 'cumulative_deaths')
            # print(records)
            for d in dates:
                if d == datetime.date.today() and self.today_statewide_cases == 0:
                    pass  # Ignore if there's no new results for today
                else:
                    try:
                        county_date_record = [r for r in records if r['scrape_date'] == d][0]
                    except:
                        county_date_record = {
                            'scrape_date': d,
                            'county__name': c.name,
                            'daily_count': 0,
                            'cumulative_count': 0,
                            'daily_deaths': 0,
                            'cumulative_deaths': 0
                        }
                    records_by_county.append(county_date_record)
        # print(records_by_county)

        fieldnames = ['date', 'county', 'daily_cases', 'cumulative_cases', 'daily_deaths', 'cumulative_deaths']
        rows = []
        for r in records_by_county:
            row = {
                'date': r['scrape_date'].strftime('%Y-%m-%d'),
                'county': r['county__name'],
                'daily_cases': r['daily_count'],
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
        fieldnames = ['date', 'county', 'daily_cases', 'cumulative_cases', 'daily_deaths', 'cumulative_deaths']
        rows = []

        for c in CountyTestDate.objects.all().order_by('scrape_date', 'county__name'):
            if c.scrape_date == datetime.date.today() and self.today_statewide_cases == 0:
                pass  # Ignore if there's no new results for today
            else:
                # print(c.scrape_date, c.county.name, c.cumulative_count)
                row = {
                    'date': c.scrape_date.strftime('%Y-%m-%d'),
                    'county': c.county.name,
                    'daily_cases': c.daily_count,
                    'cumulative_cases': c.cumulative_count,
                    'daily_deaths': c.daily_deaths,
                    'cumulative_deaths': c.cumulative_deaths
                }
                rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data', 'mn_county_timeseries_tall.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def handle(self, *args, **options):
        today_statewide = StatewideTotalDate.objects.filter(scrape_date=datetime.date.today())
        previous_statewide = StatewideTotalDate.objects.all().order_by('-scrape_date')[1]
        if today_statewide and previous_statewide:
            self.today_statewide_cases = today_statewide[0].cumulative_positive_tests - previous_statewide.cumulative_positive_tests
            if self.today_statewide_cases > 0:
                print("Updates found for today, including today's date.")
            else:
                print("No new updates found yet for today, ignoring today's date.")

        self.dump_tall_timeseries()
        # self.dump_wide_timeseries()
        self.dump_all_counties_timeseries()
