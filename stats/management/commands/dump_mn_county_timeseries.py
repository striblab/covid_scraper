import os
import csv
import datetime
from datetime import timedelta, date

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'Export county time series'

    def daterange(self, date1, date2):
        for n in range(int ((date2 - date1).days)+1):
            yield date1 + timedelta(n)

    def dump_wide_timeseries(self):
        dates = list(self.daterange(CountyTestDate.objects.all().order_by('scrape_date').first().scrape_date, datetime.date.today()))

        fieldnames = ['county'] + [d.strftime('%Y-%m-%d') for d in dates]
        rows = []

        for c in CountyTestDate.objects.all().values('county__name').distinct().order_by('county__name'):
            row = {'county': c['county__name']}
            for d in dates:
                most_recent_observation = CountyTestDate.objects.filter(county__name=c['county__name'], scrape_date__lte=d).order_by('-scrape_date').first()
                if most_recent_observation:
                    row[d.strftime('%Y-%m-%d')] = most_recent_observation.cumulative_count
                else:
                    row[d.strftime('%Y-%m-%d')] = 0

            rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_county_timeseries.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def dump_tall_timeseries(self):
        fieldnames = ['date', 'county', 'daily_cases', 'cumulative_cases', 'daily_deaths', 'cumulative_deaths']
        rows = []
        for c in CountyTestDate.objects.all().order_by('scrape_date', 'county__name'):
            print(c.scrape_date, c.county.name, c.cumulative_count)
            row = {
                'date': c.scrape_date.strftime('%Y-%m-%d'),
                'county': c.county.name,
                'daily_cases': c.daily_count,
                'cumulative_cases': c.cumulative_count,
                'daily_deaths': c.daily_deaths,
                'cumulative_deaths': c.cumulative_deaths
            }
            rows.append(row)

        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_county_timeseries_tall.csv'), 'w') as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def handle(self, *args, **options):
        self.dump_tall_timeseries()
        # self.dump_wide_timeseries()
