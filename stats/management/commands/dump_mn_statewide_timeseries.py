import os
import csv

from django.conf import settings
from django.db.models import Sum
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'Calculate change per day to export cumulative and daily counts'


    def handle(self, *args, **options):
        with open(os.path.join(settings.BASE_DIR, 'exports', 'mn_statewide_timeseries.csv'), 'w') as csvfile:
            fieldnames = ['date', 'total_positive_tests', 'new_positive_tests']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            previous_total = 0
            for date in CountyTestDate.objects.all().order_by('scrape_date').values('scrape_date').distinct():
                # Get the totals for each county as of that date. The latest observation may be an earlier date.
                county_totals = 0
                for c in County.objects.all():
                    latest_county_observation = CountyTestDate.objects.filter(county=c, scrape_date__lte=date['scrape_date']).order_by('-scrape_date').first()
                    if latest_county_observation:
                        county_totals += latest_county_observation.cumulative_count

                new_cases = county_totals - previous_total
                previous_total = county_totals

                row = {
                    'date': date['scrape_date'].strftime('%Y-%m-%d'),
                    'total_positive_tests': county_totals,
                    'new_positive_tests': new_cases
                }
                writer.writerow(row)
