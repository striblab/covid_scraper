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
                # print(date)
                total_as_of_date = CountyTestDate.objects.filter(scrape_date=date['scrape_date']).aggregate(Sum('case_count'))['case_count__sum']
                # print(total_as_of_date)
                new_cases = total_as_of_date - previous_total
                # print(total_as_of_date, new_cases)
                row = {
                    'date': date['scrape_date'].strftime('%Y-%m-%d'),
                    'total_positive_tests': total_as_of_date,
                    'new_positive_tests': new_cases
                }
                writer.writerow(row)
