import os
from datetime import datetime
import csv

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'One-time import of existing data that was manually compiled'


    def handle(self, *args, **options):
        pass
        with open(os.path.join(settings.BASE_DIR, 'data', 'mn-covid-tracker-manual-entry.csv'), 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            records = {}
            for row in reader:
                # print(row)
                if row['COUNTY'] != '':
                    if row['DATE'] not in records.keys():
                        records[row['DATE']] = {
                            row['COUNTY']: 1
                        }

                    elif row['COUNTY'] not in records[row['DATE']].keys():
                        records[row['DATE']][row['COUNTY']] = 1

                    else:
                        records[row['DATE']][row['COUNTY']] += 1

            print(records)

            for text_date, counties in records.items():
                parsed_date = datetime.strptime(text_date, '%m/%d/%Y').date()
                for county, daily_count in counties.items():
                    county_obj = County.objects.get(name__iexact=county)
                    # Get existing total to add to
                    previous_total_obj = CountyTestDate.objects.filter(county=county_obj, scrape_date__lt=parsed_date).order_by('-scrape_date').first()
                    if previous_total_obj:
                        print('{} + {}'.format(previous_total_obj.cumulative_count, daily_count))
                        new_total = previous_total_obj.cumulative_count + daily_count
                    else:
                        new_total = daily_count
                        print(daily_count)

                    print(parsed_date, county, new_total)
                    obj, created = CountyTestDate.objects.update_or_create(
                        county=county_obj,
                        scrape_date=parsed_date,
                        defaults={'daily_count': daily_count, 'cumulative_count': new_total}
                    )
                    # obj = CountyTestDate(
                    #     county=county_obj,
                    #     scrape_date=parsed_date,
                    #     daily_count=daily_count,
                    #     cumulative_count=new_total
                    # )
                    # obj.save()

                # else:
                #     records[row['DATE']]
        #     fieldnames = ['date', 'total_positive_tests', 'new_positive_tests']
        #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #     writer.writeheader()
        #
        #     previous_total = 0
        #     for date in CountyTestDate.objects.all().order_by('scrape_date').values('scrape_date').distinct():
        #         # print(date)
        #         total_as_of_date = CountyTestDate.objects.filter(scrape_date=date['scrape_date']).aggregate(Sum('case_count'))['case_count__sum']
        #         # print(total_as_of_date)
        #         new_cases = total_as_of_date - previous_total
        #         # print(total_as_of_date, new_cases)
        #         row = {
        #             'date': date['scrape_date'].strftime('%Y-%m-%d'),
        #             'total_positive_tests': total_as_of_date,
        #             'new_positive_tests': new_cases
        #         }
        #         writer.writerow(row)
