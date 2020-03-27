import csv
import requests
import datetime
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen

from django.db.models import Sum
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from stats.models import County, CountyTestDate
from stats.utils import slack_latest


class Command(BaseCommand):
    help = 'Check for new or updated results by date from Minnesota Department of Health table: https://www.health.state.mn.us/diseases/coronavirus/situation.html'

    DEATH_DATA_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT1zrZSE_O7GVQF6IXhrClF-Izj-hZHr3lsV5w63c0qomI3XfWjlTG_lYf-wf0ANjDedtd-7J5IZeMQ/pub?gid=86034077&single=true&output=csv'

    def get_page_content(self):
        headers = {'user-agent': 'Michael Corey, Star Tribune, michael.corey@startribune.com'}
        r = requests.get('https://www.health.state.mn.us/diseases/coronavirus/situation.html', headers=headers)
        if r.status_code == requests.codes.ok:
            return r.content
        else:
            return False

    def get_county_data(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        county_data = []

        # Right now this is the only table on the page
        table = soup.find('table')

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) > 0:
                county_name = tds[0].text
                county_count = tds[1].text
                county_data.append((county_name, county_count))

        return county_data

    def get_death_csv(self):
        stream = urlopen(self.DEATH_DATA_URL)
        csvfile = csv.DictReader(codecs.iterdecode(stream, 'utf-8'))
        return list(csvfile)

    def find_matching_deaths(self, deaths_obj, date, county_name):
        for row in deaths_obj:
            if row['COUNTY'] == county_name and datetime.datetime.strptime(row['DATE'], '%m/%d/%Y').date() == date:
                return row['NUM_DEATHS']
        return 0

    def handle(self, *args, **options):
        deaths_obj = self.get_death_csv()
        # for row in deaths_obj:
        #     print(row['DATE'])
        if not deaths_obj:
            print('WARNING: Cannot retrieve deaths data. Not proceeding.')
        else:

            html = self.get_page_content()
            if not html:
                print('WARNING: Scraper error. Not proceeding.')
            else:
                county_data = self.get_county_data(html)

                today = datetime.date.today()
                print(today)

                for observation in county_data:
                    previous_county_observation = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip(), scrape_date__lt=today).order_by('-scrape_date').first()
                    if previous_county_observation:
                        previous_county_total = previous_county_observation.cumulative_count
                    else:
                        previous_county_total = 0

                    daily_count = int(observation[1]) - previous_county_total

                    # Get death count for this county/date
                    daily_deaths = self.find_matching_deaths(deaths_obj, today, observation[0].strip())
                    cumulative_deaths = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip()).aggregate(Sum('daily_deaths'))['daily_deaths__sum']
                    if not cumulative_deaths:
                        cumulative_deaths = daily_deaths

                    # Check if there is already an entry today
                    try:
                        existing_today_observation = CountyTestDate.objects.get(
                            county__name__iexact=observation[0].strip(),
                            scrape_date=today
                        )
                        print('Updating {} County: {}'.format(observation[0], observation[1]))
                        existing_today_observation.daily_count = daily_count
                        existing_today_observation.cumulative_count = observation[1]
                        existing_today_observation.daily_deaths = daily_deaths
                        existing_today_observation.cumulative_deaths = cumulative_deaths
                        existing_today_observation.save()
                    except ObjectDoesNotExist:
                        try:
                            print('Creating 1st {} County record of day: {}'.format(observation[0], observation[1]))
                            new_observation = CountyTestDate(
                                county=County.objects.get(name__iexact=observation[0].strip()),
                                scrape_date=today,
                                daily_count=daily_count,
                                cumulative_count=observation[1],
                                daily_deaths=daily_deaths,
                                cumulative_deaths=cumulative_deaths,
                            )
                            new_observation.save()
                        except Exception as e:
                            slack_latest('SCRAPER ERROR: {}'.format(e))
                            raise
