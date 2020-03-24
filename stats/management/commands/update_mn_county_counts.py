import requests
import datetime
from bs4 import BeautifulSoup

from django.core.management.base import BaseCommand
from stats.models import County, CountyTestDate


class Command(BaseCommand):
    help = 'Check for new or updated results by date from Minnesota Department of Health table: https://www.health.state.mn.us/diseases/coronavirus/situation.html'

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

    def handle(self, *args, **options):
        html = self.get_page_content()
        if not html:
            print('WARNING: Scraper error. Not proceeding.')
        else:
            county_data = self.get_county_data(html)

            today = datetime.date.today()
            print(today)

            for observation in county_data:
                previous_county_observation = CountyTestDate.objects.filter(county__name__iexact=observation[0], scrape_date__lt=today).order_by('-scrape_date').first()
                if previous_county_observation:
                    previous_county_total = previous_county_observation.cumulative_count
                else:
                    previous_county_total = 0

                daily_count = int(observation[1]) - previous_county_total

                # Check if there is already an entry today
                try:
                    existing_today_observation = CountyTestDate.objects.get(
                        county__name__iexact=observation[0],
                        scrape_date=today
                    )
                    print('Updating {} County: {}'.format(observation[0], observation[1]))
                    existing_today_observation.daily_count = daily_count
                    existing_today_observation.cumulative_count = observation[1]
                    existing_today_observation.save()
                except:
                    print('Creating 1st {} County record of day: {}'.format(observation[0], observation[1]))
                    new_observation = CountyTestDate(
                        county=County.objects.get(name__iexact=observation[0]),
                        scrape_date=today,
                        daily_count=daily_count,
                        cumulative_count=observation[1]
                    )
                    new_observation.save()
