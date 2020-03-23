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
        # print(r.content)

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
            # print(county_data)

            today = datetime.date.today()
            print(today)

            for observation in county_data:
                # Check if there is already an entry today
                obj, created = CountyTestDate.objects.update_or_create(
                    county__name=observation[0],
                    scrape_datetime__date=today,
                    defaults={'case_count': observation[1]},
                )
