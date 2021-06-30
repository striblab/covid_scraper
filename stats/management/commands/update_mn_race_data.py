import datetime
from bs4 import BeautifulSoup

from django.core.management.base import BaseCommand
from django.conf import settings

from stats.models import StatewideAgeDate
from stats.utils import get_situation_page_content, timeseries_table_parser, parse_comma_int, slack_latest

class Command(BaseCommand):
    help = 'Age data breakout from main scraper'

    def get_age_data(self, soup):
        age_table = soup.find("table", {'id': 'agetable'})
        ages_data = timeseries_table_parser(age_table)

        cleaned_ages_data = []
        for d in ages_data:
            d['Number of Cases'] = parse_comma_int(d['Number of Cases'])
            d['Number of Deaths'] = parse_comma_int(d['Number of Deaths'])
            cleaned_ages_data.append(d)

        return cleaned_ages_data

    def update_age_records(self, age_data):
        today = datetime.date.today()
        for a in age_data:
            try:
                current_observation = StatewideAgeDate.objects.get(
                  scrape_date=today,
                  age_group=a['Age Group']
                )
                print('Updating existing ages for {} on {}'.format(a['Age Group'], today))
            except:
                print('First age record for {} on {}'.format(a['Age Group'], today))
                current_observation = StatewideAgeDate(
                  scrape_date=today,
                  age_group=a['Age Group']
                )
            current_observation.case_count = a['Number of Cases']
            current_observation.death_count = a['Number of Deaths']
            current_observation.save()

        return 'COVID scraper: Age records updated.'

    def handle(self, *args, **options):
        html = get_situation_page_content()
        if not html:
            slack_latest("COVID scraper ERROR: update_mn_age_data.py can't find page HTML. Not proceeding.", '#robot-dojo')
        else:

            soup = BeautifulSoup(html, 'html.parser')

            age_data = self.get_age_data(soup)

            if len(age_data) > 0:
                age_msg_output = self.update_age_records(age_data)
                slack_latest(age_msg_output, '#robot-dojo')
            else:
                slack_latest('COVID scraper warning: No age records found.', '#robot-dojo')
