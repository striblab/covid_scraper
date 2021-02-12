import datetime
from bs4 import BeautifulSoup

from django.core.management.base import BaseCommand
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist

from stats.models import County, CountyTestDate
from stats.utils import get_situation_page_content, timeseries_table_parser, parse_comma_int, slack_latest, updated_today

class Command(BaseCommand):
    help = '''County data, broken out from the situation page.'''

    def get_county_data(self, soup):
        county_data = []
        county_table = soup.find("table", {'id': 'maptable'})
        county_list = timeseries_table_parser(county_table)

        for county in county_list:
            county_name = ' '.join(county['County'].split()).replace(' County', '')
            if county_name != 'Unknown/missing':
                county_data.append({
                    'county': county_name,
                    'cumulative_count': parse_comma_int(county['Total cases']),
                    'cumulative_confirmed_cases': parse_comma_int(county['Total confirmed cases']),
                    'cumulative_probable_cases': parse_comma_int(county['Total probable cases']),
                    'cumulative_deaths': parse_comma_int(county['Total deaths']),
                })

        return county_data

    def update_county_records(self, county_data, update_date):
        msg_output = ''

        today = datetime.date.today()

        bool_any_changes = False  # Used to decide on Slack alert
        for observation in county_data:
            previous_county_observation = CountyTestDate.objects.filter(county__name__iexact=observation['county'].strip(), scrape_date__lt=today).order_by('-scrape_date').first()
            if previous_county_observation:
                previous_county_cases_total = previous_county_observation.cumulative_count
                previous_county_deaths_total = previous_county_observation.cumulative_deaths
            else:
                previous_county_cases_total = 0
                previous_county_deaths_total = 0

            daily_cases = observation['cumulative_count'] - previous_county_cases_total
            daily_deaths = observation['cumulative_deaths'] - previous_county_deaths_total

            # Check if there is already an entry today
            try:
                county_observation = CountyTestDate.objects.get(
                    county__name__iexact=observation['county'].strip(),
                    scrape_date=today
                )
                print('Updating {} County: {}'.format(observation['county'], observation['cumulative_count']))
                county_observation.update_date = update_date
                county_observation.daily_total_cases = daily_cases
                county_observation.cumulative_count = observation['cumulative_count']
                county_observation.daily_deaths = daily_deaths
                county_observation.cumulative_deaths = observation['cumulative_deaths']

                county_observation.cumulative_confirmed_cases = observation['cumulative_confirmed_cases']
                county_observation.cumulative_probable_cases = observation['cumulative_probable_cases']

                county_observation.save()
            except ObjectDoesNotExist:
                try:
                    print('Creating 1st {} County record of day: {}'.format(observation['county'], observation['cumulative_count']))
                    bool_any_changes = True
                    county_observation = CountyTestDate(
                        county=County.objects.get(name__iexact=observation['county'].strip()),
                        scrape_date=today,
                        update_date=update_date,
                        daily_total_cases=daily_cases,
                        cumulative_count=observation['cumulative_count'],
                        daily_deaths=daily_deaths,
                        cumulative_deaths=observation['cumulative_deaths'],

                        cumulative_confirmed_cases = observation['cumulative_confirmed_cases'],
                        cumulative_probable_cases = observation['cumulative_probable_cases'],
                    )
                    county_observation.save()
                except Exception as e:
                    slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                    raise

        return bool_any_changes

    def change_sign(self, input_int):
        optional_plus = ''
        if not input_int:
            return 0
        elif input_int != 0:
            optional_plus = '+'
            if input_int < 0:
                optional_plus = ':rotating_light: '
        return '{}{}'.format(optional_plus, f'{input_int:,}')

    def slack_county_of_interest(self, record, channel):
        msg = '*{} County, {}*\n'.format(record.county.name, record.scrape_date.strftime('%B %d, %Y'))
        msg += f'*{record.cumulative_count:,}* cases total (change of *{self.change_sign(record.daily_total_cases)}* today)\n'
        msg += f'*{record.cumulative_deaths:,}* deaths total (change of *{self.change_sign(record.daily_deaths)}* today)\n'
        msg += '\n\n\n'

        slack_latest(msg, channel)
        # slack_latest(msg, '#robot-dojo')

    def handle(self, *args, **options):
        html = get_situation_page_content()
        if not html:
            slack_latest("COVID scraper ERROR: update_mn_county_data.py can't find page HTML. Not proceeding.", '#robot-dojo')
        else:

            soup = BeautifulSoup(html, 'html.parser')
            bool_updated_today, update_date = updated_today(soup)

            if bool_updated_today:
                print('Updated today')
                county_data = self.get_county_data(soup)

                if len(county_data) > 0:
                    bool_any_changes = self.update_county_records(county_data, update_date)

                    if bool_any_changes:
                        slack_latest('*COVID daily update*\n\n', '#duluth_live')
                        for county in ['St. Louis', 'Carlton', 'Itasca', 'Lake', 'Cook']:
                            record = CountyTestDate.objects.get(county__name=county, scrape_date=datetime.date.today())
                            self.slack_county_of_interest(record, '#duluth_live')

                        slack_latest('*COVID daily update*\n\n', '#stcloud_live')
                        for county in ['Stearns', 'Benton', 'Sherburne']:
                            record = CountyTestDate.objects.get(county__name=county, scrape_date=datetime.date.today())
                            self.slack_county_of_interest(record, '#stcloud_live')

                else:
                    slack_latest('COVID scraper warning: No county records found.', '#robot-dojo')
            else:
                print('No update yet today')
