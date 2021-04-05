import re
import datetime
from bs4 import BeautifulSoup

from django.core.management.base import BaseCommand
from django.conf import settings
from django.db.models import Count

from stats.models import County, Death
from stats.utils import get_situation_page_content, timeseries_table_parser, parse_comma_int, slack_latest, updated_today

class Command(BaseCommand):
    help = '''Recent deaths data from the scraper. This isn't currently output anywhere but seems worth collecting.'''

    def get_recent_deaths_data(self, soup, update_date):
        # today = datetime.date.today()
        recent_deaths_table = soup.find("table", {'id': 'dailydeathar'})
        if recent_deaths_table:
            recent_deaths_ages = timeseries_table_parser(recent_deaths_table)

            cleaned_data = []

            for group in recent_deaths_ages:
                # County workaround
                if group['County of residence'] == 'Otter':
                    d_county_name = 'Otter Tail'
                elif group['County of residence'] == 'Unknown/missing':
                    d_county_name = None
                else:
                    d_county_name = re.sub('\s+', ' ', group['County of residence'])

                print(d_county_name)
                clean_row = {
                    'scrape_date': update_date,
                    'county__name': d_county_name,
                    'age_group': group['Age group'].replace(' years', '').strip(),
                    'count': int(group['Number of newly reported deaths']),
                }
                cleaned_data.append(clean_row)
            return cleaned_data
        else:
            # Find out if you have an error or if there really were zero deaths
            deaths_total_table = soup.find("table", {'id': 'dailydeathtotal'})
            last_td = deaths_total_table.find_all("td")[-1]
            num_deaths = int(last_td.text)
            if num_deaths == 0:
                print("Actual 0 death day, no error detected.")
                return 0
            else:
                print("WARNING: Couln't find recent deaths table but can't confirm 0.")
                return None
        return None

    def load_recent_deaths(self, scraped_deaths, update_date):
        existing_deaths = Death.objects.filter(scrape_date=update_date).values('scrape_date', 'county__name', 'age_group').annotate(count=Count('pk'))

        date_stripped_scraped = [{i:d[i] for i in d if i != 'scrape_date'} for d in scraped_deaths]
        date_stripped_existing = [{i:d[i] for i in d if i != 'scrape_date'} for d in existing_deaths]

        bool_changes = [i for i in date_stripped_scraped if i not in date_stripped_existing] != []

        if not bool_changes:
            print("No updates needed.")
        else:

            deaths_to_add = []
            deaths_to_remove = []
            for sd in scraped_deaths:
                similar = [ed for ed in existing_deaths if ed['county__name'] == sd['county__name'] and ed['age_group'] == sd['age_group']]
                # print('Similar: ', similar)
                if len(similar) == 0:
                    # add all to list
                    deaths_to_add.append(sd)
                else:
                    # Check if we have enough or too many for this grouping
                    unknown_deaths = sd['count'] - similar[0]['count']
                    if unknown_deaths > 0:
                        print('Adding {} deaths: {} {}'.format(unknown_deaths, sd['county__name'], sd['age_group']))
                        sd['count'] = unknown_deaths
                        deaths_to_add.append(sd)
                    elif unknown_deaths < 0:
                        print('WARNING: Subtracting {} deaths: {} {}'.format(unknown_deaths, sd['county__name'], sd['age_group']))
                        sd['count'] = unknown_deaths
                        sd['scrape_date'] = similar[0]['scrape_date']  # If you're going to remove, make sure it's from the max_date, not necessarily today
                        deaths_to_remove.append(sd)

            # Removing records
            for group in deaths_to_remove:
                remove_count = group['count']
                county = County.objects.get(name=group['county__name'])
                while remove_count < 0:
                    Death.objects.filter(scrape_date=group['scrape_date'], age_group=group['age_group'], county=county).last().delete()
                    remove_count += 1

            # Adding records
            new_deaths = []
            for group in deaths_to_add:
                add_count = group['count']
                if group['county__name']:
                    county = County.objects.get(name=group['county__name'])
                else:
                    county = None
                while add_count > 0:
                    death = Death(
                        scrape_date=group['scrape_date'],
                        age_group=group['age_group'],
                        county=county,
                    )
                    new_deaths.append(death)
                    add_count -= 1
            Death.objects.bulk_create(new_deaths)

    def handle(self, *args, **options):
        # archive_list = [
        #     'http://static.startribune.com.s3.amazonaws.com/news/projects/all/2021-covid-scraper/raw/html/situation_2021-03-26_1103.html',
        #     'http://static.startribune.com.s3.amazonaws.com/news/projects/all/2021-covid-scraper/raw/html/situation_2021-03-27_1103.html',
        #     'http://static.startribune.com.s3.amazonaws.com/news/projects/all/2021-covid-scraper/raw/html/situation_2021-03-28_1103.html'
        # ]
        # for url in archive_list:
            # html = get_situation_page_content(url)  # TEMP MANUAL OVERRIDE
        html = get_situation_page_content()  # TEMP MANUAL OVERRIDE
        if not html:
            slack_latest("COVID scraper ERROR: update_mn_recent_deaths.py can't find page HTML. Not proceeding.", '#robot-dojo')
        else:

            soup = BeautifulSoup(html, 'html.parser')
            # bool_updated_today, update_date = updated_today(soup, True)  # TEMP: MANUAL OVERRIDE
            bool_updated_today, update_date = updated_today(soup)  # TEMP: MANUAL OVERRIDE

            if bool_updated_today:
                print('Updated today')
                recent_deaths_data = self.get_recent_deaths_data(soup, update_date)
                if recent_deaths_data == None:
                    slack_latest('COVID scraper warning: No recent deaths records found.', '#robot-dojo')
                elif type(recent_deaths_data) == list and len(recent_deaths_data) > 0:
                    deaths_msg_output = self.load_recent_deaths(recent_deaths_data, update_date)
                    # slack_latest(deaths_msg_output, '#robot-dojo')
                else:
                    slack_latest('COVID scraper OK: Seems to be 0-death day.', '#robot-dojo')
            else:
                print('No update yet today')
