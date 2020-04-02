import re
import os
import csv
import requests
import datetime
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen

from django.db.models import Sum
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from stats.models import County, CountyTestDate, StatewideTotalDate
from stats.utils import slack_latest

from django.conf import settings


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
        table = soup.find_all('table')[2]

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) > 0:
                county_name = tds[0].text
                county_count = tds[1].text
                county_data.append((county_name, county_count))

        return county_data

    def update_county_records(self, county_data):
        msg_output = ''
        deaths_obj = self.get_death_csv()
        # for row in deaths_obj:
        #     print(row['DATE'])
        if not deaths_obj:
            print('WARNING: Cannot retrieve deaths data. Not proceeding.')
        else:
            today = datetime.date.today()
            for observation in county_data:
                previous_county_observation = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip(), scrape_date__lt=today).order_by('-scrape_date').first()
                if previous_county_observation:
                    previous_county_total = previous_county_observation.cumulative_count
                else:
                    previous_county_total = 0

                daily_count = int(observation[1]) - previous_county_total

                # Get death count for this county/date
                daily_deaths = self.find_matching_deaths(deaths_obj, today, observation[0].strip())
                # print('daily deaths: {}'.format(daily_deaths))
                cumulative_deaths = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip()).aggregate(Sum('daily_deaths'))['daily_deaths__sum']
                if not cumulative_deaths:
                    cumulative_deaths = daily_deaths
                # print('cumulative deaths: {}'.format(cumulative_deaths))

                # Check if there is already an entry today
                try:
                    county_observation = CountyTestDate.objects.get(
                        county__name__iexact=observation[0].strip(),
                        scrape_date=today
                    )
                    print('Updating {} County: {}'.format(observation[0], observation[1]))
                    county_observation.daily_count = daily_count
                    county_observation.cumulative_count = observation[1]
                    county_observation.daily_deaths = daily_deaths
                    county_observation.cumulative_deaths = cumulative_deaths
                    county_observation.save()
                except ObjectDoesNotExist:
                    try:
                        print('Creating 1st {} County record of day: {}'.format(observation[0], observation[1]))
                        county_observation = CountyTestDate(
                            county=County.objects.get(name__iexact=observation[0].strip()),
                            scrape_date=today,
                            daily_count=daily_count,
                            cumulative_count=observation[1],
                            daily_deaths=daily_deaths,
                            cumulative_deaths=cumulative_deaths,
                        )
                        county_observation.save()
                    except Exception as e:
                        slack_latest('SCRAPER ERROR: {}'.format(e))
                        raise

                # Slack lastest results
                change_text = ''
                if county_observation.daily_count != 0:
                    optional_plus = '+'
                    if county_observation.daily_count < 0:
                        optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE *** '
                    elif county_observation.daily_count == county_observation.cumulative_count:
                        optional_plus = ':heavy_plus_sign: NEW COUNTY '

                    change_text = ' (:point_right: {}{} today)'.format(optional_plus, county_observation.daily_count)

                print('{}: {}{}\n'.format(county_observation.county.name, county_observation.cumulative_count, change_text))
                msg_output = msg_output + '{}: {}{}\n'.format(county_observation.county.name, county_observation.cumulative_count, change_text)

        return msg_output

    def get_death_csv(self):
        stream = urlopen(self.DEATH_DATA_URL)
        csvfile = csv.DictReader(codecs.iterdecode(stream, 'utf-8'))
        return list(csvfile)

    def find_matching_deaths(self, deaths_obj, date, county_name):
        for row in deaths_obj:
            # print(row['COUNTY'], datetime.datetime.strptime(row['DATE'], '%m/%d/%Y').date(), date, county_name)
            if row['COUNTY'] == county_name and datetime.datetime.strptime(row['DATE'], '%m/%d/%Y').date() == date:
                return row['NUM_DEATHS']
        return 0

    def ul_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def get_statewide_data(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        uls = soup.find_all('ul')

        output = {}
        for ul in uls:
            # print(ul.text)
            cumulative_positive_tests_match = self.ul_regex('Total positive', ul.text)
            if cumulative_positive_tests_match:
                output['cumulative_positive_tests'] = cumulative_positive_tests_match

            # Not in a ul
            # cumulative_completed_tests_match = self.ul_regex('Total approximate number of completed tests', ul.text)
            # if cumulative_completed_tests_match:
            #     output['cumulative_completed_tests'] = cumulative_completed_tests_match

            cumulative_completed_mdh_match = self.ul_regex('Total approximate number of completed tests from the MDH Public Health Lab', ul.text)
            if cumulative_completed_mdh_match:
                output['cumulative_completed_mdh'] = cumulative_completed_mdh_match

            cumulative_completed_private_match = self.ul_regex('Total approximate number of completed tests from external laboratories', ul.text)
            if cumulative_completed_private_match:
                output['cumulative_completed_private'] = cumulative_completed_private_match

            cumulative_statewide_deaths_match = self.ul_regex('Deaths', ul.text)
            if cumulative_statewide_deaths_match:
                output['cumulative_statewide_deaths'] = cumulative_statewide_deaths_match

            cumulative_statewide_recoveries_match = self.ul_regex('Patients who no longer need to be isolated', ul.text)
            if cumulative_statewide_recoveries_match:
                output['cumulative_statewide_recoveries'] = cumulative_statewide_recoveries_match

            cumulative_hospitalized_match = self.ul_regex('Total cases requiring hospitalization', ul.text)
            if cumulative_hospitalized_match:
                output['cumulative_hospitalized'] = cumulative_hospitalized_match

            currently_hospitalized_match = self.ul_regex('Hospitalized as of today', ul.text)
            if currently_hospitalized_match:
                output['currently_hospitalized'] = currently_hospitalized_match

            currently_in_icu_match = self.ul_regex('Hospitalized in ICU as of today', ul.text)
            if currently_in_icu_match:
                output['currently_in_icu'] = currently_in_icu_match

        # print(cumulative_hospitalized, currently_hospitalized, currently_in_icu)
        return output

    def update_statewide_records(self, statewide_data):
        today = datetime.date.today()
        total_statewide_tests = statewide_data['cumulative_completed_mdh'] + statewide_data['cumulative_completed_private']
        try:
            existing_today_observation = StatewideTotalDate.objects.get(
                scrape_date=today
            )
            print('Updating existing statewide for {}'.format(today))

            existing_today_observation.cumulative_positive_tests = statewide_data['cumulative_positive_tests']
            existing_today_observation.cumulative_completed_tests = total_statewide_tests
            existing_today_observation.cumulative_completed_mdh = statewide_data['cumulative_completed_mdh']
            existing_today_observation.cumulative_completed_private = statewide_data['cumulative_completed_private']
            existing_today_observation.cumulative_hospitalized = statewide_data['cumulative_hospitalized']
            existing_today_observation.currently_hospitalized = statewide_data['currently_hospitalized']
            existing_today_observation.currently_in_icu = statewide_data['currently_in_icu']
            existing_today_observation.cumulative_statewide_deaths = statewide_data['cumulative_statewide_deaths']
            existing_today_observation.cumulative_statewide_recoveries = statewide_data['cumulative_statewide_recoveries']

            existing_today_observation.save()
        except ObjectDoesNotExist:
            try:
                print('Creating 1st statewide record for {}'.format(today))
                new_observation = StatewideTotalDate(
                    cumulative_positive_tests=statewide_data['cumulative_positive_tests'],
                    cumulative_completed_tests=total_statewide_tests,
                    cumulative_completed_mdh=statewide_data['cumulative_completed_mdh'],
                    cumulative_completed_private=statewide_data['cumulative_completed_private'],
                    cumulative_hospitalized=statewide_data['cumulative_hospitalized'],
                    currently_hospitalized=statewide_data['currently_hospitalized'],
                    currently_in_icu=statewide_data['currently_in_icu'],
                    cumulative_statewide_deaths=statewide_data['cumulative_statewide_deaths'],
                    cumulative_statewide_recoveries=statewide_data['cumulative_statewide_recoveries'],
                    scrape_date=today,
                )
                new_observation.save()
            except Exception as e:
                slack_latest('SCRAPER ERROR: {}'.format(e))
                raise

        return


    def handle(self, *args, **options):
        html = self.get_page_content()
        if not html:
            slack_latest('WARNING: Scraper error. Not proceeding.')
        else:
            # Save a copy of HTML
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            with open(os.path.join(settings.BASE_DIR, 'exports', 'html', 'situation_{}.html').format(now), 'wb') as html_file:
                html_file.write(html)
                html_file.close()

            previous_statewide_cases = StatewideTotalDate.objects.order_by('-scrape_date').first().cumulative_positive_tests

            statewide_data = self.get_statewide_data(html)
            self.update_statewide_records(statewide_data)

            county_data = self.get_county_data(html)
            msg_output = self.update_county_records(county_data)


            if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
                new_statewide_cases = statewide_data['cumulative_positive_tests'] - previous_statewide_cases
                slack_header = '*{} new cases announced statewide.*\n\n'.format(new_statewide_cases)
                slack_latest(slack_header + msg_output)
            else:
                slack_latest('Scraper update: No county changes detected.')
