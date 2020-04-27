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
        # table = soup.find_all('table')[3]
        table = soup.find("th", text="County").find_parent("table")

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) > 0:
                county_name = tds[0].text
                county_cases = tds[1].text
                county_deaths = tds[2].text
                county_data.append((county_name, county_cases, county_deaths))

        return county_data

    def update_county_records(self, county_data):
        msg_output = ''

        today = datetime.date.today()
        num_counties_with_cases = len(county_data)
        msg_output += '{} counties with at least 1 confirmed case\n\n'.format(num_counties_with_cases)

        for observation in county_data:
            previous_county_observation = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip(), scrape_date__lt=today).order_by('-scrape_date').first()
            if previous_county_observation:
                previous_county_cases_total = previous_county_observation.cumulative_count
                previous_county_deaths_total = previous_county_observation.cumulative_deaths
            else:
                previous_county_cases_total = 0
                previous_county_deaths_total = 0

            daily_cases = self.parse_comma_int(observation[1]) - previous_county_cases_total
            daily_deaths = self.parse_comma_int(observation[2]) - previous_county_deaths_total

            # Check if there is already an entry today
            try:
                county_observation = CountyTestDate.objects.get(
                    county__name__iexact=observation[0].strip(),
                    scrape_date=today
                )
                print('Updating {} County: {}'.format(observation[0], observation[1]))
                county_observation.daily_count = daily_cases
                county_observation.cumulative_count = self.parse_comma_int(observation[1])
                county_observation.daily_deaths = daily_deaths
                county_observation.cumulative_deaths = self.parse_comma_int(observation[2])
                # county_observation.cumulative_deaths = cumulative_deaths
                county_observation.save()
            except ObjectDoesNotExist:
                try:
                    print('Creating 1st {} County record of day: {}'.format(observation[0], self.parse_comma_int(observation[1])))
                    county_observation = CountyTestDate(
                        county=County.objects.get(name__iexact=observation[0].strip()),
                        scrape_date=today,
                        daily_count=daily_cases,
                        cumulative_count=self.parse_comma_int(observation[1]),
                        daily_deaths=daily_deaths,
                        cumulative_deaths=self.parse_comma_int(observation[2]),
                    )
                    county_observation.save()
                except Exception as e:
                    slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                    raise

            # Now calculate cumulative deaths after adding latest daily deaths
            # cumulative_deaths = CountyTestDate.objects.filter(county__name__iexact=observation[0].strip()).aggregate(Sum('daily_deaths'))['daily_deaths__sum']
            # if cumulative_deaths:
            #     county_observation.cumulative_deaths = cumulative_deaths
            #     county_observation.save()

            # Slack lastest results
            case_change_text = ''
            if county_observation.daily_count != 0:
                optional_plus = '+'
                if county_observation.daily_count < 0:
                    optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE *** '
                elif county_observation.daily_count == county_observation.cumulative_count:
                    optional_plus = ':heavy_plus_sign: NEW COUNTY '

                case_change_text = ' (:point_right: {}{} today)'.format(optional_plus, county_observation.daily_count)

            deaths_change_text = ''
            if int(county_observation.cumulative_deaths) > 0:
                deaths_change_text = ', {} death'.format(county_observation.cumulative_deaths)
                if int(county_observation.cumulative_deaths) > 1:
                    deaths_change_text += 's' # pluralize

                if county_observation.daily_deaths != 0:
                    optional_plus = '+'
                    if county_observation.daily_deaths < 0:
                        optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE '
                    elif county_observation.daily_deaths == county_observation.cumulative_deaths:
                        optional_plus = ':heavy_plus_sign: NEW COUNTY '

                    deaths_change_text += ' (:point_right: {}{} today)'.format(optional_plus, county_observation.daily_deaths)

            # print('{}: {}{}\n'.format(county_observation.county.name, county_observation.cumulative_count, case_change_text))
            msg_output = msg_output + '{}: {} cases{}{}\n'.format(
                county_observation.county.name,
                county_observation.cumulative_count,
                case_change_text,
                deaths_change_text
            )

        return 'COVID scraper county-by-county results: \n\n' + msg_output

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

    def detail_tables_regex(self, soup, th_text):
        table = soup.find("th", text=th_text).find_parent("table")
        rows = table.find_all("tr")
        num_rows = len(rows)
        for k, row in enumerate(rows):
            if k == 0:
                first_row = row.find_all("th")
            if k == num_rows - 1:
                last_row = row.find_all(['th', 'td'])

        col_names = [th.text for th in first_row]
        last_row_values = {}
        for k, c in enumerate(col_names):
            last_row_values[c] = last_row[k].text
        return last_row_values

    def ages_table_parser(self, soup):
        ''' should work on multiple columns '''
        table = soup.find("th", text='Age Group').find_parent("table")
        rows = table.find_all("tr")
        num_rows = len(rows)
        data_rows = []
        for k, row in enumerate(rows):
            if k == 0:
                first_row = row.find_all("th")
                col_names = [th.text for th in first_row]
            else:
                data_row = {}
                cells = row.find_all(["th", "td"])
                for k, c in enumerate(col_names):
                    data_row[c] = cells[k].text
                data_rows.append(data_row)

        return data_rows


    def p_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def parse_comma_int(self, input_str):
        return int(input_str.replace(',', ''))

    def get_statewide_data(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        output = {}

        hosp_table_latest = self.detail_tables_regex(soup, 'Hospitalized in ICU (daily)')
        output['cumulative_hospitalized'] = self.parse_comma_int(hosp_table_latest['Total hospitalizations'])
        output['currently_in_icu'] = self.parse_comma_int(hosp_table_latest['Hospitalized in ICU (daily)'])
        output['currently_non_icu_hospitalized'] = self.parse_comma_int(hosp_table_latest['Hospitalized, not in ICU (daily)'])  # Not used except to add up
        output['currently_hospitalized'] = output['currently_in_icu'] + output['currently_non_icu_hospitalized']

        deaths_table_latest = self.detail_tables_regex(soup, 'Total deaths')
        output['cumulative_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Total deaths'])

        recoveries_table_latest = self.detail_tables_regex(soup, 'No longer needing isolation')
        output['cumulative_statewide_recoveries'] = self.parse_comma_int(recoveries_table_latest['No longer needing isolation'])

        # ages
        # ages_data = self.ages_table_parser(soup)
        # output['cases_age_0_5'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == '0-5 years'][0]
        # output['cases_age_6_19'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == '6-19 years'][0]
        # output['cases_age_20_44'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == '20-44 years'][0]
        # output['cases_age_45_64'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == '45-64 years'][0]
        # output['cases_age_65_plus'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == '65+ years'][0]
        # output['cases_age_unknown'] = [g['Number of cases'] for g in ages_data if g['Age Group'] == 'Unknown/ missing'][0]

        ps = soup.find_all('p')
        for p in ps:
            cumulative_positive_tests_match = self.p_regex('Total positive', p.text)
            if cumulative_positive_tests_match:
                output['cumulative_positive_tests'] = cumulative_positive_tests_match

        uls = soup.find_all('ul')
        for ul in uls:
            cumulative_completed_mdh_match = self.ul_regex('Total approximate number of completed tests from the MDH Public Health Lab', ul.text)
            if cumulative_completed_mdh_match:
                output['cumulative_completed_mdh'] = cumulative_completed_mdh_match

            cumulative_completed_private_match = self.ul_regex('Total approximate number of completed tests from external laboratories', ul.text)
            if cumulative_completed_private_match:
                output['cumulative_completed_private'] = cumulative_completed_private_match

        # print(cumulative_hospitalized, currently_hospitalized, currently_in_icu)
        return output

    def change_sign(self, input_int):
        optional_plus = ''
        if input_int != 0:
            optional_plus = '+'
            if input_int < 0:
                optional_plus = ':rotating_light: '
        return '{}{}'.format(optional_plus, f'{input_int:,}')

    def update_statewide_records(self, statewide_data):
        previous_statewide_results = StatewideTotalDate.objects.all().order_by('-scrape_date').first()

        today = datetime.date.today()
        total_statewide_tests = statewide_data['cumulative_completed_mdh'] + statewide_data['cumulative_completed_private']
        try:
            current_statewide_observation = StatewideTotalDate.objects.get(
                scrape_date=today
            )
            print('Updating existing statewide for {}'.format(today))

            current_statewide_observation.cumulative_positive_tests = statewide_data['cumulative_positive_tests']
            current_statewide_observation.cumulative_completed_tests = total_statewide_tests
            current_statewide_observation.cumulative_completed_mdh = statewide_data['cumulative_completed_mdh']
            current_statewide_observation.cumulative_completed_private = statewide_data['cumulative_completed_private']
            current_statewide_observation.cumulative_hospitalized = statewide_data['cumulative_hospitalized']
            current_statewide_observation.currently_hospitalized = statewide_data['currently_hospitalized']
            current_statewide_observation.currently_in_icu = statewide_data['currently_in_icu']
            current_statewide_observation.cumulative_statewide_deaths = statewide_data['cumulative_statewide_deaths']
            current_statewide_observation.cumulative_statewide_recoveries = statewide_data['cumulative_statewide_recoveries']
            # current_statewide_observation.cases_age_0_5 = statewide_data['cases_age_0_5']
            # current_statewide_observation.cases_age_6_19 = statewide_data['cases_age_6_19']
            # current_statewide_observation.cases_age_20_44 = statewide_data['cases_age_20_44']
            # current_statewide_observation.cases_age_45_64 = statewide_data['cases_age_45_64']
            # current_statewide_observation.cases_age_65_plus = statewide_data['cases_age_65_plus']
            # current_statewide_observation.cases_age_unknown = statewide_data['cases_age_unknown']

            current_statewide_observation.save()
        except ObjectDoesNotExist:
            try:
                print('Creating 1st statewide record for {}'.format(today))
                current_statewide_observation = StatewideTotalDate(
                    cumulative_positive_tests=statewide_data['cumulative_positive_tests'],
                    cumulative_completed_tests=total_statewide_tests,
                    cumulative_completed_mdh=statewide_data['cumulative_completed_mdh'],
                    cumulative_completed_private=statewide_data['cumulative_completed_private'],
                    cumulative_hospitalized=statewide_data['cumulative_hospitalized'],
                    currently_hospitalized=statewide_data['currently_hospitalized'],
                    currently_in_icu=statewide_data['currently_in_icu'],
                    cumulative_statewide_deaths=statewide_data['cumulative_statewide_deaths'],
                    cumulative_statewide_recoveries=statewide_data['cumulative_statewide_recoveries'],

                    # cases_age_0_5 = statewide_data['cases_age_0_5'],
                    # cases_age_6_19 = statewide_data['cases_age_6_19'],
                    # cases_age_20_44 = statewide_data['cases_age_20_44'],
                    # cases_age_45_64 = statewide_data['cases_age_45_64'],
                    # cases_age_65_plus = statewide_data['cases_age_65_plus'],
                    # cases_age_unknown = statewide_data['cases_age_unknown'],

                    scrape_date=today,
                )
                current_statewide_observation.save()
            except Exception as e:
                slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                raise

        msg_output = ''
        if (previous_statewide_results.cumulative_positive_tests != current_statewide_observation.cumulative_positive_tests):
            new_cases = current_statewide_observation.cumulative_positive_tests - previous_statewide_results.cumulative_positive_tests
            new_deaths = current_statewide_observation.cumulative_statewide_deaths - previous_statewide_results.cumulative_statewide_deaths
            hospitalizations_change = current_statewide_observation.currently_hospitalized - previous_statewide_results.currently_hospitalized
            icu_change = current_statewide_observation.currently_in_icu - previous_statewide_results.currently_in_icu
            new_tests = current_statewide_observation.cumulative_completed_tests - previous_statewide_results.cumulative_completed_tests

            print('Change found, composing statewide Slack message...')
            msg_output += '*{}* deaths total (*{}* today)\n'.format(f'{current_statewide_observation.cumulative_statewide_deaths:,}', self.change_sign(new_deaths))
            msg_output += '*{}* cases total (*{}* today)\n'.format(f'{current_statewide_observation.cumulative_positive_tests:,}', self.change_sign(new_cases))
            msg_output += '*{}* currently hospitalized (*{}* today)\n'.format(f'{current_statewide_observation.currently_hospitalized:,}', self.change_sign(hospitalizations_change))
            msg_output += '*{}* currently in ICU (*{}* today)\n'.format(f'{current_statewide_observation.currently_in_icu:,}', self.change_sign(icu_change))
            msg_output += '*{}* total tests completed (*{}* today)\n'.format(f'{current_statewide_observation.cumulative_completed_tests:,}', self.change_sign(new_tests))

            return 'COVID scraper found updated data on the <https://www.health.state.mn.us/diseases/coronavirus/situation.html|MDH situation page>...\n\n' + msg_output + '\n'

        return 'COVID scraper: No updates found in statewide numbers.\n\n'


    def handle(self, *args, **options):
        html = self.get_page_content()
        if not html:
            slack_latest('WARNING: Scraper error. Not proceeding.', '#robot-dojo')
        else:
            # Save a copy of HTML
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            with open(os.path.join(settings.BASE_DIR, 'exports', 'html', 'situation_{}.html').format(now), 'wb') as html_file:
                html_file.write(html)
                html_file.close()

            previous_statewide_cases = StatewideTotalDate.objects.order_by('-scrape_date').first().cumulative_positive_tests

            statewide_data = self.get_statewide_data(html)
            statewide_msg_output = self.update_statewide_records(statewide_data)

            county_data = self.get_county_data(html)
            county_msg_output = self.update_county_records(county_data)
            # county_msg_output = "Leaving county counts at yesterday's total until state clarifies"

            print(statewide_data['cumulative_positive_tests'], previous_statewide_cases)
            if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
                new_statewide_cases = statewide_data['cumulative_positive_tests'] - previous_statewide_cases
                # slack_header = '*{} new cases announced statewide.*\n\n'.format(new_statewide_cases)
                slack_latest(statewide_msg_output + county_msg_output, '#virus')
                slack_latest(statewide_msg_output + county_msg_output, '#covid-tracking')
            else:
                # slack_latest(statewide_msg_output + county_msg_output, '#robot-dojo')
                # slack_latest('Scraper update: No county changes detected.', '#covid-tracking')
                # slack_latest(statewide_msg_output + county_msg_output, '#virus')  # Force output anyway
                slack_latest('COVID scraper update: No changes detected.', '#robot-dojo')
