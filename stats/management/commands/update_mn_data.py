import re
import os
import csv
import requests
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup

from django.db.models import Count
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from stats.models import County, CountyTestDate, StatewideAgeDate, StatewideTotalDate, Death, StatewideCasesBySampleDate, StatewideTestsDate, StatewideDeathsDate, StatewideHospitalizationsDate
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

    def ul_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def detail_tables_regex(self, table):
        # table = soup.find("th", text=th_text).find_parent("table")
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

    def parse_comma_int(self, input_str):
        if input_str == '-':
            return None
        else:
            return int(input_str.replace(',', ''))

    def parse_mdh_date(self, input_str, today):
        return datetime.datetime.strptime('{}/{}'.format(input_str, today.year), '%m/%d/%Y')

    def change_sign(self, input_int):
        optional_plus = ''
        if not input_int:
            return '+:shrug:'
        elif input_int != 0:
            optional_plus = '+'
            if input_int < 0:
                optional_plus = ':rotating_light: '
        return '{}{}'.format(optional_plus, f'{input_int:,}')

    def totals_table_parser(self, table):
        ''' First row is totals, others are breakouts '''
        data = {}
        rows = table.find_all("tr")
        for k, row in enumerate(rows):
            label = ' '.join(row.find("th").text.split())
            data[label] = row.find("td").text.strip()
        return data

    def timeseries_table_parser(self, table):
        ''' should work on multiple columns '''
        rows = table.find_all("tr")
        num_rows = len(rows)
        data_rows = []
        for k, row in enumerate(rows):
            if k == 0:
                first_row = row.find_all("th")
                col_names = [' '.join(th.text.split()).replace('<br>', ' ') for th in first_row]
            else:
                data_row = {}
                cells = row.find_all(["th", "td"])
                if len(cells) > 0:  # Filter out bad TRs
                    for k, c in enumerate(col_names):

                        data_row[c] = cells[k].text
                    data_rows.append(data_row)

        return data_rows

    def pct_filter(self, input_str):
        '''Removing pct sign, and changing <1 to -1'''
        if input_str == '<1%':
            return -1
        try:
            return int(input_str.replace('%', ''))
        except:
            return None

    def get_county_data(self, soup):
        county_data = []
        county_table = soup.find("table", {'id': 'maptable'})
        county_list = self.timeseries_table_parser(county_table)

        for county in county_list:
            county_name = ' '.join(county['County'].split()).replace(' County', '')
            if county_name != 'Unknown/missing':
                county_data.append({
                    'county': county_name,
                    'cumulative_count': self.parse_comma_int(county['Total cases']),
                    'cumulative_confirmed_cases': self.parse_comma_int(county['Total confirmed cases']),
                    'cumulative_probable_cases': self.parse_comma_int(county['Total probable cases']),
                    'cumulative_deaths': self.parse_comma_int(county['Total deaths']),
                })

        return county_data

    def update_county_records(self, county_data, update_date):
        msg_output = ''

        today = datetime.date.today()

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

            # # Slack lastest results
            # case_change_text = ''
            # if county_observation.daily_total_cases != 0:
            #     optional_plus = '+'
            #     if county_observation.daily_total_cases < 0:
            #         optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE *** '
            #     elif county_observation.daily_total_cases == county_observation.cumulative_count:
            #         optional_plus = ':heavy_plus_sign: NEW COUNTY '
            #
            #     case_change_text = ' (:point_right: {}{} today)'.format(optional_plus, county_observation.daily_total_cases)
            #
            # deaths_change_text = ''
            # if int(county_observation.cumulative_deaths) > 0:
            #     deaths_change_text = ', {} death'.format(county_observation.cumulative_deaths)
            #     if int(county_observation.cumulative_deaths) > 1:
            #         deaths_change_text += 's' # pluralize
            #
            #     if county_observation.daily_deaths != 0:
            #         optional_plus = '+'
            #         if county_observation.daily_deaths < 0:
            #             optional_plus = ':rotating_light::rotating_light: ALERT NEGATIVE '
            #         elif county_observation.daily_deaths == county_observation.cumulative_deaths:
            #             optional_plus = ':heavy_plus_sign: NEW COUNTY '
            #
            #         deaths_change_text += ' (:point_right: {}{} today)'.format(optional_plus, county_observation.daily_deaths)
            #
            # # print('{}: {}{}\n'.format(county_observation.county.name, county_observation.cumulative_count, case_change_text))
            # msg_output = msg_output + '{}: {} cases{}{}\n'.format(
            #     county_observation.county.name,
            #     f'{county_observation.cumulative_count:,}',
            #     case_change_text,
            #     deaths_change_text
            # )

        # final_msg = 'COVID scraper county-by-county results: \n\n' + msg_output
        # print(final_msg)

        return msg_output

    def get_statewide_cases_timeseries(self, soup, update_date):
        '''How to deal with back-dated statewide totals if they use sample dates'''
        print('Parsing statewide cases timeseries...')

        cases_table = soup.find("table", {'id': 'casetable'})
        cases_timeseries = self.timeseries_table_parser(cases_table)
        # print(cases_timeseries)
        if len(cases_timeseries) > 0:
            today = datetime.date.today()
            # Remove old records from today
            existing_today_records = StatewideCasesBySampleDate.objects.filter(scrape_date=today)
            print('Removing {} records of case timeseries data'.format(existing_today_records.count()))
            existing_today_records.delete()
            case_objs = []
            for c in cases_timeseries:
                if c['Specimen collection date'] == 'Unknown/missing':
                    sample_date = None
                else:
                    sample_date = self.parse_mdh_date(c['Specimen collection date'], today)

                new_pcr_tests = self.parse_comma_int(c['Confirmed cases (PCR positive)'])
                new_antigen_tests = self.parse_comma_int(c['Probable cases (Antigen positive)'])
                if new_antigen_tests:  # Handle old dates
                    new_cases = int(new_pcr_tests) + int(new_antigen_tests)
                else:
                    new_cases = new_pcr_tests

                co = StatewideCasesBySampleDate(
                    sample_date=sample_date,
                    new_cases=new_cases,
                    total_cases=self.parse_comma_int(c['Total positive cases (cumulative)']),

                    new_pcr_tests = new_pcr_tests,
                    new_antigen_tests = new_antigen_tests,
                    total_pcr_tests = self.parse_comma_int(c['Total confirmed cases (cumulative)']),
                    total_antigen_tests = self.parse_comma_int(c['Total probable cases (cumulative)']),

                    update_date=update_date,
                    scrape_date=today,
                )
                case_objs.append(co)
            print('Adding {} records of case timeseries data'.format(len(case_objs)))
            StatewideCasesBySampleDate.objects.bulk_create(case_objs)

    def get_statewide_tests_timeseries(self, soup, update_date):
        print('Parsing statewide tests timeseries...')

        tests_table = soup.find("table", {'id': 'labtable'})
        tests_timeseries = self.timeseries_table_parser(tests_table)

        if len(tests_timeseries) > 0:
            today = datetime.date.today()
            # Remove old records from today
            existing_today_records = StatewideTestsDate.objects.filter(scrape_date=today)
            print('Removing {} records of test timeseries data'.format(existing_today_records.count()))
            existing_today_records.delete()
            test_objs = []
            for c in tests_timeseries:
                if c['Date reported to MDH'] == 'Unknown/missing':
                    reported_date = None
                else:
                    reported_date = self.parse_mdh_date(c['Date reported to MDH'], today)

                new_state_tests = self.parse_comma_int(c['Completed PCR tests reported from the MDH Public Health Lab'])
                new_external_tests = self.parse_comma_int(c['Completed PCR tests reported from external laboratories'])

                total_tests = self.parse_comma_int(c['Total approximate number of completed tests (cumulative)'])

                new_antigen_tests = self.parse_comma_int(c['Completed antigen tests reported from external laboratories'])
                total_pcr_tests = self.parse_comma_int(c['Total approximate number of completed PCR tests (cumulative)'])
                total_antigen_tests = self.parse_comma_int(c['Total approximate number of completed antigen tests (cumulative)'])

                new_pcr_tests = new_state_tests + new_external_tests
                if new_antigen_tests:
                    new_tests = new_pcr_tests + new_antigen_tests
                else:
                    new_tests = new_pcr_tests

                std = StatewideTestsDate(
                    reported_date=reported_date,
                    new_state_tests=new_state_tests,
                    new_external_tests=new_external_tests,
                    new_tests=new_tests,
                    total_tests=total_tests,

                    new_pcr_tests=new_pcr_tests,
                    new_antigen_tests=new_antigen_tests,
                    total_pcr_tests=total_pcr_tests,
                    total_antigen_tests=total_antigen_tests,

                    update_date=update_date,
                    scrape_date=today,
                )
                test_objs.append(std)
            print('Adding {} records of test timeseries data'.format(len(test_objs)))
            StatewideTestsDate.objects.bulk_create(test_objs)

        msg_output = '*{}* total tests completed (*{}* today)\n\n'.format(f'{total_tests:,}', self.change_sign(new_tests))
        print(msg_output)

        return msg_output

    def get_statewide_hospitalizations_timeseries(self, soup, update_date):
        print('Parsing statewide hospitalizations timeseries...')

        hosp_table = table = soup.find("table", {'id': 'hosptable'})
        hosp_timeseries = self.timeseries_table_parser(hosp_table)

        if len(hosp_timeseries) > 0:
            today = datetime.date.today()
            # Remove old records from today
            existing_today_records = StatewideHospitalizationsDate.objects.filter(scrape_date=today)
            print('Removing {} records of hospitalizations timeseries data'.format(existing_today_records.count()))
            existing_today_records.delete()
            hosp_objs = []
            for c in hosp_timeseries:
                if c['Date'] == 'Unknown/missing':
                    reported_date = None
                elif c['Date'] == 'Admitted on or before 3/5':
                    reported_date = self.parse_mdh_date('3/5', today)
                else:
                    reported_date = self.parse_mdh_date(c['Date'], today)

                # check for hyphen in table cell, set to null if hyphen exists
                if c['Cases admitted to a hospital'] in ['-', '-\xa0\xa0 ']:
                    new_hospitalizations = None
                else:
                    new_hospitalizations = self.parse_comma_int(c['Cases admitted to a hospital'])

                if c['Cases admitted to an ICU'] in ['-', '-\xa0\xa0 ']:
                    new_icu_admissions = None
                else:
                    new_icu_admissions = self.parse_comma_int(c['Cases admitted to an ICU'])

                total_hospitalizations = self.parse_comma_int(c['Total hospitalizations (cumulative)'])
                total_icu_admissions = self.parse_comma_int(c['Total ICU hospitalizations (cumulative)'])

                std = StatewideHospitalizationsDate(
                    reported_date=reported_date,
                    new_hosp_admissions=new_hospitalizations,
                    new_icu_admissions=new_icu_admissions,
                    total_hospitalizations=total_hospitalizations,
                    total_icu_admissions=total_icu_admissions,
                    update_date=update_date,
                    scrape_date=today,
                )
                hosp_objs.append(std)

            print('Adding {} records of hospitalizations timeseries data'.format(len(hosp_objs)))
            StatewideHospitalizationsDate.objects.bulk_create(hosp_objs)

        return total_hospitalizations

    def get_statewide_deaths_timeseries(self, soup, update_date):
        print('Parsing statewide deaths timeseries...')

        deaths_table = table = soup.find("table", {'id': 'deathtable'})
        deaths_timeseries = self.timeseries_table_parser(deaths_table)
        # print(deaths_timeseries)

        if len(deaths_timeseries) > 0:
            today = datetime.date.today()
            # Remove old records from today
            existing_today_records = StatewideDeathsDate.objects.filter(scrape_date=today)
            print('Removing {} records of deaths timeseries data'.format(existing_today_records.count()))
            existing_today_records.delete()
            death_objs = []
            for c in deaths_timeseries:
                if c['Date reported'] == 'Unknown/missing':
                  reported_date = None
                else:
                  reported_date = self.parse_mdh_date(c['Date reported'], today)

                # check for hyphen in deaths table, set to null if hyphen exists
                if c['Newly reported deaths'] in ['', '-', '-\xa0\xa0 ']:
                  new_deaths = None
                else:
                  new_deaths = self.parse_comma_int(c['Newly reported deaths'])

                total_deaths = self.parse_comma_int(c['Total deaths (cumulative)'])

                std = StatewideDeathsDate(
                  reported_date=reported_date,
                  new_deaths=new_deaths,
                  total_deaths=total_deaths,
                  update_date=update_date,
                  scrape_date=today,
                )
                death_objs.append(std)
            print('Adding {} records of deaths timeseries data'.format(len(death_objs)))
            StatewideDeathsDate.objects.bulk_create(death_objs)

            msg_output = '*{}* total deaths (*{}* reported today)\n\n'.format(f'{total_deaths:,}', self.change_sign(new_deaths))
            print(msg_output)

            return msg_output

    def get_statewide_data(self, soup):
        output = {}

        hosp_table = soup.find("table", {'id': 'hosptotal'})
        # TODO: Unify with other totals to simplify timeseries output
        hosp_table_latest = self.totals_table_parser(hosp_table)
        output['cumulative_hospitalized'] = self.parse_comma_int(hosp_table_latest['Total cases hospitalized (cumulative)'])
        output['cumulative_icu'] = self.parse_comma_int(hosp_table_latest['Total cases hospitalized in ICU (cumulative)'])

        print(output['cumulative_hospitalized'], output['cumulative_icu'])

        cumulative_cases_table = soup.find("table", {'id': 'casetotal'})
        cumulative_cases_latest = self.totals_table_parser(cumulative_cases_table)
        output['cumulative_positive_tests'] = self.parse_comma_int(cumulative_cases_latest['Total positive cases (cumulative)'])
        output['cumulative_confirmed_cases'] = self.parse_comma_int(cumulative_cases_latest['Total confirmed cases (PCR positive) (cumulative)'])
        output['cumulative_probable_cases'] = self.parse_comma_int(cumulative_cases_latest['Total probable cases (Antigen positive) (cumulative)'])

        new_cases_table = soup.find("table", {'id': 'dailycasetotal'})
        new_cases_latest = self.totals_table_parser(new_cases_table)
        output['cases_newly_reported'] = self.parse_comma_int(new_cases_latest['Newly reported cases'])
        output['confirmed_cases_newly_reported'] = self.parse_comma_int(new_cases_latest['Newly reported confirmed cases'])
        output['probable_cases_newly_reported'] = self.parse_comma_int(new_cases_latest['Newly reported probable cases'])

        cumulative_tests_table = soup.find("table", {'id': 'testtotal'})
        cumulative_tests_latest = self.totals_table_parser(cumulative_tests_table)
        output['total_statewide_tests'] = self.parse_comma_int(cumulative_tests_latest['Total approximate completed tests (cumulative)'])
        output['cumulative_pcr_tests'] = self.parse_comma_int(cumulative_tests_latest['Total approximate number of completed PCR tests (cumulative)'])
        output['cumulative_antigen_tests'] = self.parse_comma_int(cumulative_tests_latest['Total approximate number of completed antigen tests (cumulative)'])

        deaths_table = soup.find("table", {'id': 'deathtotal'})
        deaths_table_latest = self.totals_table_parser(deaths_table)
        output['cumulative_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Total deaths (cumulative)'])
        output['cumulative_confirmed_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Deaths from confirmed cases (cumulative)'])
        output['cumulative_probable_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Deaths from probable cases (cumulative)'])

        recoveries_table = soup.find("table", {'id': 'noisototal'})
        recoveries_latest = self.totals_table_parser(recoveries_table)
        output['cumulative_statewide_recoveries'] = self.parse_comma_int(recoveries_latest['Patients no longer needing isolation (cumulative)'])

        uls = soup.find_all('ul')
        for ul in uls:

            daily_cases_removed_match = self.ul_regex('Cases removed', ul.text)
            if daily_cases_removed_match is not False:
                output['removed_cases'] = daily_cases_removed_match

        return output

    def update_statewide_records(self, statewide_data, update_date):
        yesterday = update_date - timedelta(days=1)
        yesterday_results = StatewideTotalDate.objects.get(scrape_date=yesterday)

        today = datetime.date.today()
        total_statewide_tests = statewide_data['total_statewide_tests']
        cases_daily_change = statewide_data['cumulative_positive_tests'] - yesterday_results.cumulative_positive_tests
        deaths_daily_change = statewide_data['cumulative_statewide_deaths'] - yesterday_results.cumulative_statewide_deaths
        hospitalized_total_daily_change = statewide_data['cumulative_hospitalized'] - yesterday_results.cumulative_hospitalized
        icu_total_daily_change = statewide_data['cumulative_icu'] - yesterday_results.cumulative_icu
        try:
            current_statewide_observation = StatewideTotalDate.objects.get(
                scrape_date=today
            )
            print('Updating existing statewide for {}'.format(today))

            current_statewide_observation.cumulative_positive_tests = statewide_data['cumulative_positive_tests']
            current_statewide_observation.cases_daily_change = cases_daily_change
            current_statewide_observation.cases_newly_reported = statewide_data['cases_newly_reported']
            current_statewide_observation.removed_cases = statewide_data['removed_cases']
            current_statewide_observation.deaths_daily_change = deaths_daily_change
            current_statewide_observation.cumulative_completed_tests = total_statewide_tests
            current_statewide_observation.cumulative_hospitalized = statewide_data['cumulative_hospitalized']
            current_statewide_observation.hospitalized_total_daily_change = hospitalized_total_daily_change

            current_statewide_observation.cumulative_icu = statewide_data['cumulative_icu']
            current_statewide_observation.icu_total_daily_change = icu_total_daily_change

            current_statewide_observation.cumulative_statewide_deaths = statewide_data['cumulative_statewide_deaths']
            current_statewide_observation.cumulative_statewide_recoveries = statewide_data['cumulative_statewide_recoveries']
            current_statewide_observation.update_date=update_date

            current_statewide_observation.save()
        except ObjectDoesNotExist:
            try:
                print('Creating 1st statewide record for {}'.format(today))
                current_statewide_observation = StatewideTotalDate(
                    cumulative_positive_tests=statewide_data['cumulative_positive_tests'],
                    cases_daily_change=cases_daily_change,
                    cases_newly_reported=statewide_data['cases_newly_reported'],
                    removed_cases=statewide_data['removed_cases'],
                    deaths_daily_change=deaths_daily_change,
                    cumulative_completed_tests=total_statewide_tests,
                    cumulative_hospitalized=statewide_data['cumulative_hospitalized'],
                    hospitalized_total_daily_change=hospitalized_total_daily_change,

                    cumulative_icu = statewide_data['cumulative_icu'],
                    icu_total_daily_change = icu_total_daily_change,

                    cumulative_statewide_deaths=statewide_data['cumulative_statewide_deaths'],
                    cumulative_statewide_recoveries=statewide_data['cumulative_statewide_recoveries'],
                    confirmed_cases_newly_reported=statewide_data['confirmed_cases_newly_reported'],
                    probable_cases_newly_reported=statewide_data['probable_cases_newly_reported'],
                    cumulative_confirmed_cases=statewide_data['cumulative_confirmed_cases'],
                    cumulative_probable_cases=statewide_data['cumulative_probable_cases'],
                    cumulative_pcr_tests=statewide_data['cumulative_pcr_tests'],
                    cumulative_antigen_tests=statewide_data['cumulative_antigen_tests'],
                    cumulative_confirmed_statewide_deaths=statewide_data['cumulative_confirmed_statewide_deaths'],
                    cumulative_probable_statewide_deaths=statewide_data['cumulative_probable_statewide_deaths'],
                    update_date=update_date,
                    scrape_date=today,
                )
                current_statewide_observation.save()
            except Exception as e:
                slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                raise

        msg_output = ''

        new_tests = current_statewide_observation.cumulative_completed_tests - yesterday_results.cumulative_completed_tests

        print('Change found, composing statewide Slack message...')

        msg_output += '*{}* cases total (change of *{}* today, *{}* newly reported, *{}* removed)\n'.format(f'{current_statewide_observation.cumulative_positive_tests:,}', self.change_sign(cases_daily_change), f'{current_statewide_observation.cases_newly_reported:,}', f'{current_statewide_observation.removed_cases:,}')

        msg_output += '*{}* total hospitalizations (*{}* new admissions reported today)\n\n'.format(f'{statewide_data["cumulative_hospitalized"]:,}', self.change_sign(hospitalized_total_daily_change))
        msg_output += '*{}* total icu (*{}* icu admissions reported today)\n\n'.format(f'{statewide_data["cumulative_icu"]:,}', self.change_sign(icu_total_daily_change))

        final_msg = 'COVID scraper found updated data on the <https://www.health.state.mn.us/diseases/coronavirus/situation.html|MDH situation page>...\n\n' + msg_output
        print(final_msg)

        return final_msg


    def get_recent_deaths_data(self, soup):
        today = datetime.date.today()
        recent_deaths_table = table = soup.find("table", {'id': 'dailydeathar'})
        recent_deaths_ages = self.timeseries_table_parser(recent_deaths_table)

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
                'scrape_date': today,
                'county__name': d_county_name,
                'age_group': group['Age group'].replace(' years', '').strip(),
                'count': int(group['Number of newly reported deaths']),
            }
            cleaned_data.append(clean_row)
        return cleaned_data

    def load_recent_deaths(self, scraped_deaths):
        existing_deaths = Death.objects.filter(scrape_date=datetime.date.today()).values('scrape_date', 'county__name', 'age_group').annotate(count=Count('pk'))

        date_stripped_scraped = [{i:d[i] for i in d if i != 'scrape_date'} for d in scraped_deaths]
        date_stripped_existing = [{i:d[i] for i in d if i != 'scrape_date'} for d in existing_deaths]

        bool_changes = [i for i in date_stripped_scraped if i not in date_stripped_existing] != []

        if bool_changes:

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

    def get_age_data(self, soup):
        age_table = soup.find("table", {'id': 'agetable'})
        ages_data = self.timeseries_table_parser(age_table)

        cleaned_ages_data = []
        for d in ages_data:
            d['Number of Cases'] = self.parse_comma_int(d['Number of Cases'])
            d['Number of Deaths'] = self.parse_comma_int(d['Number of Deaths'])
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

    def updated_today(self, soup):
        update_date_node = soup.find("strong", text=re.compile('Updated [A-z]+ \d{1,2}, \d{4}')).text

        update_date = datetime.datetime.strptime(re.search('([A-z]+ \d{1,2}, \d{4})', update_date_node).group(1), '%B %d, %Y').date()
        if update_date == datetime.datetime.now().date():
            return True, update_date
        else:
            return False, update_date

    def handle(self, *args, **options):
        html = self.get_page_content()
        if not html:
            slack_latest('WARNING: Scraper error. Not proceeding.', '#robot-dojo')
        else:
            previous_statewide_cases = StatewideTotalDate.objects.order_by('-scrape_date').first().cumulative_positive_tests

            soup = BeautifulSoup(html, 'html.parser')

            # Will make this more important after we're sure it works
            bool_updated_today, update_date = self.updated_today(soup)
            print(update_date)
            if bool_updated_today:
                print('Updated today')
            else:
                print('No update yet today')

            statewide_data = self.get_statewide_data(soup)
            statewide_msg_output = self.update_statewide_records(statewide_data, update_date)

            self.get_statewide_cases_timeseries(soup, update_date)
            test_msg_output = self.get_statewide_tests_timeseries(soup, update_date)
            death_msg_output = self.get_statewide_deaths_timeseries(soup, update_date)
            total_hospitalizations = self.get_statewide_hospitalizations_timeseries(soup, update_date)

            # TODO: MOVE TO NEW SCRIPT
            age_data = self.get_age_data(soup)
            age_msg_output = self.update_age_records(age_data)

            # TODO: MOVE TO NEW SCRIPT
            if bool_updated_today:
                recent_deaths_data = self.get_recent_deaths_data(soup)
                self.load_recent_deaths(recent_deaths_data)

            # TODO: MOVE TO NEW SCRIPT
            county_data = self.get_county_data(soup)
            county_msg_output = self.update_county_records(county_data, update_date)

            if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
                slack_latest(statewide_msg_output + death_msg_output + test_msg_output + county_msg_output, '#virus')
            else:

                # slack_latest(statewide_msg_output + death_msg_output + test_msg_output + county_msg_output, '#virus')  # Force output anyway
                slack_latest('COVID scraper update: No changes detected.', '#robot-dojo')
