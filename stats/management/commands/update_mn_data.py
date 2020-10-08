import re
import os
import csv
import requests
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
from urllib.request import urlopen

from django.db.models import Sum, Count, Max, Avg, F, RowRange, Window
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from stats.models import County, CountyTestDate, StatewideAgeDate, StatewideTotalDate, Death, StatewideCasesBySampleDate, StatewideTestsDate, StatewideDeathsDate, StatewideHospitalizationsDate
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

    def get_county_data(self, soup):

        county_data = []

        table = soup.find("table", {'id': 'maptable'})
        # table = soup.find("th", text="Deaths").find_parent("table")

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) > 0 and tds[0].text  != 'Unknown/missing':
                # print(tds)
                county_name = tds[0].text
                county_cases = tds[1].text
                county_deaths = tds[2].text
                county_data.append((county_name, county_cases, county_deaths))

        return county_data

    def update_county_records(self, county_data, update_date):
        msg_output = ''

        today = datetime.date.today()
        # print(county_data)
        num_counties_with_cases = len([c for c in county_data if self.parse_comma_int(c[1]) > 0])

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
                county_observation.update_date = update_date
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
                        update_date=update_date,
                        daily_count=daily_cases,
                        cumulative_count=self.parse_comma_int(observation[1]),
                        daily_deaths=daily_deaths,
                        cumulative_deaths=self.parse_comma_int(observation[2]),
                    )
                    county_observation.save()
                except Exception as e:
                    slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                    raise

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

        final_msg = 'COVID scraper county-by-county results: \n\n' + msg_output
        print(final_msg)

        return final_msg

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

    def p_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def parse_comma_int(self, input_str):
        if input_str == '-':
            return '-'
        else:
            return int(input_str.replace(',', ''))

    def parse_mdh_date(self, input_str, today):
        return datetime.datetime.strptime('{}/{}'.format(input_str, today.year), '%m/%d/%Y')

    def get_statewide_cases_timeseries(self, soup, update_date):
        '''How to deal with back-dated statewide totals if they use sample dates'''
        print('Parsing statewide cases timeseries...')

        cases_table = soup.find("table", {'id': 'casetable'})
        cases_timeseries = self.full_table_parser(cases_table)
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

                if c['Positive cases'] == '':
                    new_cases = '0'
                else:
                    new_cases = c['Positive cases']

                co = StatewideCasesBySampleDate(
                    sample_date=sample_date,
                    new_cases=self.parse_comma_int(new_cases),
                    total_cases=self.parse_comma_int(c['Total positive cases (cumulative)']),
                    update_date=update_date,
                    scrape_date=today,
                )
                case_objs.append(co)
            print('Adding {} records of case timeseries data'.format(len(case_objs)))
            StatewideCasesBySampleDate.objects.bulk_create(case_objs)

    def get_statewide_tests_timeseries(self, soup, update_date):
        print('Parsing statewide tests timeseries...')

        tests_table = soup.find("table", {'id': 'labtable'})
        tests_timeseries = self.full_table_parser(tests_table)
        # print(tests_timeseries)

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

                if c['Completed tests reported from the MDH Public Health Lab (daily)'] == '-\xa0\xa0 ' or c['Completed tests reported from the MDH Public Health Lab (daily)'] == '-':
                    new_state_tests = 0
                    new_external_tests = self.parse_comma_int(c['Completed tests reported from external laboratories (daily)'])
                    new_tests = new_state_tests + new_external_tests
                    total_tests = self.parse_comma_int(c['Total approximate number of completed tests (cumulative)'])
                else:
                    new_state_tests = self.parse_comma_int(c['Completed tests reported from the MDH Public Health Lab (daily)'])
                    new_external_tests = self.parse_comma_int(c['Completed tests reported from external laboratories (daily)'])
                    new_tests = new_state_tests + new_external_tests
                    total_tests = self.parse_comma_int(c['Total approximate number of completed tests (cumulative)'])

                std = StatewideTestsDate(
                    reported_date=reported_date,
                    new_state_tests=new_state_tests,
                    new_external_tests=new_external_tests,
                    new_tests=new_tests,
                    total_tests=total_tests,
                    update_date=update_date,
                    scrape_date=today,
                )
                test_objs.append(std)
            print('Adding {} records of test timeseries data'.format(len(test_objs)))
            StatewideTestsDate.objects.bulk_create(test_objs)

            # calculate/add rolling average
            print('Calculating rolling averages ...')
            tests_added = StatewideTestsDate.objects.filter(scrape_date=today).annotate(
                new_tests_rolling_temp=Window(
                    expression=Avg('new_tests'),
                    order_by=F('reported_date').asc(),
                    frame=RowRange(start=-6,end=0)
                )
            )
            for t in tests_added:
                t.new_tests_rolling = t.new_tests_rolling_temp
                t.save()

        msg_output = '*{}* total tests completed (*{}* today)\n\n'.format(f'{total_tests:,}', self.change_sign(new_tests))
        print(msg_output)

        return msg_output

    def get_statewide_hospitalizations_timeseries(self, soup, update_date):
        print('Parsing statewide hospitalizations timeseries...')

        hosp_table = table = soup.find("table", {'id': 'hosptable'})
        hosp_timeseries = self.full_table_parser(hosp_table)
        # print(hosp_timeseries)
        # print(deaths_timeseries)

        if len(hosp_timeseries) > 0:
            today = datetime.date.today()
            # Remove old records from today
            existing_today_records = StatewideHospitalizationsDate.objects.filter(scrape_date=today)
            print('Removing {} records of hospitalizations timeseries data'.format(existing_today_records.count()))
            existing_today_records.delete()
            hosp_objs = []
            for c in hosp_timeseries:
                if c['Date '] == 'Unknown/missing':
                    reported_date = None
                elif c['Date '] == 'Admitted on or before 3/5':
                    reported_date = self.parse_mdh_date('3/5', today)
                else:
                    reported_date = self.parse_mdh_date(c['Date '], today)

                # check for hyphen in table cell, set to null if hyphen exists
                if c['Cases admitted to a hospital'] in ['-', '-\xa0\xa0 ']:
                    new_hospitalizations = None
                else:
                    new_hospitalizations = self.parse_comma_int(c['Cases admitted to a hospital'])

                if c['Cases    admitted to an ICU'] in ['-', '-\xa0\xa0 ']:
                    new_icu_admissions = None
                else:
                    new_icu_admissions = self.parse_comma_int(c['Cases    admitted to an ICU'])

                total_hospitalizations = self.parse_comma_int(c['Total hospitalizations    (cumulative)'])
                total_icu_admissions = self.parse_comma_int(c['Total ICU hospitalizations    (cumulative)'])

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

          # calculate/add rolling average
          # print('Calculating rolling averages ...')
          # hosps_added = StatewideHospitalizationsDate.objects.filter(scrape_date=today).annotate(
          #     new_non_icu_admissions_rolling_temp=Window(
          #         expression=Avg('new_non_icu_admissions'),
          #         order_by=F('reported_date').asc(),
          #         frame=RowRange(start=-6,end=0)
          #     ),
          #     new_icu_admissions_rolling_rolling_temp=Window(
          #         expression=Avg('new_icu_admissions'),
          #         order_by=F('reported_date').asc(),
          #         frame=RowRange(start=-6,end=0)
          #     ),
          # )
          #
          # for t in hosps_added:
          #     t.new_non_icu_admissions_rolling = t.new_non_icu_admissions_rolling_temp
          #     t.new_icu_admissions_rolling = t.new_icu_admissions_rolling_temp
          #     t.save()

        # Move this message to overall state data
        # msg_output = '*{}* total hospitalizations (*{}* new admissions reported today)\n\n'.format(f'{total_hospitalizations:,}', self.change_sign(new_deaths))
        # return msg_output

        return total_hospitalizations


    def get_statewide_deaths_timeseries(self, soup, update_date):
        print('Parsing statewide deaths timeseries...')

        deaths_table = table = soup.find("table", {'id': 'deathtable'})
        deaths_timeseries = self.full_table_parser(deaths_table)
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
                if c['Newly reported deaths (daily)'] in ['', '-', '-\xa0\xa0 ']:
                  new_deaths = None
                else:
                  new_deaths = self.parse_comma_int(c['Newly reported deaths (daily)'])

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

            # calculate/add rolling average
            print('Calculating rolling averages ...')
            deaths_added = StatewideDeathsDate.objects.filter(scrape_date=today).annotate(
                new_deaths_rolling_temp=Window(
                  expression=Avg('new_deaths'),
                  order_by=F('reported_date').asc(),
                  frame=RowRange(start=-6,end=0)
                )
            )
            for t in deaths_added:
                t.new_deaths_rolling = t.new_deaths_rolling_temp
                t.save()

            msg_output = '*{}* total deaths (*{}* reported today)\n\n'.format(f'{total_deaths:,}', self.change_sign(new_deaths))
            print(msg_output)

            return msg_output


    # TODO: Hospitalization timeseries table

    def get_statewide_data(self, soup):
        output = {}

        hosp_table = soup.find("table", {'id': 'hosptable'})
        hosp_table_latest = self.detail_tables_regex(hosp_table)
        print(hosp_table_latest)
        output['cumulative_hospitalized'] = self.parse_comma_int(hosp_table_latest['Total hospitalizations    (cumulative)'])
         # Not used except to add up

        # '*{}* total hospitalizations (*{}* new admissions reported today)\n\n'.format(f'{total_hospitalizations:,}', self.change_sign(new_deaths))

        # check for null hospitalizations

        # if hosp_table_latest['Hospitalized in ICU (daily)'] == '-\xa0\xa0 ' or hosp_table_latest['Hospitalized in ICU (daily)'] == '-':
        #     output['currently_in_icu'] = None
        # else:
        #     output['currently_in_icu'] = self.parse_comma_int(hosp_table_latest['Hospitalized in ICU (daily)'])

        # if hosp_table_latest['Hospitalized, not in ICU (daily)'] == '-\xa0\xa0 ' or hosp_table_latest['Hospitalized, not in ICU (daily)'] == '-':
        #     output['currently_non_icu_hospitalized'] = None
        # else:
        #     output['currently_non_icu_hospitalized'] = self.parse_comma_int(hosp_table_latest['Hospitalized, not in ICU (daily)'])
        #
        # if output['currently_in_icu'] == None or output['currently_non_icu_hospitalized'] == None:
        #     output['currently_hospitalized'] = None
        # else:
        #     output['currently_hospitalized'] = output['currently_in_icu'] + output['currently_non_icu_hospitalized']




        deaths_table = soup.find("table", {'id': 'deathtable'})
        deaths_table_latest = self.detail_tables_regex(deaths_table)
        output['cumulative_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Total deaths (cumulative)'])

        newly_reported_cases_match = self.parse_comma_int(soup.find('span', text=re.compile('Newly reported cases')).find_parent('td').find('strong').text)
        # print(newly_reported_cases_match)
        if newly_reported_cases_match:
            output['cases_newly_reported'] = newly_reported_cases_match

        # new_deaths_match = self.parse_comma_int(soup.find('span', text=re.compile('Newly reported deaths')).find_parent('td').find('strong').text)
        # # print(new_deaths_match)
        # if new_deaths_match:
        #     output['new_deaths'] = new_deaths_match

        # THIS IS WHERE CARNAGE FROM THOMAS BEGINS

        mdh_tests = 0
        private_tests = 0


        output['total_statewide_tests'] = self.parse_comma_int(soup.find('strong', text=re.compile('Total approximate number of completed tests')).find_parent('p').text.replace('Total approximate number of completed tests:\xa0', '').strip())
        # newly_reported_cases_match = self.parse_comma_int(soup.find('span', text=re.compile('Newly reported cases')).find_parent('td').find('strong').text)
        # print(int(tests_timeseries[len(tests_timeseries) - 1]['Total approximate number of completed tests '].replace(',', '')))

        # mike, I am truly sorry for this - TO
        # output['total_statewide_tests'] = int(tests_timeseries[len(tests_timeseries) - 1]['Total approximate number of completed tests '].replace(',', ''))

        tests_table = table = soup.find("table", {'id': 'labtable'})
        tests_timeseries = self.full_table_parser(tests_table)
        for c in tests_timeseries:
            if c['Date reported to MDH'] == 'Unknown/missing':
                continue
            else:
                if c['Completed tests reported from the MDH Public Health Lab (daily)'] == '-\xa0\xa0 ':
                    continue
                else:
                    mdh_tests = mdh_tests + self.parse_comma_int(c['Completed tests reported from the MDH Public Health Lab (daily)'])
                private_tests = private_tests + self.parse_comma_int(c['Completed tests reported from external laboratories (daily)'])

        output['cumulative_completed_mdh'] = mdh_tests
        output['cumulative_completed_private'] = private_tests

        uls = soup.find_all('ul')
        for ul in uls:
            cumulative_positive_tests_match = self.ul_regex('Total positive cases', ul.text)
            if cumulative_positive_tests_match:
                # print(cumulative_positive_tests_match)
                output['cumulative_positive_tests'] = cumulative_positive_tests_match

            daily_cases_removed_match = self.ul_regex('Cases removed', ul.text)
            if daily_cases_removed_match is not False:
                output['removed_cases'] = daily_cases_removed_match
                # print('cases removed', daily_cases_removed_match)

            # cumulative_completed_mdh_match = self.ul_regex('Total approximate number of completed tests from the MDH Public Health Lab', ul.text)
            # if cumulative_completed_mdh_match:
            #     output['cumulative_completed_mdh'] = cumulative_completed_mdh_match
            #
            # cumulative_completed_private_match = self.ul_regex('Total approximate number of completed tests from external laboratories', ul.text)
            # if cumulative_completed_private_match:
            #     output['cumulative_completed_private'] = cumulative_completed_private_match

            recoveries_match = self.ul_regex('Patients no longer needing isolation', ul.text)
            if recoveries_match:
                output['cumulative_statewide_recoveries'] = recoveries_match

        return output

    def change_sign(self, input_int):
        optional_plus = ''
        if not input_int:
            return '+:shrug:'
        elif input_int != 0:
            optional_plus = '+'
            if input_int < 0:
                optional_plus = ':rotating_light: '
        return '{}{}'.format(optional_plus, f'{input_int:,}')

    def full_table_parser(self, table):
        ''' should work on multiple columns '''
        # table = soup.find("th", text=find_str).find_parent("table")
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
                    # print(cells[k].text)
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

    def get_recent_deaths_data(self, soup):
        today = datetime.date.today()
        recent_deaths_table = table = soup.find("table", {'id': 'dailydeathar'})
        recent_deaths_ages = self.full_table_parser(recent_deaths_table)

        # print(recent_deaths_ages)
        cleaned_data = []

        for group in recent_deaths_ages:
            # County workaround
            if group['County of residence'] == 'Otter':
                d_county_name = 'Otter Tail'
            else:
                d_county_name = re.sub('\s+', ' ', group['County of residence'])

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
        # print(bool_changes)

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

            # print(deaths_to_add, deaths_to_remove)
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
                county = County.objects.get(name=group['county__name'])
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
        ages_data = self.full_table_parser(age_table)

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


    def update_statewide_records(self, statewide_data, update_date):
        yesterday = update_date - timedelta(days=1)
        yesterday_results = StatewideTotalDate.objects.get(scrape_date=yesterday)
        # previous_statewide_results = StatewideTotalDate.objects.all().order_by('-scrape_date').first()

        today = datetime.date.today()
        total_statewide_tests = statewide_data['total_statewide_tests']
        cases_daily_change = statewide_data['cumulative_positive_tests'] - yesterday_results.cumulative_positive_tests
        deaths_daily_change = statewide_data['cumulative_statewide_deaths'] - yesterday_results.cumulative_statewide_deaths
        hospitalized_total_daily_change = statewide_data['cumulative_hospitalized'] - yesterday_results.cumulative_hospitalized
        try:
            current_statewide_observation = StatewideTotalDate.objects.get(
                scrape_date=today
            )
            print('Updating existing statewide for {}'.format(today))
            print(statewide_data['removed_cases'])

            current_statewide_observation.cumulative_positive_tests = statewide_data['cumulative_positive_tests']
            current_statewide_observation.cases_daily_change = cases_daily_change
            current_statewide_observation.cases_newly_reported = statewide_data['cases_newly_reported']
            current_statewide_observation.removed_cases = statewide_data['removed_cases']

            current_statewide_observation.deaths_daily_change = deaths_daily_change

            current_statewide_observation.cumulative_completed_tests = total_statewide_tests
            current_statewide_observation.cumulative_completed_mdh = statewide_data['cumulative_completed_mdh']
            current_statewide_observation.cumulative_completed_private = statewide_data['cumulative_completed_private']
            current_statewide_observation.cumulative_hospitalized = statewide_data['cumulative_hospitalized']
            # current_statewide_observation.currently_hospitalized = statewide_data['currently_hospitalized']
            # current_statewide_observation.currently_in_icu = statewide_data['currently_in_icu']

            current_statewide_observation.hospitalized_total_daily_change = hospitalized_total_daily_change

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
                    # new_cases=statewide_data['new_cases'],
                    deaths_daily_change=deaths_daily_change,
                    # cumulative_positive_tests=statewide_data['cumulative_positive_tests'],
                    cumulative_completed_tests=total_statewide_tests,
                    cumulative_completed_mdh=statewide_data['cumulative_completed_mdh'],
                    cumulative_completed_private=statewide_data['cumulative_completed_private'],
                    cumulative_hospitalized=statewide_data['cumulative_hospitalized'],
                    # currently_hospitalized=statewide_data['currently_hospitalized'],
                    # currently_in_icu=statewide_data['currently_in_icu'],
                    hospitalized_total_daily_change=hospitalized_total_daily_change,
                    cumulative_statewide_deaths=statewide_data['cumulative_statewide_deaths'],
                    cumulative_statewide_recoveries=statewide_data['cumulative_statewide_recoveries'],

                    update_date=update_date,
                    scrape_date=today,
                )
                current_statewide_observation.save()
            except Exception as e:
                slack_latest('SCRAPER ERROR: {}'.format(e), '#robot-dojo')
                raise

        msg_output = ''
        # if (cases_daily_change != 0):
        # new_deaths = current_statewide_observation.cumulative_statewide_deaths - previous_statewide_results.cumulative_statewide_deaths

        # if yesterday_results.currently_hospitalized:
        #     hospitalizations_change = current_statewide_observation.currently_hospitalized - yesterday_results.currently_hospitalized
        # else:
        #     hospitalizations_change = None
        #
        # if yesterday_results.currently_in_icu:
        #     icu_change = current_statewide_observation.currently_in_icu - yesterday_results.currently_in_icu
        # else:
        #     icu_change = None

        new_tests = current_statewide_observation.cumulative_completed_tests - yesterday_results.cumulative_completed_tests

        print('Change found, composing statewide Slack message...')
        # msg_output += '*{}* deaths total (*{}* today)\n'.format(f'{current_statewide_observation.cumulative_statewide_deaths:,}', self.change_sign(deaths_daily_change))
        msg_output += '*{}* cases total (change of *{}* today, *{}* newly reported, *{}* removed)\n'.format(f'{current_statewide_observation.cumulative_positive_tests:,}', self.change_sign(cases_daily_change), f'{current_statewide_observation.cases_newly_reported:,}', f'{current_statewide_observation.removed_cases:,}')

        msg_output += '*{}* total hospitalizations (*{}* new admissions reported today)\n\n'.format(f'{statewide_data["cumulative_hospitalized"]:,}', self.change_sign(hospitalized_total_daily_change))
        # msg_output += '*{}* currently hospitalized (*{}* today)\n'.format(f'{current_statewide_observation.currently_hospitalized:,}', self.change_sign(hospitalizations_change))
        # msg_output += '*{}* currently in ICU (*{}* today)\n'.format(f'{current_statewide_observation.currently_in_icu:,}', self.change_sign(icu_change))

        # TODO: Separate new tests in message
        # msg_output += '*{}* total tests completed (*{}* today)\n'.format(f'{current_statewide_observation.cumulative_completed_tests:,}', self.change_sign(new_tests))

        final_msg = 'COVID scraper found updated data on the <https://www.health.state.mn.us/diseases/coronavirus/situation.html|MDH situation page>...\n\n' + msg_output
        print(final_msg)

        return final_msg

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
            # DEPRECATED -- HANDLED IN .SH FILE... Save a copy of HTML
            # now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            # with open(os.path.join(settings.BASE_DIR, 'exports', 'html', 'situation_{}.html').format(now), 'wb') as html_file:
            #     html_file.write(html)
            #     html_file.close()

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

            # TODO: AGE UPDATES
            age_data = self.get_age_data(soup)
            age_msg_output = self.update_age_records(age_data)

            if bool_updated_today:

                recent_deaths_data = self.get_recent_deaths_data(soup)
                # print(recent_deaths_data)

                # existing_today_deaths, bool_yesterday = self.get_existing_recent_deaths_records()
                # print(existing_today_deaths, bool_yesterday)
                self.load_recent_deaths(recent_deaths_data)

            county_data = self.get_county_data(soup)
            county_msg_output = self.update_county_records(county_data, update_date)
            # county_msg_output = "Leaving county counts at yesterday's total until state clarifies"

            print(statewide_data['cumulative_positive_tests'], previous_statewide_cases)
            if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
                slack_latest(statewide_msg_output + death_msg_output + test_msg_output + county_msg_output, '#virus')
            else:

                # slack_latest(statewide_msg_output + death_msg_output + test_msg_output + county_msg_output, '#virus')  # Force output anyway
                slack_latest('COVID scraper update: No changes detected.', '#robot-dojo')
