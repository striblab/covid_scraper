import re
import os
import csv
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup

from django.db.models import Count
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings

from stats.models import County, CountyTestDate, StatewideTotalDate, Death, StatewideCasesBySampleDate, StatewideTestsDate, StatewideDeathsDate, StatewideHospitalizationsDate
from stats.utils import get_situation_page_content, timeseries_table_parser, parse_comma_int, updated_today, slack_latest


class Command(BaseCommand):
    help = 'Check for new or updated results by date from Minnesota Department of Health table: https://www.health.state.mn.us/diseases/coronavirus/situation.html'

    def ul_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def parse_mdh_date(self, input_str, today):
        date_split = input_str.split('/')
        month = date_split[0]
        day = date_split[1]
        year = "20" + date_split[2]
        return datetime.date(int(year), int(month), int(day))
        # fake_date = datetime.date(2021, int(month), int(day))
        # if fake_date > today:
        #     real_date = datetime.date(2020, int(month), int(day))
        # else:
        #     real_date = datetime.date(2021, int(month), int(day))
        # return real_date
        # return datetime.datetime.strptime('{}/{}'.format(input_str, today.year), '%m/%d/%Y')

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

    def pct_filter(self, input_str):
        '''Removing pct sign, and changing <1 to -1'''
        if input_str == '<1%':
            return -1
        try:
            return int(input_str.replace('%', ''))
        except:
            return None

    def get_statewide_cases_timeseries(self, soup, update_date):
        '''How to deal with back-dated statewide totals if they use sample dates'''
        print('Parsing statewide cases timeseries...')

        cases_table = soup.find("table", {'id': 'casetable'})
        cases_timeseries = timeseries_table_parser(cases_table)
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
                new_pcr_tests = parse_comma_int(c['Confirmed cases (PCR positive)'])
                new_antigen_tests = parse_comma_int(c['Probable cases (Antigen positive)'])

                if new_antigen_tests:  # Handle old dates
                    new_cases = int(new_pcr_tests) + int(new_antigen_tests)
                else:
                    new_cases = new_pcr_tests
                # print(sample_date, new_pcr_tests, new_antigen_tests, new_cases)

                co = StatewideCasesBySampleDate(
                    sample_date=sample_date,
                    new_cases=new_cases,
                    total_cases=parse_comma_int(c['Total positive cases (cumulative)']),

                    new_pcr_tests = new_pcr_tests,
                    new_antigen_tests = new_antigen_tests,
                    total_pcr_tests = parse_comma_int(c['Total confirmed cases (cumulative)']),
                    total_antigen_tests = parse_comma_int(c['Total probable cases (cumulative)']),

                    update_date=update_date,
                    scrape_date=today,
                )
                case_objs.append(co)
            print('Adding {} records of case timeseries data'.format(len(case_objs)))
            StatewideCasesBySampleDate.objects.bulk_create(case_objs)

    def get_statewide_tests_timeseries(self, soup, update_date):
        print('Parsing statewide tests timeseries...')

        tests_table = soup.find("table", {'id': 'labtable'})
        tests_timeseries = timeseries_table_parser(tests_table)

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

                new_state_tests = parse_comma_int(c['Completed PCR tests reported from the MDH Public Health Lab'])
                new_external_tests = parse_comma_int(c['Completed PCR tests reported from external laboratories'])

                total_tests = parse_comma_int(c['Total approximate number of completed tests (cumulative)'])

                new_antigen_tests = parse_comma_int(c['Completed antigen tests reported from external laboratories'])
                total_pcr_tests = parse_comma_int(c['Total approximate number of completed PCR tests (cumulative)'])
                total_antigen_tests = parse_comma_int(c['Total approximate number of completed antigen tests (cumulative)'])

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
        hosp_timeseries = timeseries_table_parser(hosp_table)

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
                elif c['Date'] == 'Admitted on or before 3/5/20':
                    reported_date = self.parse_mdh_date('3/5/20', today)
                else:
                    reported_date = self.parse_mdh_date(c['Date'], today)

                # check for hyphen in table cell, set to null if hyphen exists
                if c['Cases admitted to a hospital'] in ['-', '-\xa0\xa0 ']:
                    new_hospitalizations = None
                else:
                    new_hospitalizations = parse_comma_int(c['Cases admitted to a hospital'])

                if c['Cases admitted to an ICU'] in ['-', '-\xa0\xa0 ']:
                    new_icu_admissions = None
                else:
                    new_icu_admissions = parse_comma_int(c['Cases admitted to an ICU'])

                total_hospitalizations = parse_comma_int(c['Total hospitalizations (cumulative)'])
                total_icu_admissions = parse_comma_int(c['Total ICU hospitalizations (cumulative)'])

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
        deaths_timeseries = timeseries_table_parser(deaths_table)
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
                  new_deaths = parse_comma_int(c['Newly reported deaths'])

                total_deaths = parse_comma_int(c['Total deaths (cumulative)'])

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
        hosp_table_latest = self.totals_table_parser(hosp_table)
        output['cumulative_hospitalized'] = parse_comma_int(hosp_table_latest['Total cases hospitalized (cumulative)'])
        output['cumulative_icu'] = parse_comma_int(hosp_table_latest['Total cases hospitalized in ICU (cumulative)'])

        cumulative_cases_table = soup.find("table", {'id': 'casetotal'})
        cumulative_cases_latest = self.totals_table_parser(cumulative_cases_table)
        output['cumulative_positive_tests'] = parse_comma_int(cumulative_cases_latest['Total positive cases (cumulative)'])
        output['cumulative_confirmed_cases'] = parse_comma_int(cumulative_cases_latest['Total confirmed cases (PCR positive) (cumulative)'])
        output['cumulative_probable_cases'] = parse_comma_int(cumulative_cases_latest['Total probable cases (Antigen positive) (cumulative)'])

        new_cases_table = soup.find("table", {'id': 'dailycasetotal'})
        new_cases_latest = self.totals_table_parser(new_cases_table)
        output['cases_newly_reported'] = parse_comma_int(new_cases_latest['Newly reported cases'])
        output['confirmed_cases_newly_reported'] = parse_comma_int(new_cases_latest['Newly reported confirmed cases'])
        output['probable_cases_newly_reported'] = parse_comma_int(new_cases_latest['Newly reported probable cases'])

        cumulative_tests_table = soup.find("table", {'id': 'testtotal'})
        cumulative_tests_latest = self.totals_table_parser(cumulative_tests_table)
        output['total_statewide_tests'] = parse_comma_int(cumulative_tests_latest['Total approximate completed tests (cumulative)'])
        output['cumulative_pcr_tests'] = parse_comma_int(cumulative_tests_latest['Total approximate number of completed PCR tests (cumulative)'])
        output['cumulative_antigen_tests'] = parse_comma_int(cumulative_tests_latest['Total approximate number of completed antigen tests (cumulative)'])

        deaths_table = soup.find("table", {'id': 'deathtotal'})
        deaths_table_latest = self.totals_table_parser(deaths_table)
        output['cumulative_statewide_deaths'] = parse_comma_int(deaths_table_latest['Total deaths (cumulative)'])
        output['cumulative_confirmed_statewide_deaths'] = parse_comma_int(deaths_table_latest['Deaths from confirmed cases (cumulative)'])
        output['cumulative_probable_statewide_deaths'] = parse_comma_int(deaths_table_latest['Deaths from probable cases (cumulative)'])

        recoveries_table = soup.find("table", {'id': 'noisototal'})
        recoveries_latest = self.totals_table_parser(recoveries_table)
        output['cumulative_statewide_recoveries'] = parse_comma_int(recoveries_latest['Patients no longer needing isolation (cumulative)'])

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

        base_record_obj = {
            'cumulative_positive_tests': statewide_data['cumulative_positive_tests'],
            'cases_daily_change': cases_daily_change,
            'cases_newly_reported': statewide_data['cases_newly_reported'],
            'removed_cases': statewide_data['removed_cases'],
            'deaths_daily_change': deaths_daily_change,
            'cumulative_completed_tests': total_statewide_tests,
            'cumulative_hospitalized': statewide_data['cumulative_hospitalized'],
            'hospitalized_total_daily_change': hospitalized_total_daily_change,
            'cumulative_icu': statewide_data['cumulative_icu'],
            'icu_total_daily_change': icu_total_daily_change,
            'cumulative_statewide_deaths': statewide_data['cumulative_statewide_deaths'],
            'cumulative_statewide_recoveries': statewide_data['cumulative_statewide_recoveries'],
            'confirmed_cases_newly_reported': statewide_data['confirmed_cases_newly_reported'],
            'probable_cases_newly_reported': statewide_data['probable_cases_newly_reported'],
            'cumulative_confirmed_cases': statewide_data['cumulative_confirmed_cases'],
            'cumulative_probable_cases': statewide_data['cumulative_probable_cases'],
            'cumulative_pcr_tests': statewide_data['cumulative_pcr_tests'],
            'cumulative_antigen_tests': statewide_data['cumulative_antigen_tests'],
            'cumulative_confirmed_statewide_deaths': statewide_data['cumulative_confirmed_statewide_deaths'],
            'cumulative_probable_statewide_deaths': statewide_data['cumulative_probable_statewide_deaths'],
            'update_date': update_date
        }

        try:
            current_statewide_observation = StatewideTotalDate.objects.get(
                scrape_date=today
            )
            print('Updating existing statewide for {}'.format(today))

            for k, v in base_record_obj.items():
                setattr(current_statewide_observation, k, v)
            current_statewide_observation.save()

        except ObjectDoesNotExist:
            try:
                print('Creating 1st statewide record for {}'.format(today))
                base_record_obj['scrape_date'] = today

                current_statewide_observation = StatewideTotalDate(**base_record_obj)
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

    def handle(self, *args, **options):
        html = get_situation_page_content()
        if not html:
            slack_latest('WARNING: Scraper error. Not proceeding.', '#robot-dojo')
        else:
            previous_statewide_cases = StatewideTotalDate.objects.order_by('-scrape_date').first().cumulative_positive_tests

            soup = BeautifulSoup(html, 'html.parser')

            bool_updated_today, update_date = updated_today(soup)
            print(update_date)
            if bool_updated_today:
                print('Updated today')

                statewide_data = self.get_statewide_data(soup)
                statewide_msg_output = self.update_statewide_records(statewide_data, update_date)

                self.get_statewide_cases_timeseries(soup, update_date)
                test_msg_output = self.get_statewide_tests_timeseries(soup, update_date)
                death_msg_output = self.get_statewide_deaths_timeseries(soup, update_date)
                total_hospitalizations = self.get_statewide_hospitalizations_timeseries(soup, update_date)

                if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
                    slack_latest(statewide_msg_output + death_msg_output + test_msg_output, '#virus')
                else:

                    # slack_latest(statewide_msg_output + death_msg_output + test_msg_output, '#virus')  # Force output anyway
                    slack_latest('COVID scraper update: No changes detected.', '#robot-dojo')
            else:
                print('No update yet today')
