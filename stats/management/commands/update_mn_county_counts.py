import re
import os
import csv
import requests
import datetime
import codecs
from bs4 import BeautifulSoup
from urllib.request import urlopen

from django.db.models import Sum, Count, Max
from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from stats.models import County, CountyTestDate, StatewideAgeDate, StatewideTotalDate, Death, StatewideCasesBySampleDate, StatewideTestsDate
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
        # soup = BeautifulSoup(html, 'html.parser')

        county_data = []

        # table = soup.find_all('table')[3]
        # parent_div = soup.find("p", text=re.compile('County of residence data table'))
        # print(parent_div)
        table = soup.find("th", text="Deaths").find_parent("table")

        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) > 0 and tds[0].text  != 'Unknown/missing':
                print(tds)
                county_name = tds[0].text
                county_cases = tds[1].text
                county_deaths = tds[2].text
                county_data.append((county_name, county_cases, county_deaths))

        return county_data

    def update_county_records(self, county_data):
        msg_output = ''

        today = datetime.date.today()
        print(county_data)
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

        final_msg = 'COVID scraper county-by-county results: \n\n' + msg_output
        print(final_msg)

        return final_msg

    # def find_matching_deaths(self, deaths_obj, date, county_name):
    #     for row in deaths_obj:
    #         # print(row['COUNTY'], datetime.datetime.strptime(row['DATE'], '%m/%d/%Y').date(), date, county_name)
    #         if row['COUNTY'] == county_name and datetime.datetime.strptime(row['DATE'], '%m/%d/%Y').date() == date:
    #             return row['NUM_DEATHS']
    #     return 0

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

    def p_regex(self, preceding_text, input_str):
        match = re.search(r'{}: ([\d,]+)'.format(preceding_text), input_str)
        if match:
            return int(match.group(1).replace(',', ''))
        return False

    def parse_comma_int(self, input_str):
        return int(input_str.replace(',', ''))

    def parse_mdh_date(self, input_str, today):
        return datetime.datetime.strptime('{}/{}'.format(input_str, today.year), '%m/%d/%Y')

    def get_statewide_cases_timeseries(self, soup):
        '''How to deal with back-dated statewide totals if they use sample dates'''
        print('Parsing statewide cases timeseries...')
        cases_timeseries = self.full_table_parser(soup, 'Specimen collection date') # Fragile
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
                    total_cases=self.parse_comma_int(c['Cumulative positive cases']),
                    scrape_date=today,
                )
                case_objs.append(co)
            print('Adding {} records of case timeseries data'.format(len(case_objs)))
            StatewideCasesBySampleDate.objects.bulk_create(case_objs)

    def get_statewide_tests_timeseries(self, soup):
        print('Parsing statewide tests timeseries...')
        tests_timeseries = self.full_table_parser(soup, 'Completed tests reported from external laboratories (daily)') # Fragile
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

                std = StatewideTestsDate(
                    reported_date=reported_date,
                    new_state_tests=self.parse_comma_int(c['Completed tests reported from the MDH Public Health Lab (daily)']),
                    new_external_tests=self.parse_comma_int(c['Completed tests reported from external laboratories (daily)']),
                    total_tests=self.parse_comma_int(c['Total approximate number of completed tests ']),
                    scrape_date=today,
                )
                test_objs.append(std)
            print('Adding {} records of test timeseries data'.format(len(test_objs)))
            StatewideTestsDate.objects.bulk_create(test_objs)


    # TODO: Hospitalization timeseries table

    def get_statewide_data(self, soup):
        # soup = BeautifulSoup(html, 'html.parser')
        output = {}

        hosp_table_latest = self.detail_tables_regex(soup, 'Hospitalized in ICU (daily)')
        output['cumulative_hospitalized'] = self.parse_comma_int(hosp_table_latest['Total hospitalizations'])
        output['currently_in_icu'] = self.parse_comma_int(hosp_table_latest['Hospitalized in ICU (daily)'])
        output['currently_non_icu_hospitalized'] = self.parse_comma_int(hosp_table_latest['Hospitalized, not in ICU (daily)'])  # Not used except to add up
        output['currently_hospitalized'] = output['currently_in_icu'] + output['currently_non_icu_hospitalized']

        deaths_table_latest = self.detail_tables_regex(soup, 'Total deaths')
        output['cumulative_statewide_deaths'] = self.parse_comma_int(deaths_table_latest['Total deaths'])

        # recoveries_table_latest = self.detail_tables_regex(soup, 'No longer needing isolation')
        # output['cumulative_statewide_recoveries'] = self.parse_comma_int(recoveries_table_latest['No longer needing isolation'])

        # output['cumulative_statewide_recoveries'] = self.parse_comma_int(recoveries_table_latest['No longer needing isolation'])

        # ps = soup.find_all('p')
        # for p in ps:
        #     cumulative_positive_tests_match = self.p_regex('Total positive', p.text)
        #     if cumulative_positive_tests_match:
        #         output['cumulative_positive_tests'] = cumulative_positive_tests_match

        uls = soup.find_all('ul')
        for ul in uls:
            cumulative_positive_tests_match = self.ul_regex('Total positive', ul.text)
            if cumulative_positive_tests_match:
                output['cumulative_positive_tests'] = cumulative_positive_tests_match

            cumulative_completed_mdh_match = self.ul_regex('Total approximate number of completed tests from the MDH Public Health Lab', ul.text)
            if cumulative_completed_mdh_match:
                output['cumulative_completed_mdh'] = cumulative_completed_mdh_match

            cumulative_completed_private_match = self.ul_regex('Total approximate number of completed tests from external laboratories', ul.text)
            if cumulative_completed_private_match:
                output['cumulative_completed_private'] = cumulative_completed_private_match

            recoveries_match = self.ul_regex('Patients no longer needing isolation', ul.text)
            if recoveries_match:
                output['cumulative_statewide_recoveries'] = recoveries_match


        # print(cumulative_hospitalized, currently_hospitalized, currently_in_icu)
        return output

    def change_sign(self, input_int):
        optional_plus = ''
        if input_int != 0:
            optional_plus = '+'
            if input_int < 0:
                optional_plus = ':rotating_light: '
        return '{}{}'.format(optional_plus, f'{input_int:,}')

    def full_table_parser(self, soup, find_str):
        ''' should work on multiple columns '''
        table = soup.find("th", text=find_str).find_parent("table")
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
        recent_deaths_ages = self.full_table_parser(soup, 'County of residence') # Fragile
        print(recent_deaths_ages)
        cleaned_data = []
        for group in recent_deaths_ages:
            clean_row = {
                'scrape_date': today,
                'county__name': group['County of residence'],
                'age_group': group['Age group'].replace(' years', '').strip(),
                'count': int(group['Number of newly reported deaths']),
            }
            cleaned_data.append(clean_row)
        return cleaned_data

    def get_existing_recent_deaths_records(self):
        ''' See if you already have any deaths from today'''
        today = datetime.date.today()
        max_death_date = Death.objects.all().aggregate(Max('scrape_date'))['scrape_date__max']
        if max_death_date != today:
            bool_yesterday = True
        else:
            bool_yesterday = False

        most_recent_deaths = Death.objects.filter(scrape_date=max_death_date).values('scrape_date', 'county__name', 'age_group').annotate(count=Count('pk'))
        print(most_recent_deaths, bool_yesterday)
        return most_recent_deaths, bool_yesterday
        # if max_death_date == today:
        #     # There is already data for today, compare to that
        # print(max_death_date)
        #     today_deaths = Death.objects.filter(scrape_date=today).values('scrape_date', 'county__name', 'age_group').annotate(count=Count('pk'))
        #     return today_deaths
        # else:
        #     # There isn't data yet for today, so compare to existing max day
        # if today_deaths.count() > 0:
        #     # TODO: Are these the same as yesterday's? (I.E. has the daily update not happened yet)
        #
        #     return today_deaths
        #
        #
        # return None

    def reconcile_load_recent_deaths(self, scraped_deaths, existing_deaths, bool_yesterday):
        # print([{i:d[i] for i in d if i != 'scrape_date'} for d in scraped_deaths])
        if bool_yesterday:
            print('Last data is from yesterday')
            # TODO: Date is different, that is why you fail

            # date_stripped_scraped = [{'county__name': i['county__name'], 'age_group': i['age_group'], 'count': i['count']} for i in scraped_deaths]
            date_stripped_scraped = [{i:d[i] for i in d if i != 'scrape_date'} for d in scraped_deaths]
            date_stripped_existing = [{i:d[i] for i in d if i != 'scrape_date'} for d in existing_deaths]
            print(date_stripped_scraped)
            print(date_stripped_existing)

            bool_changes = [i for i in date_stripped_scraped if i not in date_stripped_existing] != []
            print(bool_changes)


            if bool_changes:
                print('New data for today...')
                existing_deaths = None  # Ignore old data
            else:
                print('No changes from yesterday yet.')
                return False

        # Looking for updates from today
        if existing_deaths:
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
            extra_deaths = []
            for group in deaths_to_remove:
                remove_count = group['count']
                county = County.objects.get(name=group['county__name'])
                while remove_count < 0:
                    Death.objects.filter(scrape_date=group['scrape_date'], age_group=group['age_group'], county=county).last().delete()
                    remove_count += 1

        else:  # This should happen on first run of the day with changes
            print('Starting fresh...')
            deaths_to_add = scraped_deaths

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
        # soup = BeautifulSoup(html, 'html.parser')

        ages_data = self.full_table_parser(soup, 'Age Group')
        cleaned_ages_data = []
        for d in ages_data:
            d['Number of Cases'] = self.pct_filter(d['Number of Cases'])
            d['Number of Deaths'] = self.pct_filter(d['Number of Deaths'])
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

            final_msg = 'COVID scraper found updated data on the <https://www.health.state.mn.us/diseases/coronavirus/situation.html|MDH situation page>...\n\n' + msg_output + '\n'
            print(final_msg)

            return final_msg

        return 'COVID scraper: No updates found in statewide numbers.\n\n'

    def updated_today(self, soup):
        update_date_node = soup.find("strong", text=re.compile('Updated [A-z]+ \d{1,2}, \d{4}.')).text
        update_date = datetime.datetime.strptime(re.search('([A-z]+ \d{1,2}, \d{4})', update_date_node).group(1), '%B %d, %Y')
        if update_date.date() == datetime.datetime.now().date():
            return True
        else:
            return False

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

            soup = BeautifulSoup(html, 'html.parser')

            # Will make this more important after we're sure it works
            bool_updated_today = self.updated_today(soup)
            if bool_updated_today:
                print('Updated today')
            else:
                print('No update yet today')

            # statewide_data = self.get_statewide_data(soup)
            # statewide_msg_output = self.update_statewide_records(statewide_data)
            #
            self.get_statewide_cases_timeseries(soup)
            self.get_statewide_tests_timeseries(soup)
            #
            # age_data = self.get_age_data(soup)
            # # print(age_data)
            # age_msg_output = self.update_age_records(age_data)
            #
            # # recent_deaths_data = self.get_recent_deaths_data(soup)
            # # print(recent_deaths_data)
            # #
            # # existing_today_deaths, bool_yesterday = self.get_existing_recent_deaths_records()
            # # print(existing_today_deaths, bool_yesterday)
            # # death_msg_output = self.reconcile_load_recent_deaths(recent_deaths_data, existing_today_deaths, bool_yesterday)
            #
            # county_data = self.get_county_data(soup)
            # county_msg_output = self.update_county_records(county_data)
            # # county_msg_output = "Leaving county counts at yesterday's total until state clarifies"
            #
            # print(statewide_data['cumulative_positive_tests'], previous_statewide_cases)
            # if statewide_data['cumulative_positive_tests'] != previous_statewide_cases:
            #     new_statewide_cases = statewide_data['cumulative_positive_tests'] - previous_statewide_cases
            #     # slack_header = '*{} new cases announced statewide.*\n\n'.format(new_statewide_cases)
            #     # slack_latest(statewide_msg_output, '#virus')
            #     slack_latest(statewide_msg_output + county_msg_output, '#virus')
            #     # slack_latest(statewide_msg_output + county_msg_output, '#covid-tracking')
            # else:
            #     # slack_latest(statewide_msg_output + county_msg_output, '#robot-dojo')
            #     # slack_latest('Scraper update: No county changes detected.', '#covid-tracking')
            #     # slack_latest(statewide_msg_output + county_msg_output, '#virus')  # Force output anyway
            #     slack_latest('COVID scraper update: No changes detected.', '#robot-dojo')
