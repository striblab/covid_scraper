import os
import pandas as pd
import requests

from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Grab the latest state-by-state figures from Covid Tracking Project, calculate per capita stuff'

    CTP_ENDPOINT = 'https://covidtracking.com/api/v1/states/current.json'

    STATES_LATEST_EXPORT_PATH = os.path.join(settings.BASE_DIR, 'exports', 'states_latest_ctp.json')

    STATE_POPS = [
        {"state_name": "Alaska", "state_abbr": "AK", "pop": 731545},
        {"state_name": "Alabama", "state_abbr": "AL", "pop": 4903185},
        {"state_name": "Arkansas", "state_abbr": "AR", "pop": 3017804},
        # {"state_name": "American Samoa", "state_abbr": "AS", "pop": #N/A},
        {"state_name": "Arizona", "state_abbr": "AZ", "pop": 7278717},
        {"state_name": "California", "state_abbr": "CA", "pop": 39512223},
        {"state_name": "Colorado", "state_abbr": "CO", "pop": 5758736},
        {"state_name": "Connecticut", "state_abbr": "CT", "pop": 3565287},
        {"state_name": "District Of Columbia", "state_abbr": "DC", "pop": 705749},
        {"state_name": "Delaware", "state_abbr": "DE", "pop": 973764},
        {"state_name": "Florida", "state_abbr": "FL", "pop": 21477737},
        {"state_name": "Georgia", "state_abbr": "GA", "pop": 10617423},
        # {"state_name": "Guam", "state_abbr": "GU", "pop": #N/A},
        {"state_name": "Hawaii", "state_abbr": "HI", "pop": 1415872},
        {"state_name": "Iowa", "state_abbr": "IA", "pop": 3155070},
        {"state_name": "Idaho", "state_abbr": "ID", "pop": 1787065},
        {"state_name": "Illinois", "state_abbr": "IL", "pop": 12671821},
        {"state_name": "Indiana", "state_abbr": "IN", "pop": 6732219},
        {"state_name": "Kansas", "state_abbr": "KS", "pop": 2913314},
        {"state_name": "Kentucky", "state_abbr": "KY", "pop": 4467673},
        {"state_name": "Louisiana", "state_abbr": "LA", "pop": 4648794},
        {"state_name": "Massachusetts", "state_abbr": "MA", "pop": 6892503},
        {"state_name": "Maryland", "state_abbr": "MD", "pop": 6045680},
        {"state_name": "Maine", "state_abbr": "ME", "pop": 1344212},
        {"state_name": "Michigan", "state_abbr": "MI", "pop": 9986857},
        {"state_name": "Minnesota", "state_abbr": "MN", "pop": 5639632},
        {"state_name": "Missouri", "state_abbr": "MO", "pop": 6137428},
        # {"state_name": "Northern Mariana Islands", "state_abbr": "MP", "pop": #N/A},
        {"state_name": "Mississippi", "state_abbr": "MS", "pop": 2976149},
        {"state_name": "Montana", "state_abbr": "MT", "pop": 1068778},
        {"state_name": "North Carolina", "state_abbr": "NC", "pop": 10488084},
        {"state_name": "North Dakota", "state_abbr": "ND", "pop": 762062},
        {"state_name": "Nebraska", "state_abbr": "NE", "pop": 1934408},
        {"state_name": "New Hampshire", "state_abbr": "NH", "pop": 1359711},
        {"state_name": "New Jersey", "state_abbr": "NJ", "pop": 8882190},
        {"state_name": "New Mexico", "state_abbr": "NM", "pop": 2096829},
        {"state_name": "Nevada", "state_abbr": "NV", "pop": 3080156},
        {"state_name": "New York", "state_abbr": "NY", "pop": 19453561},
        {"state_name": "Ohio", "state_abbr": "OH", "pop": 11689100},
        {"state_name": "Oklahoma", "state_abbr": "OK", "pop": 3956971},
        {"state_name": "Oregon", "state_abbr": "OR", "pop": 4217737},
        {"state_name": "Pennsylvania", "state_abbr": "PA", "pop": 12801989},
        {"state_name": "Puerto Rico", "state_abbr": "PR", "pop": 3193694},
        {"state_name": "Rhode Island", "state_abbr": "RI", "pop": 1059361},
        {"state_name": "South Carolina", "state_abbr": "SC", "pop": 5148714},
        {"state_name": "South Dakota", "state_abbr": "SD", "pop": 884659},
        {"state_name": "Tennessee", "state_abbr": "TN", "pop": 6829174},
        {"state_name": "Texas", "state_abbr": "TX", "pop": 28995881},
        {"state_name": "Utah", "state_abbr": "UT", "pop": 3205958},
        {"state_name": "Virginia", "state_abbr": "VA", "pop": 8535519},
        # {"state_name": "US Virgin Islands", "state_abbr": "VI", "pop": #N/A},
        {"state_name": "Vermont", "state_abbr": "VT", "pop": 623989},
        {"state_name": "Washington", "state_abbr": "WA", "pop": 7614893},
        {"state_name": "Wisconsin", "state_abbr": "WI", "pop": 5822434},
        {"state_name": "West Virginia", "state_abbr": "WV", "pop": 1792147},
        {"state_name": "Wyoming", "state_abbr": "WY", "pop": 578759},
    ]

    def get_ctp(self):
        r = requests.get(self.CTP_ENDPOINT)
        if r.status_code == requests.codes.ok:
            return r.json()
        return False

    def join_and_slice(self, ctp):
        pop_df = pd.DataFrame(self.STATE_POPS)
        ctp_df = pd.DataFrame(ctp)
        merged = pop_df.merge(ctp_df, how="left", left_on="state_abbr", right_on="state")
        merged['cases_p_100k'] = round((merged['positive'] / merged['pop']) * 100000, 1)
        merged['deaths_p_100k'] = round((merged['death'] / merged['pop']) * 100000, 1)
        merged['tests_p_100k'] = round((merged['totalTestResults'] / merged['pop']) * 100000, 1)
        merged['pct_tests_pos'] = round(merged['positive'] / merged['totalTestResults'], 3)

        merged.rename(columns={'positive': 'cases', 'death': 'deaths', 'totalTestResults': 'total_tests', 'dateModified': 'last_update'}, inplace=True)

        out_df = merged[[
            'fips',
            'state_abbr',
            'state_name',
            'cases',
            'cases_p_100k',
            'deaths',
            'deaths_p_100k',
            'total_tests',
            'tests_p_100k',
            'pct_tests_pos',
            'last_update'
        ]]

        return out_df

    def handle(self, *args, **options):
        print('Getting latest CTP state-by-state figures ...')

        ctp = self.get_ctp()
        out_df = self.join_and_slice(ctp)
        out_df.to_json(self.STATES_LATEST_EXPORT_PATH, orient="records")
