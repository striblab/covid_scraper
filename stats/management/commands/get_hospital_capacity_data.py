import os
import io
import csv
import requests
import datetime
import pandas as pd
from django.core.management.base import BaseCommand
from django.conf import settings

from stats.utils import slack_latest

class Command(BaseCommand):
    help = 'Check for new or updated results from the Minnesota "dashboard": https://mn.gov/covid19/data/response.jsp'

    # RAW_OUTPATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_hosp_capacity_timeseries_raw.csv')
    CLEAN_OUTPATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_hosp_capacity_timeseries.csv')

    HEADERS = {
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    }

    def reshape_capacities(self, raw_df):
        capacity_df = raw_df[raw_df.Detail2.isin(['CAPACITY', 'Capacity'])][[
            'Detail1',
            'Detail3',
            'Detail4',
            'Data Date (MM/DD/YYYY)',
            'Value_NUMBER'
        ]].fillna('')
        capacity_df['detail_combined'] = capacity_df['Detail1'] + capacity_df['Detail3'] + capacity_df['Detail4']
        capacity_df.drop(columns=['Detail1', 'Detail3', 'Detail4'], inplace=True)

        capacity_pivot = pd.pivot_table(capacity_df, values='Value_NUMBER', index=['Data Date (MM/DD/YYYY)'], columns=['detail_combined']).reset_index()

        capacity_pivot.rename(columns={
            'Data Date (MM/DD/YYYY)': 'data_date',
            'ICU': 'icu_cap',
            'ICUSurgeReady in 72 hours': 'icu_cap_72',
            'Non-ICU': 'non_icu_cap',
            'Ventilators': 'vent_cap',
            'VentilatorsSurgeSurge': 'vent_cap_surge'
        }, inplace=True)

        return capacity_pivot

    def reshape_counts(self, raw_df):
        counts_df = raw_df[raw_df.Detail2 == 'IN USE'][[
            'Detail1',
            'Detail3',
            'Data Date (MM/DD/YYYY)',
            'Value_NUMBER'
        ]].fillna('')
        counts_df['detail_combined'] = counts_df['Detail1'] + counts_df['Detail3']
        counts_df.drop(columns=['Detail1', 'Detail3'], inplace=True)

        counts_pivot = pd.pivot_table(counts_df, values='Value_NUMBER', index=['Data Date (MM/DD/YYYY)'],columns=['detail_combined']).reset_index()

        counts_pivot.rename(columns={
            'Data Date (MM/DD/YYYY)': 'data_date',
            'ICUCOVID+': 'icu_covid',
            'ICUnon-COVID+': 'icu_other',
            'Non-ICUCOVID+': 'non_icu_covid',
            'Non-ICUnon-COVID+': 'non_icu_other',
            'Ventilators': 'vents'
        }, inplace=True)

        return counts_pivot

    def handle(self, *args, **options):
        try:
            s = requests.Session()
            # r = s.get('https://mn.gov/covid19/assets/HospitalCapacity_HistoricCSV_tcm1148-449110.csv', headers=self.HEADERS)
            r = s.get('https://mn.gov/covid19/assets/MNTRAC_ICU_NonICU_BedAvailability_IdentifiedSurge_CSV_tcm1148-455098.csv', headers=self.HEADERS)

            csv_obj = csv.DictReader(io.StringIO(r.text))
            capacity_raw_df = pd.DataFrame(csv_obj)
            capacity_raw_df['Value_NUMBER'] = pd.to_numeric(capacity_raw_df['Value_NUMBER'])

            capacity_raw_df_statewide = capacity_raw_df[capacity_raw_df['GeographicLevel'] == 'State']
            print(capacity_raw_df_statewide)
            # capacity_raw_df = capacity_raw_df[~capacity_raw_df['Detail4'].isin(['On back order', 'In warehouse'])]

            # capacity_pivot = self.reshape_capacities(capacity_raw_df)
            # counts_pivot = self.reshape_counts(capacity_raw_df)
            #
            # merged_df = counts_pivot.merge(capacity_pivot, how="outer", on="data_date")
            # merged_df['data_date'] = pd.to_datetime(merged_df['data_date'])
            # merged_df = merged_df.sort_values('data_date')
            #
            # merged_df['icu_total'] = merged_df['icu_covid'] + merged_df['icu_other']
            # merged_df['covid_total'] = merged_df['icu_covid'] + merged_df['non_icu_covid']
            #
            # merged_df['icu_pct_cap'] = round(merged_df['icu_total'] / merged_df['icu_cap'], 3)
            # merged_df['vent_pct_cap'] = round(merged_df['vents'] / merged_df['vent_cap'], 3)
            #
            # merged_df.to_csv(self.CLEAN_OUTPATH, index=False)

        except Exception as e:
            # raise
            slack_latest('WARNING: Hospital capacity CSV scraper error... \n{}'.format(e), '#robot-dojo')
