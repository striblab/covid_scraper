import os
import pandas as pd
import csv
import requests
import datetime

from django.core.management.base import BaseCommand
from stats.utils import slack_latest

from django.conf import settings


class Command(BaseCommand):
    help = 'Check for new or updated results from the Minnesota "dashboard": https://mn.gov/covid19/data/response.jsp'

    # legacy file: 'http://mn.gov/covid19/assets/StateofMNResponseDashboardCSV_tcm1148-427143.csv'

    FILENAMES = [
        {'orig_file': 'http://mn.gov/covid19/assets/StateofMNResponseDashboardCSV_tcm1148-427143.csv', 'download_base_name': 'db_crit_supplies_{}.csv'},
        # {'orig_file': 'https://mn.gov/covid19/assets/ProcurementCSV_tcm1148-429709.csv', 'download_base_name': 'db_procurement_legacy_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/DaysonHand_CCS_Chart_tcm1148-430853.csv', 'download_base_name': 'db_days_on_hand_chart_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/DaysonHand_CCS_Table_tcm1148-430852.csv', 'download_base_name': 'db_days_on_hand_tbl_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/SourceofCCS_Table_tcm1148-430849.csv', 'download_base_name': 'db_crit_care_supply_sources_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/ProcurementOrderDeliveryCCS_Table_tcm1148-430850.csv', 'download_base_name': 'db_procurement_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/DialBackCSV_tcm1148-431875.csv', 'download_base_name': 'db_dialback_{}.csv'},
        {'orig_file': 'https://mn.gov/covid19/assets/HospitalCapacity_HistoricCSV_tcm1148-449110.csv', 'download_base_name': 'db_hosp_cap_{}.csv'}
    ]

    HEADERS = {
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'referer': 'https://mn.gov/covid19/data/response-prep/response-capacity.jsp',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'if-none-match': 'W/"1605899003:dtagent10205201116183137LCt5:dtagent10205201116183137LCt5"',
        'if-modified-since': 'Fri, 20 Nov 2020 19:03:21 GMT',
    }





    # def get_csv(self):
    #     for f in self.FILENAMES:
    #
    #         if r.status_code == requests.codes.ok:
    #
    #             # Save a copy of CSV
    #             now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
    #             outpath = os.path.join(settings.BASE_DIR, 'exports', 'dashboard', f['download_base_name']).format(now)
    #             with open(outpath, 'w') as csv_file:
    #                 csv_file.write(r.text)
    #                 csv_file.close()
    #         else:
    #             slack_latest('WARNING: Dashboard CSV scraper error.: {}'.format(f['orig_file']), '#robot-dojo')

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

        capacity_pivot = pd.pivot_table(capacity_df, values='Value_NUMBER', index=['Data Date (MM/DD/YYYY)'],columns=['detail_combined']).reset_index()

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
        # self.get_csv()
        try:
            s = requests.Session()
            # all cookies received will be stored in the session object

            # s.get('https://mn.gov/covid19/data/response-prep/response-capacity.jsp', headers=self.HEADERS)
            r = s.get('https://mn.gov/covid19/assets/HospitalCapacity_HistoricCSV_tcm1148-449110.csv', headers=self.HEADERS)

            # r = requests.get('https://mn.gov/covid19/assets/HospitalCapacity_HistoricCSV_tcm1148-449110.csv', headers=self.HEADERS)
            print(r.text)
            # print(list(csv.DictReader(r.text)))

            # # capacity_raw_df = pd.read_csv('~/Downloads/HospitalCapacity_HistoricCSV_tcm1148-449110.csv')
            # capacity_raw_df = pd.read_csv('https://mn.gov/covid19/assets/HospitalCapacity_HistoricCSV_tcm1148-449110.csv')
            #
            # print(capacity_raw_df)
            #
            # capacity_pivot = self.reshape_capacities(capacity_raw_df)
            # counts_pivot = self.reshape_counts(capacity_raw_df)
            #
            # merged_df = counts_pivot.merge(capacity_pivot, how="outer", on="data_date")
            # merged_df['data_date'] = pd.to_datetime(merged_df['data_date'])
            # merged_df = merged_df.sort_values('data_date')
            #
            # merged_df['icu_total'] = merged_df['icu_covid'] + merged_df['icu_other']
            # merged_df['icu_pct_cap'] = round(merged_df['icu_total'] / merged_df['icu_cap'], 3)
            #
            # merged_df['vent_pct_cap'] = round(merged_df['vents'] / merged_df['vent_cap'], 3)
            #
            # merged_df['covid_total'] = merged_df['icu_covid'] + merged_df['non_icu_covid']
            #
            # # int_exclude = ['data_date', 'vent_pct_cap', 'icu_pct_cap']
            # # for col in merged_df.columns:
            # #     if col not in int_exclude:
            # #         merged_df[col] = merged_df[col].fillastype('int')
            #
            # # print(merged_df)
            #
            # OUTPATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_hosp_capacity_timeseries.csv')
            # merged_df.to_csv(OUTPATH, index=False)

        except Exception as e:
            raise
            # slack_latest('WARNING: Hospital capacity CSV scraper error... \n{}'.format(e), '#robot-dojo')
