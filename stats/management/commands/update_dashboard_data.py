import os
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
    ]

    def get_csv(self):
        for f in self.FILENAMES:
            r = requests.get(f['orig_file'])
            if r.status_code == requests.codes.ok:

                # Save a copy of CSV
                now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
                outpath = os.path.join(settings.BASE_DIR, 'exports', 'dashboard', f['download_base_name']).format(now)
                with open(outpath, 'w') as csv_file:
                    csv_file.write(r.text)
                    csv_file.close()
            else:
                slack_latest('WARNING: Dashboard CSV scraper error.: {}'.format(f['orig_file']), '#robot-dojo')

    def handle(self, *args, **options):
        self.get_csv()
