import os
import csv
import json
import requests
import datetime

from django.core.management.base import BaseCommand
from stats.utils import slack_latest

from django.conf import settings


class Command(BaseCommand):
    help = 'Check for new or updated results from the Minnesota "dashboard": https://mn.gov/covid19/data/response.jsp'

    HOSP_CAPACITY_CSV = 'https://public.tableau.com/vizql/w/StateofMNResponseDashboard/v/COVIDResponseDashboard/vud/sessions/{sessionid}/views/14008886057350627737_5353518886485370878?csv=true'

    SUPPLIES_CSV = 'https://public.tableau.com/vizql/w/StateofMNResponseDashboard/v/COVIDResponseDashboard/vud/sessions/{sessionid}/views/14008886057350627737_513822615169932817?csv=true'


    def initialize_session(self, session):
        h = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://public.tableau.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        }
        baseurl = "https://public.tableau.com/views/StateofMNResponseDashboard/COVIDResponseDashboard?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Fpublic.tableau.com%2F&:embed_code_version=3&:tabs=no&:toolbar=yes&:animate_transition=yes&:display_static_image=no&:display_spinner=no&:display_overlay=yes&:display_count=yes&publish=yes&:loadOrderID=0"
        r = session.get(baseurl, headers=h)
        # print(json.dumps(dict(r.headers), indent=2))
        sessionid = r.headers["X-Session-Id"]
        # print(sessionid)

        csv = f"https://public.tableau.com/vizql/w/StateofMNResponseDashboard/v/COVIDResponseDashboard/bootstrapSession/sessions/{sessionid}"
        data = "worksheetPortSize=%7B%22w%22%3A1100%2C%22h%22%3A850%7D&dashboardPortSize=%7B%22w%22%3A1100%2C%22h%22%3A850%7D&clientDimension=%7B%22w%22%3A1398%2C%22h%22%3A816%7D&renderMapsClientSide=true&isBrowserRendering=true&browserRenderingThreshold=100&formatDataValueLocally=false&clientNum=&navType=Nav&navSrc=Boot&devicePixelRatio=1&clientRenderPixelLimit=25000000&allowAutogenWorksheetPhoneLayouts=false&sheet_id=COVIDResponseDashboard&showParams=%7B%22checkpoint%22%3Afalse%2C%22refresh%22%3Afalse%2C%22refreshUnmodified%22%3Afalse%7D&stickySessionKey=%7B%22featureFlags%22%3A%22%7B%5C%22MetricsAuthoringBeta%5C%22%3Afalse%7D%22%2C%22isAuthoring%22%3Afalse%2C%22isOfflineMode%22%3Afalse%2C%22lastUpdatedAt%22%3A1586192662452%2C%22viewId%22%3A31911253%2C%22workbookId%22%3A5922257%7D&filterTileSize=200&locale=en_US&language=en&verboseMode=false&%3Asession_feature_flags=%7B%7D&keychain_version=1"
        r = session.post(csv, data=data, headers=h)

        return sessionid

    def get_hospital_capacities(self, session, sessionid):
        print("Caching hospitalization data...")
        r = session.get(self.HOSP_CAPACITY_CSV.format(sessionid=sessionid))
        if r.status_code == requests.codes.ok:
            # print(r.text)
            # Save a copy of CSV
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            outpath = os.path.join(settings.BASE_DIR, 'exports', 'dashboard', 'hospital_capacity_{}.csv').format(now)
            with open(outpath, 'w') as csv_file:
                csv_file.write(r.text)
                csv_file.close()
        else:
            slack_latest('WARNING: Hospital capacity CSV scraper error. Not proceeding.', '#robot-dojo')

    def get_critical_supplies(self, session, sessionid):
        print("Caching critical supplies data...")
        r = session.get(self.SUPPLIES_CSV.format(sessionid=sessionid))
        if r.status_code == requests.codes.ok:
            # print(r.text)
            # Save a copy of CSV
            now = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
            outpath = os.path.join(settings.BASE_DIR, 'exports', 'dashboard', 'supplies_{}.csv').format(now)
            with open(outpath, 'w') as csv_file:
                csv_file.write(r.text)
                csv_file.close()
        else:
            slack_latest('WARNING: Critical supplies CSV scraper error. Not proceeding.', '#robot-dojo')

    def handle(self, *args, **options):
        session = requests.session()
        sessionid = self.initialize_session(session)
        self.get_hospital_capacities(session, sessionid)
        self.get_critical_supplies(session, sessionid)
