import os
import csv
import pandas as pd

from django.conf import settings
from django.core.management.base import BaseCommand
from stats.models import County


class Command(BaseCommand):
    help = 'One-time helper to add 2019 populations to MN counties'

    CENSUS_POP_ESTIMATES_LOCAL = os.path.join(settings.BASE_DIR, 'data', 'co-est2019-alldata.csv')

    def handle(self, *args, **options):
        pops_2019 = pd.read_csv(self.CENSUS_POP_ESTIMATES_LOCAL, encoding='latin-1', dtype={'STATE': 'object', 'COUNTY': 'object'})
        pops_2019['full_fips'] = pops_2019['STATE'] + pops_2019['COUNTY']
        mn_pops = pops_2019[pops_2019['STATE'] == '27']
        # print(pops_2019.head())

        for c in County.objects.all():
            pop_match = mn_pops[mn_pops['full_fips'] == c.fips].POPESTIMATE2019.item()
            print(c.name, c.fips, pop_match)
            c.pop_2019 = pop_match
            c.save()
