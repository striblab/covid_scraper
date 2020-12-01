from django.core.management.base import BaseCommand
from django.db.models import Window, F
from django.db.models.functions import Lead
from stats.models import StatewideHospitalizationsDate, StatewideTotalDate


class Command(BaseCommand):
    help = 'Copy daily change numbers from timeseries model to main daily results model for simplication of outputting in timeseries spreadsheet'

    def get_timeseries_values(self):
        # this is a weird one -- we want the totals as they were reported at the time, not the updated totals. Only needed for the cumulative totals
        hosp_totals_values = {}
        hosp_scrape_dates =  StatewideHospitalizationsDate.objects.all().values_list('scrape_date', flat=True).distinct()
        for t in hosp_scrape_dates:
            try:
                # For most dates now the max value is contained in the "Missing" cell
                total_record = StatewideHospitalizationsDate.objects.get(scrape_date=t, reported_date=None).__dict__
            except:
                total_record = StatewideHospitalizationsDate.objects.filter(scrape_date=t).latest('reported_date').__dict__
            hosp_totals_values[t] = total_record

        return hosp_totals_values

    def handle(self, *args, **options):
        ts_values = self.get_timeseries_values()
        for td in StatewideTotalDate.objects.all().order_by('scrape_date'):
            if td.scrape_date in ts_values:
                td.cumulative_icu = ts_values[td.scrape_date]['total_icu_admissions']
            else:
                td.cumulative_icu = 0
            td.save()

        # Now calculate daily change
        for ntd in StatewideTotalDate.objects.all().annotate(
                prev_cumulative_icu=Window(
                    expression=Lead('cumulative_icu', offset=1, default=0),
                    order_by=F('scrape_date').desc()
                ),
                difference=F('cumulative_icu')-F('prev_cumulative_icu')
            ).order_by('scrape_date'):
            ntd.icu_total_daily_change = ntd.difference
            ntd.save()
