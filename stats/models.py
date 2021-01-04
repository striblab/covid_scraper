from datetime import timedelta
from django.db import models


class County(models.Model):
    name = models.CharField(max_length=50)
    fips = models.CharField(max_length=5)
    latitude = models.FloatField()
    longitude = models.FloatField()
    pop_2010 = models.IntegerField()
    pop_2019 = models.IntegerField(null=True)

    def __str__(self):
        return self.name

class AgeGroupPop(models.Model):
    ''' static population stats '''
    age_group = models.CharField(max_length=100)
    age_min = models.IntegerField(null=True)
    age_max = models.IntegerField(null=True)
    population = models.IntegerField()
    pct_pop = models.IntegerField(null=True)


class Death(models.Model):
    scrape_date = models.DateField()
    age_group = models.CharField(max_length=100)
    actual_age = models.IntegerField(null=True)
    county = models.ForeignKey(County, null=True, on_delete=models.CASCADE)
    bool_ltc = models.BooleanField(null=True)

    # scrape_date = models.DateField()
    last_update = models.DateTimeField(auto_now=True)


class StatewideAgeDate(models.Model):
    scrape_date = models.DateField()
    age_group = models.CharField(max_length=100)
    age_min = models.IntegerField(null=True)
    age_max = models.IntegerField(null=True)
    cases_pct = models.IntegerField(default=None, null=True)
    deaths_pct = models.IntegerField(default=None, null=True)
    case_count = models.IntegerField(default=None, null=True)
    death_count = models.IntegerField(default=None, null=True)
    last_update = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):

        split_ages = self.age_group.replace(' years', '').split('-')

        if self.age_group == '100+ years':
            self.age_min = 100
            self.age_max = 200
        elif len(split_ages) == 2:
            self.age_min = split_ages[0]
            self.age_max = split_ages[1]
        super(StatewideAgeDate, self).save(*args, **kwargs)


class StatewideCasesBySampleDate(models.Model):
    '''used for timeseries and charts'''
    sample_date = models.DateField(null=True)
    new_cases = models.IntegerField(default=0, null=True)
    total_cases = models.IntegerField(default=0, null=True)

    # oct 14 add
    new_pcr_tests = models.IntegerField(default=0, null=True)
    new_antigen_tests = models.IntegerField(default=0, null=True)
    total_pcr_tests = models.IntegerField(default=0, null=True)
    total_antigen_tests = models.IntegerField(default=0, null=True)

    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()


class StatewideHospitalizationsDate(models.Model):
    reported_date = models.DateField(null=True)
    new_hosp_admissions = models.IntegerField(default=0, null=True)
    # new_non_icu_admissions_rolling = models.FloatField(default=0, null=True)
    new_icu_admissions = models.IntegerField(default=0, null=True)
    # new_icu_admissions_rolling = models.FloatField(default=0, null=True)
    total_hospitalizations = models.IntegerField(default=0, null=True)
    total_icu_admissions = models.IntegerField(default=0, null=True)
    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()


class StatewideDeathsDate(models.Model):
    reported_date = models.DateField(null=True)
    new_deaths = models.IntegerField(default=0, null=True)
    new_deaths_rolling = models.FloatField(default=0, null=True)  # No longer calculated at DB level
    total_deaths = models.IntegerField(default=0, null=True)
    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()


class StatewideTestsDate(models.Model):
    '''used for timeseries and charts'''
    reported_date = models.DateField(null=True)
    new_state_tests = models.IntegerField(default=0, null=True)
    new_external_tests = models.IntegerField(default=0, null=True)

    # oct 14 add
    new_pcr_tests = models.IntegerField(default=0, null=True)
    new_antigen_tests = models.IntegerField(default=0, null=True)
    total_pcr_tests = models.IntegerField(default=0, null=True)
    total_antigen_tests = models.IntegerField(default=0, null=True)

    new_tests = models.IntegerField(default=0, null=True)
    new_tests_rolling = models.FloatField(default=0, null=True)
    total_tests = models.IntegerField(default=0, null=True)
    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()


class StatewideTotalDate(models.Model):
    '''used for topline numbers'''
    cases_daily_change = models.IntegerField(default=0, null=True)  # Difference between total yesterday and today
    cases_newly_reported  = models.IntegerField(default=0, null=True)  # "new" cases per MDH, should add up to daily change when combined with cases_removed
    confirmed_cases_newly_reported = models.IntegerField(default=0, null=True)
    probable_cases_newly_reported = models.IntegerField(default=0, null=True)
    removed_cases = models.IntegerField(default=0, null=True)
    deaths_daily_change = models.IntegerField(default=0, null=True)
    cumulative_positive_tests = models.IntegerField(default=0, null=True)
    cumulative_confirmed_cases = models.IntegerField(default=0, null=True)
    cumulative_probable_cases = models.IntegerField(default=0, null=True)
    cumulative_completed_tests = models.IntegerField(default=0, null=True)
    cumulative_completed_mdh = models.IntegerField(default=0, null=True)
    cumulative_completed_private = models.IntegerField(default=0, null=True)
    cumulative_pcr_tests = models.IntegerField(default=0, null=True)
    cumulative_antigen_tests = models.IntegerField(default=0, null=True)
    cumulative_hospitalized = models.IntegerField(default=0, null=True)
    cumulative_icu = models.IntegerField(default=0, null=True)
    currently_hospitalized = models.IntegerField(default=0, null=True)  # Deprecated as of 9/4/2020, but have old data
    currently_in_icu = models.IntegerField(default=0, null=True)  # Deprecated as of 9/4/2020, but have old data
    hospitalized_total_daily_change = models.IntegerField(default=0, null=True)
    icu_total_daily_change = models.IntegerField(default=0, null=True)
    cumulative_statewide_deaths = models.IntegerField(default=0, null=True)  # Captured separately from county totals, so they may not match
    cumulative_statewide_recoveries = models.IntegerField(default=0, null=True)
    cumulative_confirmed_statewide_deaths = models.IntegerField(default=0, null=True)
    cumulative_probable_statewide_deaths = models.IntegerField(default=0, null=True)
    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()
    last_update = models.DateTimeField(auto_now=True)

    def backfill_new_deaths(self):
        if self.new_deaths == 0:
            try:
                previous_day = StatewideTotalDate.objects.get(scrape_date=self.scrape_date - timedelta(days=1))
                daily_deaths = self.cumulative_statewide_deaths - previous_day.cumulative_statewide_deaths
                # print(self.cumulative_statewide_deaths - previous_day.cumulative_statewide_deaths)
                self.new_deaths = daily_deaths
                self.save()
                return daily_deaths
            except:
                print("can't find previous...")
        else:
            return self.new_deaths


class CountyTestDate(models.Model):
    '''The daily total number of tests, according to the Minnesota Department of Health. If you scrape more than once a day, the count will be updated, so there is only 1 record per day.'''
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    daily_total_cases = models.IntegerField()
    cumulative_count = models.IntegerField()

    # oct 14 add
    cumulative_confirmed_cases = models.IntegerField(default=0, null=True)
    cumulative_probable_cases = models.IntegerField(default=0, null=True)

    daily_deaths = models.IntegerField(default=0, null=True)  # Deaths on this date
    cumulative_deaths = models.IntegerField(default=0, null=True)  # Deaths by this date
    update_date = models.DateField(null=True)  # The day MDH said this data was last updated
    scrape_date = models.DateField()

    last_scrape = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '{} {}: {}'.format(self.county.name, self.scrape_date, self.daily_total_cases)


class ZipCasesDate(models.Model):
    ''' Loading and exporting for this is now handled by Lambda but this model db table is still used for that'''
    data_date = models.DateField()
    zip = models.CharField(max_length=7)
    cases_cumulative = models.IntegerField()
    import_date = models.DateTimeField(auto_now_add=True)


class VacAdminTotalDate(models.Model):
    ''' Loading and exporting for this is now handled by Lambda but this model db table is still used for that'''
    data_date = models.DateField()
    admin_total= models.IntegerField(null=True)
    admin_pfizer= models.IntegerField(null=True)
    admin_moderna= models.IntegerField(null=True)
    admin_unknown= models.IntegerField(null=True)
    update_date = models.DateTimeField(auto_now=True)
