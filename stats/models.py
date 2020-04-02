from django.db import models


class County(models.Model):
    name = models.CharField(max_length=50)
    fips = models.CharField(max_length=5)
    latitude = models.FloatField()
    longitude = models.FloatField()
    pop_2010 = models.IntegerField()

    def __str__(self):
        return self.name


class StatewideTotalDate(models.Model):
    cumulative_positive_tests = models.IntegerField(default=0)
    cumulative_completed_tests = models.IntegerField(default=0)
    cumulative_completed_mdh = models.IntegerField(default=0)
    cumulative_completed_private = models.IntegerField(default=0)

    cumulative_hospitalized = models.IntegerField(default=0)
    currently_hospitalized = models.IntegerField(default=0)
    currently_in_icu = models.IntegerField(default=0)

    cumulative_statewide_deaths = models.IntegerField(default=0)  # Captured separately from county totals, so they may not match
    cumulative_statewide_recoveries = models.IntegerField(default=0)

    scrape_date = models.DateField()
    last_update = models.DateTimeField(auto_now=True)


class CountyTestDate(models.Model):
    '''The daily total number of tests, according to the Minnesota Department of Health. If you scrape more than once a day, the count will be updated, so there is only 1 record per day.'''
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    daily_count = models.IntegerField()
    cumulative_count = models.IntegerField()
    daily_deaths = models.IntegerField(default=0)  # Deaths on this date
    cumulative_deaths = models.IntegerField(default=0)  # Deaths by this date
    scrape_date = models.DateField()
    last_update = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '{} {}: {}'.format(self.county.name, self.scrape_date, self.daily_count)
