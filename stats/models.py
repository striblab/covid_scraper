from django.db import models


class County(models.Model):
    name = models.CharField(max_length=50)
    fips = models.CharField(max_length=5)
    latitude = models.FloatField()
    longitude = models.FloatField()
    pop_2010 = models.IntegerField()

    def __str__(self):
        return self.name


class CountyTestDate(models.Model):
    '''The daily total number of tests, according to the Minnesota Department of Health. If you scrape more than once a day, the count will be updated, so there is only 1 record per day.'''
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    case_count = models.IntegerField()
    scrape_date = models.DateField()
    last_update = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '{} {}: {}'.format(self.county.name, self.scrape_date, self.case_count)
