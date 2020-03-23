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
    county = models.ForeignKey(County, on_delete=models.CASCADE)
    case_count = models.IntegerField()
    scrape_date = models.DateField(auto_now=True)
    last_update = models.DateTimeField(auto_now=True)
