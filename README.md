# covid_scraper

## Monitoring the scraper day to day
The daily scrape is run by the `docker-entrypoint-scrape.sh` shell script, so you can see what order things happen in from there.

The most common cause of the scrape failing is by the [MDH page](https://www.health.state.mn.us/diseases/coronavirus/situation.html) changing something. That logic is all handled in `update_mn_data.py`


## Handling holidays

1. Export previous days totals record
```
from stats.models import StatewideTotalDate
max_record = StatewideTotalDate.objects.all().order_by('scrape_date').last().__dict__

from stats.models import VacAdminTotalDate
max_vax_record = VacAdminTotalDate.objects.all().order_by('released_date').last().__dict__
```

2. Construct a new "fake" record for the off day, using the previous day's totals for the cumulative figures, and setting daily values to 0

```
fake_data = {
    'cases_daily_change': 0,
    'cases_newly_reported': 0,
    'confirmed_cases_newly_reported': 0,
    'probable_cases_newly_reported': 0,
    'removed_cases': 0,
    'deaths_daily_change': 0,
    'cumulative_positive_tests': 526267,
    'cumulative_confirmed_cases': 495420,
    'cumulative_probable_cases': 30847,
    'cumulative_completed_tests': 8338754,
    'cumulative_completed_mdh': 0,
    'cumulative_completed_private': 0,
    'cumulative_pcr_tests': 7784787,
    'cumulative_antigen_tests': 553967,
    'cumulative_hospitalized': 27553,
    'cumulative_icu': 5660,
    'currently_hospitalized': 0,
    'currently_in_icu': 0,
    'hospitalized_total_daily_change': 0,
    'icu_total_daily_change': 0,
    'cumulative_statewide_deaths': 6875,
    'cumulative_statewide_recoveries': 503024,
    'cumulative_confirmed_statewide_deaths': 6496,
    'cumulative_probable_statewide_deaths': 379,
    'update_date': '2021-04-04',
    'scrape_date': '2021-04-04',
  }
  fake_record = StatewideTotalDate(**fake_data)
  fake_record.save()

  fake_vax_data = {
    'released_date': '2021-04-04',
    'asof_date': '2021-04-01',
    'admin_total': 2826047,
    'admin_pfizer': 1439166,
    'admin_moderna': 1307837,
    'admin_jj': 79038,
    'admin_unknown': 6,
    'shipped_pfizer_total': 1555515,
    'shipped_moderna_total': 1501500,
    'shipped_jj_total': 131000,
    'shipped_pfizer_mn_providers': 1347255,
    'shipped_moderna_mn_providers': 1093600,
    'shipped_jj_mn_providers': 89600,
    'shipped_pfizer_cdc_ltc': 208260,
    'shipped_moderna_cdc_ltc': 407900,
    'shipped_jj_cdc_ltc': 41400,
    'providers': 845,
    'admin_people_total': 1770290,
    'admin_people_completed_total': 1129863
  }
  fake_vax_record = VacAdminTotalDate(**fake_vax_data)
  fake_vax_record.save()

  ```

3. Do the same for county records, which is a little less manual
```
import datetime
from datetime import timedelta
from stats.models import County, CountyTestDate, VacCountyDate

skipped_date = datetime.date(2021, 4, 4)
previous_date = skipped_date - timedelta(days=1)

for c in County.objects.all():
    previous_record = CountyTestDate.objects.get(county=c, scrape_date=previous_date)
    new_record = CountyTestDate(
      county=c,
      daily_total_cases=0,
      cumulative_count=previous_record.cumulative_count,
      cumulative_confirmed_cases=previous_record.cumulative_confirmed_cases,
      cumulative_probable_cases=previous_record.cumulative_probable_cases,
      daily_deaths=0,
      cumulative_deaths=previous_record.cumulative_deaths,
      update_date=skipped_date,
      scrape_date=skipped_date
    )
    new_record.save()

for c in VacCountyDate.objects.all().values_list('county', flat=True).distinct():
    previous_record = VacCountyDate.objects.get(county=c, released_date=previous_date)
    new_record = VacCountyDate(
      county=c,
      released_date=skipped_date,
      asof_date=previous_record.asof_date,
      people_admin_total=previous_record.people_admin_total,
      people_admin_completed=previous_record.people_admin_completed,
    )
    new_record.save()

```


## To load county fixture data (lat/lng, fips, popluation)
`
python manage.py loaddata covid_scraper/fixtures/County.json
`

## To import previously created manual data (only do this once, ask Mike first!)
`python manage.py load_existing_test_data`

## To download the latest county counts
`python manage.py update_mn_data`

## To update weekly zip data
1. On Thursdays after 11 a.m., download the weekly file from the MDH weekly report page under "Confirmed Cases by Zip Code of Residence": https://www.health.state.mn.us/diseases/coronavirus/stats/index.html

2. Change the .csv filename to a date format, based on the update date (that Thursday): covid_scraper/covid_scraper/imports/zip_cases/covid_zip_YYYYMMDD.csv

3. Update the file name to be uploaded in `load_zip_cases.py`.

4. Run the management command:
`
python manage.py load_zip_cases
`

5. The updated zipcode data will either get exported on the next scraper run (This is generally what I do), or you can force it earlier with `dump_zip_cases.py`. If you do a manual run, remember to make sure you don't leave a dirty repo in mn_covid_data, otherwise next time you build the scraper in Docker, it will fail to run.

# To fix deaths that are taken off the board

From time to time MDH finds that people who were reported dead were not, in fact, dead. This is fairly tedious to fix in the Django shell.

1. Update the `cumulative_statewide_deaths` field of `StatewideTotalDate` for all dates from the date the death was misreported through today.

Example:
```
from stats.models import StatewideTotalDate
update_days = StatewideTotalDate.objects.filter(scrape_date__gte='2020-09-11').order_by('scrape_date')
for u in update_days:
    u.cumulative_statewide_deaths -= 1
    u.save()
```

2. Update the county-level daily deaths for the dates the deaths were misreported.

Example:
```
from stats.models import CountyTestDate
update_day = CountyTestDate.objects.get(county__name="Ramsey", scrape_date='2020-08-22')
update_day.daily_deaths = 2
update_day.save()
```

3. Update the county-level cumulative deaths for all the dates since the deaths were misreported.

Example:
```
from stats.models import CountyTestDate
update_days = CountyTestDate.objects.filter(county__name="Ramsey", scrape_date__gte='2020-08-22', scrape_date__lt='2020-10-08').order_by('scrape_date')
for u in update_days:
    u.cumulative_deaths -= 1
    u.save()
```

4. Update the age stats
```
from stats.models import StatewideAgeDate
update_days = StatewideAgeDate.objects.filter(age_min=75, age_max=79, scrape_date__gte='2020-08-22', scrape_date__lt='2020-10-08').order_by('scrape_date')
for u in update_days:
    u.death_count -= 1
    u.save()
```


The timeseries daily deaths are built from the MDH table, and is re-scraped every day, so those daily figures should be fine.

## To export new CSVs
```
python manage.py dump_mn_latest_counts
python manage.py dump_mn_statewide_timeseries
```

# To build a (Dockerized) scraper:
```
docker build -t covid_scraper -f Dockerfile.scrape .
docker run --detach=false --env-file .env.prod covid_scraper
```


# Notes on pushing stuff to Github programmatically

- Add a deploy public key to the repository you want to push to, with write privileges
- Put that private key in a .gitignored folder in your scraper app
- Be sure to include all the following in your Dockerfile
```
# Install git so you can push
RUN apt-get install -y git
# Set up for github execution
RUN git config --global user.email "michael.corey@startribune.com"
RUN git config --global user.name "Scraper Bot"
RUN chmod 600 keys/.docker_deploy_rsa
# Authorize Github as SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan github.com > /root/.ssh/known_hosts
```

- In your entrypoint, you need to pull in changes to the repo BEFORE you put any new or updated files into the repo directory
```
# from presync_github_repo
repo = Repo(self.REPO_LOCAL_PATH)
git = repo.git  

ssh_cmd = 'ssh -i /srv/keys/.docker_deploy_rsa'
with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):
    git.pull('origin', 'master', '--rebase')
```
- Then do your scraping/dumping
- Finally, commit and push new changes
```
# from update_github_repo
REPO_LOCAL_PATH = os.path.join(settings.BASE_DIR, 'exports', 'mn_covid_data')
KEYS_PATH = '/srv/keys/.docker_deploy_rsa'

def handle(self, *args, **options):
    repo = Repo(self.REPO_LOCAL_PATH)
    git = repo.git

    ssh_cmd = 'ssh -i {}'.format(self.KEYS_PATH)
    with repo.git.custom_environment(GIT_SSH_COMMAND=ssh_cmd):

        if repo.is_dirty():
            print('Changes found, committing and pushing.')
            git.commit('-am', 'Updating scraper-generated files {} ...'.format(datetime.now().strftime('%Y-%m-%d %H:%M')))
            git.push('origin', 'master')
```
