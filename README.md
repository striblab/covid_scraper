# covid_scraper

## Monitoring the scraper day to day
The daily scrape is run by the `docker-entrypoint-scrape.sh` shell script, so you can see what order things happen in from there.

The most common cause of the scrape failing is by the [MDH page](https://www.health.state.mn.us/diseases/coronavirus/situation.html) changing something. That logic is all handled in `update_mn_county_counts.py`


## To load county fixture data (lat/lng, fips, popluation)
`
python manage.py loaddata covid_scraper/fixtures/County.json
`

## To import previously created manual data (only do this once, ask Mike first!)
`python manage.py load_existing_test_data`

## To download the latest county counts
`python manage.py update_mn_county_counts`

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
