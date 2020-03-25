# covid_scraper

## To load county fixture data (lat/lng, fips, popluation)
```
python manage.py loaddata covid_scraper/fixtures/County.json
```

## To import previously created manual data (only do this once, ask Mike first!)
```
python manage.py load_existing_test_data
```

## To download the latest county counts
```
python manage.py update_mn_county_counts
```

## To export new CSVs
```
python manage.py dump_mn_latest_counts
python manage.py dump_mn_statewide_timeseries
```

# To build a (Dockerized) scraper:
docker build -t covid_scraper -f Dockerfile.scrape .
docker run --detach=false --env-file .env.prod covid_scraper
