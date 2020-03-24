#!/bin/bash
python manage.py migrate
echo Starting scrape.
python manage.py update_mn_county_counts
python manage.py dump_mn_latest_counts
python manage.py dump_mn_statewide_timeseries
