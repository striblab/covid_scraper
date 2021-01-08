#!/bin/bash
EXPORTS_ROOT=covid_scraper/exports
COUNTY_LATEST_FILENAME=mn_counties_latest
STATEWIDE_LATEST_FILENAME=mn_statewide_latest
STATEWIDE_TIMESERIES_FILENAME=mn_statewide_timeseries
COUNTY_TIMESERIES_TALL_FILENAME=mn_county_timeseries_tall
COUNTY_TIMESERIES_ALL_FILENAME=mn_county_timeseries_all_counties

TZ=America/Chicago date

echo Starting scrape...

echo "Pushing copy of MDH situation html..."
cache_datetime=$(TZ=America/Chicago date '+%Y-%m-%d_%H%M');
curl -s --compressed https://www.health.state.mn.us/diseases/coronavirus/situation.html > $EXPORTS_ROOT/html/situation_$cache_datetime.html
aws s3 cp $EXPORTS_ROOT/html/situation_$cache_datetime.html s3://$S3_URL/raw/html/situation_$cache_datetime.html \
--content-type=text/html \
--acl public-read

# echo "Pushing copy of MDH vaccine distribution html..."
# curl -s --compressed https://www.health.state.mn.us/diseases/coronavirus/vaccine/stats/distrib.html > $EXPORTS_ROOT/html/distrib_$cache_datetime.html
# aws s3 cp $EXPORTS_ROOT/html/distrib_$cache_datetime.html s3://$S3_URL/html/distrib_$cache_datetime.html \
# --content-type=text/html \
# --acl public-read
#
# echo "Pushing copy of MDH vaccine administration html..."
# curl -s --compressed https://www.health.state.mn.us/diseases/coronavirus/vaccine/stats/admin.html > $EXPORTS_ROOT/html/admin_$cache_datetime.html
# aws s3 cp $EXPORTS_ROOT/html/admin_$cache_datetime.html s3://$S3_URL/html/admin_$cache_datetime.html \
# --content-type=text/html \
# --acl public-read

echo "Pushing copy of CDC vaccine data json..."
curl -s --compressed https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data > $EXPORTS_ROOT/html/cdc_vac_$cache_datetime.json
aws s3 cp $EXPORTS_ROOT/html/cdc_vac_$cache_datetime.json s3://$S3_URL/raw/html/cdc_vac_$cache_datetime.json \
--content-type=text/html \
--acl public-read



python manage.py update_mn_data
ret=$?
if [ $ret -ne 0 ]; then
     echo "Somthing went wrong."
     exit
fi

echo Updating latest county counts...
python manage.py update_mn_county_data

# echo "Presyncing with Github..."
# python manage.py presync_github_repo

echo Dumping latest county counts...
python manage.py dump_mn_latest_counts

echo Dumping statewide timeseries...
python manage.py dump_mn_statewide_timeseries

echo Dumping county timeseries...
python manage.py dump_mn_county_timeseries

echo Updating age data...
python manage.py update_mn_age_data

echo Updating recent deaths ...
python manage.py update_mn_recent_deaths

# echo "Updating Github..."
# python manage.py update_github_repo

LINE_COUNT=($(wc -l $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 1)); then
  echo "***** Uploading latest statewide and county count CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  aws s3 cp $EXPORTS_ROOT/$COUNTY_LATEST_FILENAME.json s3://$S3_URL/latest/json/$COUNTY_LATEST_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv s3://$S3_URL/latest/csv/$STATEWIDE_LATEST_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv s3://$S3_URL/github/$STATEWIDE_LATEST_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv s3://$S3_URL/versions/csv/$STATEWIDE_LATEST_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest 'latest' file is very short. Taking no further action. *****"
fi
printf "\n\n"

# Only dump if csvs have many lines or were produced in last few minutes
LINE_COUNT=($(wc -l $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading timeseries CSVs to S3. *****"

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/latest/csv/$STATEWIDE_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/github/$STATEWIDE_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$STATEWIDE_TIMESERIES_FILENAME.json s3://$S3_URL/latest/json/$STATEWIDE_TIMESERIES_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_TALL_FILENAME.csv s3://$S3_URL/latest/csv/$COUNTY_TIMESERIES_TALL_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_TALL_FILENAME.csv s3://$S3_URL/github/$COUNTY_TIMESERIES_TALL_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_ALL_FILENAME.csv s3://$S3_URL/github/$COUNTY_TIMESERIES_ALL_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/versions/csv/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$STATEWIDE_TIMESERIES_FILENAME.json s3://$S3_URL/versions/json/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_TALL_FILENAME.csv s3://$S3_URL/versions/csv/$COUNTY_TIMESERIES_TALL_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_county_timeseries.json s3://$S3_URL/latest/json/mn_county_timeseries.json \
  --content-type=application/json \
  --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest file is very short. Taking no further action. *****"
fi
printf "\n"

# Dashboard
python manage.py update_dashboard_data

for DASHPATH in db_hosp_cap_* db_procurement_* db_days_on_hand_chart_* db_days_on_hand_tbl_* db_crit_care_supply_sources_* db_dialback_*
do

  LATEST_SUPPLIES_SCRAPE=($(find $EXPORTS_ROOT/dashboard -name $DASHPATH | sort | tail -n 1))
  echo $LATEST_SUPPLIES_SCRAPE
  echo "Pushing copy of dashboard csv... $LATEST_SUPPLIES_SCRAPE"
  aws s3 cp $LATEST_SUPPLIES_SCRAPE s3://$S3_URL/dashboard/${LATEST_SUPPLIES_SCRAPE##*/} \
  --content-type=text/csv \
  --acl public-read
done
