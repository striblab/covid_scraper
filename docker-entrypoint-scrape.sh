#!/bin/bash
EXPORTS_ROOT=covid_scraper/exports
COUNTY_TESTS_FILENAME=mn_positive_tests_by_county
COUNTY_LATEST_FILENAME=mn_counties_latest
STATEWIDE_LATEST_FILENAME=mn_statewide_latest
AGES_LATEST_FILENAME=mn_ages_latest
# DEATH_AGES_LATEST_FILENAME=mn_death_ages_detailed_latest
STATEWIDE_TIMESERIES_FILENAME=mn_statewide_timeseries
# COUNTY_TIMESERIES_FILENAME=mn_county_timeseries
COUNTY_TIMESERIES_TALL_FILENAME=mn_county_timeseries_tall
NATIONAL_TIMESERIES_FILENAME=national_cases_deaths_by_county_timeseries
NATIONAL_LATEST_FILENAME=national_cases_deaths_by_county_latest
# MIDWEST_EMERGING_COUNTIES_PATH=midwest_emerging_counties
# MIDWEST_EMERGING_COUNTIES_WIDE_PATH=midwest_emerging_counties_wide
US_LATEST_EXPORT_PATH_NYT=us_latest_nyt
US_LATEST_EXPORT_PATH_CTP=us_latest_ctp
# STATES_LATEST_EXPORT_PATH=states_latest_ctp
GLOBAL_LATEST_EXPORT_PATH=global_latest_ghu

TZ=America/Chicago date

echo "Presyncing with Github..."
python manage.py presync_github_repo

echo Starting scrape...

echo "Pushing copy of MDH html..."
cache_datetime=$(TZ=America/Chicago date '+%Y-%m-%d_%H%M');
curl -s --compressed https://www.health.state.mn.us/diseases/coronavirus/situation.html > $EXPORTS_ROOT/html/situation_$cache_datetime.html
aws s3 cp $EXPORTS_ROOT/html/situation_$cache_datetime.html s3://$S3_URL/html/situation_$cache_datetime.html \
--content-type=text/html \
--acl public-read

python manage.py update_mn_data
ret=$?
if [ $ret -ne 0 ]; then
     echo "Somthing went wrong."
     exit
fi

echo Dumping latest county counts...
python manage.py dump_mn_latest_counts
LINE_COUNT=($(wc -l $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 1)); then
  echo "***** Uploading latest statewide and county count CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  aws s3 cp $EXPORTS_ROOT/$COUNTY_LATEST_FILENAME.json s3://$S3_URL/json/$COUNTY_LATEST_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_LATEST_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_LATEST_FILENAME.csv s3://$S3_URL/csv/versions/$STATEWIDE_LATEST_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest 'latest' file is very short. Taking no further action. *****"
fi
printf "\n\n"

echo Dumping statewide timeseries...
python manage.py dump_mn_statewide_timeseries
echo Dumping county timeseries...
python manage.py dump_mn_county_timeseries

# Only dump if csvs have many lines or were produced in last few minutes
LINE_COUNT=($(wc -l $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading timeseries CSVs to S3. *****"

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$STATEWIDE_TIMESERIES_FILENAME.json s3://$S3_URL/json/$STATEWIDE_TIMESERIES_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_TALL_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TIMESERIES_TALL_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  # aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TIMESERIES_FILENAME.csv \
  # --content-type=text/csv \
  # --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/versions/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$STATEWIDE_TIMESERIES_FILENAME.json s3://$S3_URL/json/versions/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_TALL_FILENAME.csv s3://$S3_URL/csv/versions/$COUNTY_TIMESERIES_TALL_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_county_timeseries.json s3://$S3_URL/json/mn_county_timeseries.json \
  --content-type=application/json \
  --acl public-read
  #
  # aws s3 cp $EXPORTS_ROOT/mn_covid_data/$COUNTY_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/versions/$COUNTY_TIMESERIES_FILENAME-$download_datetime.csv \
  # --content-type=text/csv \
  # --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest file is very short. Taking no further action. *****"
fi
printf "\n"

# Only dump if csvs have many lines or were produced in last few minutes
python manage.py dump_mn_age_data
LINE_COUNT=($(wc -l $EXPORTS_ROOT/mn_covid_data/$AGES_LATEST_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading latest age CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$AGES_LATEST_FILENAME.csv s3://$S3_URL/csv/$AGES_LATEST_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$AGES_LATEST_FILENAME.json s3://$S3_URL/json/$AGES_LATEST_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/mn_covid_data/$AGES_LATEST_FILENAME.csv s3://$S3_URL/csv/versions/$AGES_LATEST_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read
else
  echo "***** WARNING WARNING WARNING: The newest age file is very short. Taking no further action. *****"
fi
printf "\n\n"

echo "Updating zip code data ..."
python manage.py dump_zip_cases

if (("${LINE_COUNT[0]}" > 2)); then
  echo "Updating Github..."
  python manage.py update_github_repo
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

# National death toll and cases
python manage.py get_us_latest
aws s3 cp $EXPORTS_ROOT/$US_LATEST_EXPORT_PATH_NYT.csv s3://$S3_URL/csv/$US_LATEST_EXPORT_PATH_NYT.csv \
--content-type=text/csv \
--acl public-read

aws s3 cp $EXPORTS_ROOT/$US_LATEST_EXPORT_PATH_CTP.csv s3://$S3_URL/csv/$US_LATEST_EXPORT_PATH_CTP.csv \
--content-type=text/csv \
--acl public-read

# python manage.py get_ctp_states
# aws s3 cp $EXPORTS_ROOT/$STATES_LATEST_EXPORT_PATH.json s3://$S3_URL/json/$STATES_LATEST_EXPORT_PATH.json \
# --content-type=application/json \
# --acl public-read

# Global death toll and cases
python manage.py get_jhu_global
aws s3 cp $EXPORTS_ROOT/$GLOBAL_LATEST_EXPORT_PATH.json s3://$S3_URL/json/$GLOBAL_LATEST_EXPORT_PATH.json \
--content-type=application/json \
--acl public-read

############### NATIONAL FROM NYT ###############
echo Updating NYT national numbers...
python manage.py join_us_county_data

# Only dump if csvs have many lines or were produced in last few minutes
LINE_COUNT=($(wc -l $EXPORTS_ROOT/$NATIONAL_LATEST_FILENAME.csv))
if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading latest national count CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  aws s3 cp $EXPORTS_ROOT/$NATIONAL_LATEST_FILENAME.csv s3://$S3_URL/csv/$NATIONAL_LATEST_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$NATIONAL_LATEST_FILENAME.json s3://$S3_URL/json/$NATIONAL_LATEST_FILENAME.json \
  --content-type=application/json \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT/$NATIONAL_LATEST_FILENAME.csv s3://$S3_URL/csv/versions/$NATIONAL_LATEST_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  # aws s3 cp $EXPORTS_ROOT/$NATIONAL_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$NATIONAL_TIMESERIES_FILENAME.csv \
  # --content-type=text/csv \
  # --acl public-read

  # aws s3 cp $EXPORTS_ROOT/$NATIONAL_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/versions/$NATIONAL_TIMESERIES_FILENAME-$download_datetime.csv \
  # --content-type=text/csv \
  # --acl public-read

  # aws s3 cp $EXPORTS_ROOT/$MIDWEST_EMERGING_COUNTIES_PATH.csv s3://$S3_URL/csv/$MIDWEST_EMERGING_COUNTIES_PATH.csv \
  # --content-type=text/csv \
  # --acl public-read
  #
  # aws s3 cp $EXPORTS_ROOT/$MIDWEST_EMERGING_COUNTIES_PATH.csv s3://$S3_URL/csv/versions/$MIDWEST_EMERGING_COUNTIES_PATH-$download_datetime.csv \
  # --content-type=text/csv \
  # --acl public-read
  #
  # aws s3 cp $EXPORTS_ROOT/$MIDWEST_EMERGING_COUNTIES_WIDE_PATH.csv s3://$S3_URL/csv/$MIDWEST_EMERGING_COUNTIES_WIDE_PATH.csv \
  # --content-type=text/csv \
  # --acl public-read
  #
  # aws s3 cp $EXPORTS_ROOT/$MIDWEST_EMERGING_COUNTIES_WIDE_PATH.csv s3://$S3_URL/csv/versions/$MIDWEST_EMERGING_COUNTIES_WIDE_PATH-$download_datetime.csv \
  # --content-type=text/csv \
  # --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest file is very short. Taking no further action. *****"
fi
printf "\n\n"

############### TIMESERIES FROM MN ###############
