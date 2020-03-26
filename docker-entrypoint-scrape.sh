#!/bin/bash
EXPORTS_ROOT=covid_scraper/exports/
COUNTY_TESTS_FILENAME=mn_positive_tests_by_county
STATEWIDE_TIMESERIES_FILENAME=mn_statewide_timeseries
COUNTY_TIMESERIES_FILENAME=mn_county_timeseries

python manage.py migrate
echo Starting scrape...
python manage.py update_mn_county_counts
echo Dumping latest county counts...
python manage.py dump_mn_latest_counts

# Only dump if csvs have many lines or were produced in last few minutes
LINE_COUNT=($(wc -l $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv))

if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading latest county count CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  echo Pushing csvs to S3...
  aws s3 cp $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TESTS_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TESTS_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest file is very short. Taking no further action. *****"
fi
printf "\n\n"

echo Dumping statewide timeseries...
python manage.py dump_mn_statewide_timeseries
echo Dumping county timeseries...
python manage.py dump_mn_county_timeseries

if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading timeseries CSVs to S3. *****"

  aws s3 cp $EXPORTS_ROOT$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$COUNTY_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$COUNTY_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TIMESERIES_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

fi
printf "\n"
