#!/bin/bash
EXPORTS_ROOT=covid_scraper/exports/
COUNTY_TESTS_FILENAME=mn_positive_tests_by_county
STATEWIDE_TIMESERIES_FILENAME=mn_statewide_timeseries

python manage.py migrate
echo Starting scrape...
python manage.py update_mn_county_counts
echo Dumping latest county counts...
python manage.py dump_mn_latest_counts
echo Dumping statewide timeseries...
python manage.py dump_mn_statewide_timeseries

# TODO: Only dump if csvs have many lines or were produced in last few minutes
LINE_COUNT=($(wc -l $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv))

if (("${LINE_COUNT[0]}" > 2)); then
  echo "***** Uploading CSVs to S3. *****"
  download_datetime=$(date '+%Y%m%d%H%M%S');

  echo Pushing csvs to S3...
  aws s3 cp $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TESTS_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_TIMESERIES_FILENAME-$download_datetime.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$COUNTY_TESTS_FILENAME.csv s3://$S3_URL/csv/$COUNTY_TESTS_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

  aws s3 cp $EXPORTS_ROOT$STATEWIDE_TIMESERIES_FILENAME.csv s3://$S3_URL/csv/$STATEWIDE_TIMESERIES_FILENAME.csv \
  --content-type=text/csv \
  --acl public-read

else
  echo "***** WARNING WARNING WARNING: The newest file is very short. Taking no further action. *****"
fi
printf "\n\n"



# # Test that this is a seemingly valid file
# FIRST_LEVEL="$(cat $TMPFILE | jq '[.[]][0].level')"
# if [ $FIRST_LEVEL == '"state"' ]; then
#   echo "Seems to be JSON in expected elex format. Checking for changes from last version."
#
#   if cmp --silent $TMPFILE $LATEST_FILE; then
#      echo "File unchanged. No upload will be attempted."
#   else
#      echo "Changes found. Updating latest file..."
#      cp $TMPFILE $LATEST_FILE
#
#      # Push "latest" to s3
#      gzip -vc $LATEST_FILE | aws s3 cp - s3://$ELEX_S3_URL/$LATEST_FILE \
#      --profile $AWS_PROFILE_NAME \
#      --acl public-read \
#      --content-type=application/json \
#      --content-encoding gzip
#
#      # Push timestamped to s3
#      gzip -vc $TMPFILE | aws s3 cp - "s3://$ELEX_S3_URL/json/results-$download_datetime.json" \
#      --profile $AWS_PROFILE_NAME \
#      --acl public-read \
#      --content-type=application/json \
#      --content-encoding gzip
#
#      # Make local timestamped file for new changed version
#      cp $TMPFILE "json/results-$download_datetime.json"
#   fi
#
#    # Check response headers
#    RESPONSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" $ELEX_S3_URL/$LATEST_FILE)
#    if [ $RESPONSE_CODE == '200' ]; then
#      echo "Successfully test-retrieved 'latest' file from S3."
#    else
#      echo "***** WARNING WARNING WARNING: No 'latest' file could be retrieved from S3. Response code $RESPONSE_CODE *****"
#    fi
#
#    # curl -I $ELEX_S3_URL/$LATEST_FILE
#
#    # Get first entry of uploaded json
#    FIRST_ENTRY=$(curl -s --compressed $ELEX_S3_URL/$LATEST_FILE | jq '[.[]][0]')
#    if [ "$(echo $FIRST_ENTRY | jq '.level')" == '"state"' ]; then
#      echo "$FIRST_ENTRY"
#    else
#      echo "***** WARNING WARNING WARNING: Test-retrieved 'latest' file does not seem to be parseable JSON in expected format. *****"
#    fi
# else
#   echo "***** WARNING WARNING WARNING: The newest file doesn't seem to be what we'd expect from elex JSON. Taking no further action. *****"
# fi
# printf "\n\n"
