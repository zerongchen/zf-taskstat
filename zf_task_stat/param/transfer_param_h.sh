#!/bin/bash

#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H -d  '-1 hours')
DATE=${DATEHOUR:0:8}
HOURS=${DATEHOUR:8}

if [ $# -eq 2 ]
then
	DATE=$1                 #specified date
	HOURS=$2                #specified hour
fi
#echo "get Date: ${DATE}, get Hour:${HOURS}"
#--------------------------------------------------

echo "{\"DATE\":\"${DATE}\",\"HOURS\":\"${HOURS}\"}" | tee -a $JOB_OUTPUT_PROP_FILE

exit 0
