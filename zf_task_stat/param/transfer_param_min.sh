#!/bin/bash

#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H%M -d  '-10 min')
DATE=${DATEHOUR:0:8}
HOURS=${DATEHOUR:8:2}
MINS=${DATEHOUR:10}
YUSHU=$((${MINS}%5))
MIN=$[${MINS}-${YUSHU}]

if [ $# -eq 3 ]
then
	DATE=$1                 #specified date
	HOURS=$2                #specified hour
	MIN=$3                  #specified min

    echo "input Date: ${DATE},input Hour:${HOURS},input Min:${MIN}"
fi
#echo "get Date: ${DATE},get Hour:${HOURS},get Min:${MIN}"
#--------------------------------------------------

echo "{\"DATE\":\"${DATE}\",\"HOURS\":\"${HOURS}\",\"MIN\":\"${MIN}\"}" | tee -a $JOB_OUTPUT_PROP_FILE

exit 0
