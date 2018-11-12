#!/bin/bash

##-----------------------------------------------
DATE=`date -d"1 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	DATE=$1
fi
#echo "get Date: ${DATE}"
##-----------------------------------------------

echo "{\"DATE\":\"${DATE}\"}" | tee -a $JOB_OUTPUT_PROP_FILE

exit 0
