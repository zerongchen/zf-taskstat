#!/bin/bash

##-----------------------------------------------
DATE=`date -d"7 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	DATE=$1
fi
##-----------------------------------------------

echo "{\"DATE\":\"${DATE}\"}" | tee -a $JOB_OUTPUT_PROP_FILE

exit 0
