#!/bin/bash

#-----------------------------------------------
DATE=`date -d last-month +%Y%m`01

if [ $# -eq 1 ]
then
	DATE=$1
fi
#echo "get Date: ${DATE}"
##-----------------------------------------------

echo "{\"DATE\":\"${DATE}\"}" | tee -a $JOB_OUTPUT_PROP_FILE

exit 0
