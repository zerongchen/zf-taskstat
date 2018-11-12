#!/bin/bash

#Created by turk 2018-04-08
#Project  ZF STAT
#Version 4.6.1 modified by chenzr
#Description:

SOURCE="${BASH_SOURCE[0]}"
BIN_DIR="$( dirname "$SOURCE" )"
while [ -h "$SOURCE" ]
  do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
  done
  BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

#work home directory
WORKPATH=`cd $BIN_DIR/../../;pwd`

if [ $# -ne 1  -a  $# -ne 2 -a ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:[DATE][HOURS]"
        exit 1
fi
#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H -d  '-1 hours')
DATE=${DATEHOUR:0:8}
HOURS=${DATEHOUR:8}


if [ $# -eq 2 ]
then
	DATE=$1                 #specified date
	HOURS=$2                #specified hour
fi
echo "get Date: ${DATE}, get Hour:${HOURS} "
#--------------------------------------------------

timestamp=`date -d "$DATE $HOURS" +%s`

previous=`expr ${timestamp} - 3600`
next=`expr ${timestamp} + 3600`

PREVIOUS_DATE=`date -d @$previous "+%Y%m%d"`
PREVIOUS_HOUR=`date -d @$previous "+%H"`
FUTURE_DATE=`date -d @$next "+%Y%m%d"`
FUTURE_HOUR=`date -d @$next "+%H"`

from_job_table=job_ubas_userapp
to_job_socket_save=job_ud_monitor_userapp_savedata_h
#访问指定应用的用户统计
PACKETTYPE=1
PACKETSUBTYPE=3

OUTPUTPATH=/user/hive/warehouse/zf.db/tmps/${to_job_socket_save}/


echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-04-03)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
DATABASE_SCHEMA=`java ${CLASS_PATH} com.aotain.statmange.config.ReadParamMain 'hive.database' | tail -1`
echo "hive.database:" $DATABASE_SCHEMA

stat_sql=" ROW FORMAT delimited fields terminated by '\|'
            select
                ${timestamp} as stat_time,
                ${PACKETTYPE} as packettype,
                ${PACKETSUBTYPE} as packetsubtype,
                count(1) as savenum,
                probe_type,
                area_id,
                software_provider,
                from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as createtime
                from zf.${from_job_table}
                where ((dt='${DATE}' and hour=${HOURS}) or (dt='${PREVIOUS_DATE}' and hour=${PREVIOUS_HOUR}) or (dt='${FUTURE_DATE}' and hour=${FUTURE_HOUR}))
                                      and receivedtime>=${timestamp} and receivedtime<${next}
                group by probe_type,area_id,software_provider
                "

echo "stat_sql=${stat_sql}"

hadoop fs -rm -r ${OUTPUTPATH}

hadoop fs -test -e ${OUTPUTPATH}

if [ $? -eq 0 ] ;then
    echo ''
else
    hadoop fs -mkdir -p ${OUTPUTPATH}
fi

hive -e " use ${DATABASE_SCHEMA};
        set mapred.job.name=job_userapp_socket_save_${DATE}_${HOURS};
		INSERT overwrite directory '${OUTPUTPATH}' ${stat_sql};"

sh ${WORKPATH}/script/monitor_h/mysql_job_socket_save_end_h.sh ${OUTPUTPATH} ${timestamp} ${PACKETTYPE} ${PACKETSUBTYPE}

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS],exit $ret !"
        exit $ret
fi

exit 0