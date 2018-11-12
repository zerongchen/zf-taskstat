#!/bin/bash

#Created by turk 2018-03-15
#Project  ZF STAT
#Version 4.6.1 modified by chenzr
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/..;pwd`
if [ $# -ne 1  -a  $# -ne 2 -a  $# -ne 3 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:[DATE][HOURS][MIN]"
        exit 1
fi
#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H%M -d  '-15 min')
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
fi
echo "get Date: ${DATE}, get Hour:${HOURS}, get Minutes:${MIN} "
#--------------------------------------------------

lastTimestamp=`date -d "$DATE $HOURS:${MIN}" +%s`
currTimestamp=`expr $lastTimestamp + 300`


#Radius数据采集监控详细表
detail_table=zf_v2_monitor_receivedfile_detail
#Radius数据采集监控5分钟统计表
min_table=zf_v2_monitor_receivedfile_min

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-16)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/../conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/../conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/../conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/../conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/../conf/config`


stat_sql=" use ${DB_DATABASE};
           delete from ${min_table} where stat_time=${lastTimestamp};
           INSERT INTO ${min_table} (
               stat_time,
               file_type,
               probe_type,
               area_id,
               software_provider,
               file_num,
               file_size,
               create_time
           )
           (SELECT
               ${lastTimestamp} AS stat_time,
               file_type AS file_type,
               probe_type as probe_type,
               area_id as area_id,
               software_provider as software_provider,
               COUNT(file_type) AS file_num,
               SUM(file_size) AS file_size,
               SYSDATE()
             FROM
               ${detail_table}
             WHERE filereceived_time >= ${lastTimestamp}
               AND filereceived_time < ${currTimestamp}
             GROUP BY file_type,probe_type,area_id,software_provider
           )"

echo "stat_sql=${stat_sql}"

mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS][$MIN],exit $ret !"
        exit $ret
fi

exit 0
