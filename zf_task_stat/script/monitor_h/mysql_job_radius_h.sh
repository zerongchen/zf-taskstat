#!/bin/bash

#Created by turk 2018-03-27
#Project  ZF STAT
#Version 4.6.1 modified by chenzr
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/..;pwd`
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

lastTimestamp=`date -d "$DATE $HOURS" +%s`
currTimestamp=`expr $lastTimestamp + 3600`


#Radius数据采集监控小时统计表
hour_table=zf_v2_monitor_radius_pcap_h
#Radius数据采集监控5分钟统计表
min_table=zf_v2_monitor_radius_pcap_min

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-27)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/../conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/../conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/../conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/../conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/../conf/config`

stat_sql=" use ${DB_DATABASE};
           delete from ${hour_table} where stat_time=${lastTimestamp};
           INSERT INTO ${hour_table} (
               stat_time,
               capturepacketnum,
               validpacketnum,
               invalidpacketnum,
               sendnum,
               sendsuccessnum,
               sendfailednum,
               create_time
           )
           (SELECT
               ${lastTimestamp} AS stat_time,
               SUM(capturepacketnum) AS capturepacketnum,
               SUM(validpacketnum) AS validpacketnum,
               SUM(invalidpacketnum) AS invalidpacketnum,
               SUM(sendnum) as sendnum,
               SUM(sendsuccessnum) as sendsuccessnum,
               SUM(sendfailednum) as sendfailednum,
               SYSDATE()
             FROM
               ${min_table}
             WHERE stat_time >= ${lastTimestamp}
               AND stat_time < ${currTimestamp}
           )"

echo "stat_sql=${stat_sql}"


mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS],exit $ret !"
        exit $ret
fi

exit 0
