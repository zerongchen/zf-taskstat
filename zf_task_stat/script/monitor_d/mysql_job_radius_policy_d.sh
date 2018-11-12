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
if [ $# -ne 1 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:[DATE]"
        exit 1
fi
#--------------------------------------------------
DATE=`date -d"1 days ago" +%Y%m%d`


if [ $# -eq 1 ]
then
	DATE=$1                 #specified date
fi
echo "get Date: ${DATE} "
#--------------------------------------------------

lastTimestamp=`date -d "$DATE" +%s`
currTimestamp=`expr $lastTimestamp + 86400`


#Radius数据采集监控详细表
hour_table=zf_v2_monitor_radius_policy_h
#Radius数据采集监控天统计表
date_table=zf_v2_monitor_radius_policy_d

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
           delete from ${date_table} where stat_time=${lastTimestamp};
           INSERT INTO ${date_table} (
               stat_time,
               sendnum,
               sendsuccessnum,
               sendfailednum,
               create_time
           )
           (SELECT
               ${lastTimestamp} AS stat_time,
               SUM(sendnum) AS sendnum,
               SUM(sendsuccessnum) AS sendsuccessnum,
               SUM(sendfailednum) AS sendfailednum,
               SYSDATE()
             FROM
               ${hour_table}
             WHERE stat_time >= ${lastTimestamp}
               AND stat_time < ${currTimestamp}
           )"

echo "stat_sql=${stat_sql}"

mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

exit 0
