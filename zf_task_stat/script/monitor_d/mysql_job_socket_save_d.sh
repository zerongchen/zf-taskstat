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
hour_table=zf_v2_monitor_savedata_h
#Radius数据采集监控天统计表
date_table=zf_v2_monitor_savedata_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-04-08)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`


stat_sql=" use ${DB_DATABASE};
           delete from ${date_table} where stat_time=${lastTimestamp};
           INSERT INTO ${date_table} (
               stat_time,
               packettype,
               packetsubtype,
               savenum,
               probe_type,
               area_id,
               software_provider,
               create_time
           )
           (SELECT
               ${lastTimestamp} AS stat_time,
               packettype AS packettype,
               packetsubtype as packetsubtype,
               SUM(savenum) as savenum,
               probe_type AS probe_type,
               area_id AS area_id,
               software_provider as software_provider,
               SYSDATE()
             FROM
               ${hour_table}
             WHERE stat_time >= ${lastTimestamp}
               AND stat_time < ${currTimestamp}
              group by packettype,packetsubtype,probe_type,area_id,software_provider
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
