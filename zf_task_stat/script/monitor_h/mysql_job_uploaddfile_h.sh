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


#Radius数据采集监控详细表
hour_table=zf_v2_monitor_uploaddfile_h
#Radius数据采集监控5分钟统计表
detail_table=zf_v2_monitor_uploaddfile_detail
detail_warn_table=zf_v2_monitor_uploaddfile_warn_detail

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
               file_type,
               probe_type,
               area_id,
               software_provider,
               file_num,
               file_size,
               warn_file_num,
               create_time
           )
           (SELECT
              ${lastTimestamp} AS stat_time,
              u.file_type,
              IFNULL(d.probe_type,0),
              IFNULL(d.area_id,0),
              IFNULL(software_provider,0),
              IFNULL(d.file_num,0),
              IFNULL(d.file_size,0),
              IFNULL(d.warn_file_num,0),
              SYSDATE()
              from (SELECT
                       258 AS file_type
                     UNION
                     ALL
                     SELECT
                       1023 AS file_type
                     UNION
                     ALL
                     SELECT
                       768 AS file_type
                     UNION
                     ALL
                     SELECT
                       452 AS file_type) u
                     LEFT JOIN
                   (SELECT
                       t.file_type AS file_type,
                       t.probe_type as probe_type,
                       t.area_id as area_id,
                       t.software_provider as software_provider,
                       COUNT(t.file_type) AS file_num,
                       SUM(t.file_size) AS file_size,
                       SUM(CASE WHEN w.file_name IS NOT NULL THEN 1 ELSE 0 END) AS warn_file_num
                     FROM
                       ${detail_table} t left join ${detail_warn_table} w ON  t.file_name=w.file_name AND t.fileupload_time=w.fileupload_time
                     WHERE t.fileupload_time >= ${lastTimestamp}
                       AND t.fileupload_time < ${currTimestamp}
                     GROUP BY t.file_type,t.probe_type,t.area_id,t.software_provider) d ON d.file_type = u.file_type
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
