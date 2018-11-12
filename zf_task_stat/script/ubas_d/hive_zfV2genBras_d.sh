#!/bin/bash

#Created by turk 2018-05-21
#Project  ZF STAT
#Version 4.9.1 modified by chenym
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
#--------------------------------------------------
DATE=`date -d"1 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	DATE=$1
fi
echo "get Date: ${DATE}"
#--------------------------------------------------

CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain

#--------------------------------------------------
key='hive.database'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hive.database:"$value

hive_database=$value
echo "hive_database:${hive_database}"
job_radius_log=job_radius_log
job_radius_bras_d=job_radius_bras_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.9.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.9.1 update(2018-05-21)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/tmp/job_radius_log_tmp/${DATE}
OUTPUTPATH1=/user/hive/warehouse/${hive_database}.db/${job_radius_bras_d}/${DATE}
BASE=/user/hive/warehouse/${hive_database}.db/${job_radius_bras_d}/19700101

stat_sql1="INSERT INTO TABLE ${job_radius_bras_d} partition(dt='${DATE}')
select nas_ip_address,nas_identifier,event_timestamp,event_timestamp from job_radius_log where dt='${DATE}' and length(nas_ip_address) !=0 and length(event_timestamp)!=0"

stat_sql2="insert overwrite directory '${OUTPUTPATH}' row format delimited fields terminated by '|'
 select bras_ip,collect_set(bras_name)[0],from_unixtime(min(first_time),'yyyy-MM-dd hh:mm:ss'),from_unixtime(max(last_time),'yyyy-MM-dd hh:mm:ss') from ${job_radius_bras_d} where dt in('${DATE}','19700101')
 and length(first_time)!=0 and length(last_time)!=0
 group by bras_ip"

echo "stat_sql1=${stat_sql1}"
echo "stat_sql2=${stat_sql2}"

hive -e   "use ${hive_database};
         alter table ${job_radius_bras_d} add if not exists partition(dt='19700101') location '${BASE}';
         alter table ${job_radius_bras_d} add if not exists partition(dt='${DATE}') location '${OUTPUTPATH1}';
		  set mapred.job.name=to_${job_radius_bras_d}_${DATE};
         ${stat_sql1};${stat_sql2}"
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!"

hadoop fs -rm -r ${BASE}/*
hadoop fs -mv ${OUTPUTPATH}/* ${BASE}/
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "mv data to base failed [$DATE],exit $ret !"
        exit $ret
fi
echo `date +"%Y-%m-%d %H:%M:%S"`      "mv data to base Success [$DATE]!"

exit 0
