#!/bin/bash

#Created by turk 2018-06-14
#Project  ZF STAT
#Version 2.9.2 modified by chenym
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
#--------------------------------------------------
key='hadoop.url.hive'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url.hive:"$value

hadoop_url_hive=$value
echo "hadoop_url_hive:${hadoop_url_hive}"
#--------------------------------------------------
# wlan分钟粒度表
from_job_ubas_wlan=job_ubas_wlan
# wlan小时粒度表
to_job_ubas_wlan_d=job_ubas_wlan_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.9.2 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.9.2 update(2018-06-14)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_wlan_d}/${DATE}

stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_wlan_d} partition(dt='${DATE}')
       select '${DATE}' as stat_time
               ,useraccount
               ,devicelist
               ,size(devicelist) as devicecnt
               ,probe_type
               ,area_id
       from(select
			    useraccount
				,collect_set(concat(devicetype,':',phonenum)) as devicelist
              ,probe_type
				,area_id
            from ${from_job_ubas_wlan} where dt='${DATE}'
			 group by useraccount,probe_type,area_id) h
	   "

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_wlan_d} add if not exists partition(dt='${DATE}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_wlan_d_${DATE};
		  set hive.execution.engine=spark;
         ${stat_sql}"
beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

key='hadoop.url'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url:"$value
IMPALA_SERVER=`echo ${value}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`

echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_wlan_d} "
else
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_wlan_d} " -i ${IMPALA_SERVER}
fi

exit 0
