#!/bin/bash

#Created by turk 2018-06-20
#Project  ZF STAT
#Version 2.10.1 modified by chenzr
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/..;pwd`
#--------------------------------------------------
DATE=`date -d"1 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	DATE=$1
fi
echo "get Date: ${DATE}"
#--------------------------------------------------
key='hive.database'
CLASS_PATH=" -classpath "$(echo $WORKPATH/../lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
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

impalanodekey='hadoop.url';
impalanode=`java ${CLASS_PATH} ${MAIN_CLASS} ${impalanodekey} | tail -1`
IMPALA_SERVER=`echo ${impalanode}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`

#DDOS 详情表
from_job_ubas_ddos=job_ubas_ddos
#DDOS 天表
to_job_ubas_ddos_d=job_ubas_ddos_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version  2.10.1 update(2018-06-20)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_ddos_d}/${DATE}

stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_ddos_d} partition(dt='${DATE}')
       select '${DATE}' as stat_time
			    ,pusergrougno
			    ,appattacktype
			    ,SUM(appattacktraffic) as appattacktraffic
			    ,AVG(appattackrate) as appattackrate
			    ,probe_type
			    ,area_id
             from ${from_job_ubas_ddos} where dt='${DATE}'
			 group by pusergrougno,appattacktype,probe_type,area_id

	   "

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_ddos_d} add if not exists partition(dt='${DATE}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_ddos_d_${DATE};
		  set hive.execution.engine=spark;
         ${stat_sql}"
beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

DETAIL_TABLE=job_ubas_ddos_area
echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${from_job_ubas_ddos} "
	impala-shell -q "refresh  ${hive_database}.${DETAIL_TABLE} "
else
	impala-shell -q "refresh  ${hive_database}.${from_job_ubas_ddos} " -i ${IMPALA_SERVER}
	impala-shell -q "refresh  ${hive_database}.${DETAIL_TABLE} " -i ${IMPALA_SERVER}
fi

exit 0
