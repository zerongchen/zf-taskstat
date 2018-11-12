#!/bin/bash

#Created by turk 2018-06-14
#Project  ZF STAT
#Version 2.9.2 modified by chenym
#Description: web应用站点分析

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H -d  '-1 hours')
DATE=${DATEHOUR:0:8}
HOURS=${DATEHOUR:8}

if [ $# -eq 2 ]
then
	DATE=$1                 #specified date
	HOURS=$2                #specified hour
fi
echo "get Date: ${DATE}, get Hour:${HOURS}"
#--------------------------------------------------
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain

#--------------------------------------------------
key='hive.database'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
hive_database=$value
echo "hive_database:${hive_database}"
#--------------------------------------------------
#--------------------------------------------------
key='hadoop.url.hive'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url.hive:"$value

hadoop_url_hive=$value
echo "hadoop_url_hive:${hadoop_url_hive}"
#--------------------------------------------------
#Web流量分钟粒度表
from_job_ubas_webflow=job_ubas_webflow
#Web流量小时粒度表
to_job_ubas_webflow_h=job_ubas_webflow_h

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.9.2 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.9.2 update(2018-06-14)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_webflow_h}/${DATE}/${HOURS}


stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_webflow_h} partition(dt='${DATE}',hour=${HOURS})
      select '${DATE}${HOURS}' as stat_time
			    ,usergroupno as usergroupno
				,sitename as sitename
              ,sitetype as sitetype
				,sum(sitehitfreq) as sitehitfreq
				,sum(sitetraffic_up) as sitetraffic_up
				,sum(sitetraffic_dn) as sitetraffic_dn
				,packetsubtype
              ,probe_type
				,area_id
            from ${from_job_ubas_webflow} where dt='${DATE}' and hour=${HOURS} and length(usergroupno)>0
			 group by usergroupno,sitename,sitetype,packetsubtype,probe_type,area_id
			"

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_webflow_h} add if not exists partition(dt='${DATE}',hour='${HOURS}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_webflow_h_${DATE}_${HOURS};
		  set hive.execution.engine=spark;
         ${stat_sql}"
beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS],exit $ret !" 
        exit $ret
fi

key='hadoop.url'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url:"$value
IMPALA_SERVER=`echo ${value}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`

echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_webflow_h} "
else
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_webflow_h} " -i ${IMPALA_SERVER}
fi

exit 0
