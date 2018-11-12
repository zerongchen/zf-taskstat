#!/bin/bash

#Created by turk 2018-03-15
#Project  ZF STAT
#Version 4.6.1 modified by chenym
#Description:

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
echo "hive.database:"$value

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

#通用流量分钟粒度表
from_job_ubas_appflow=job_ubas_appflow
#通用流量小时粒度表
to_job_ubas_appflow_h=job_ubas_appflow_h

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-16)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_appflow_h}/${DATE}/${HOURS}


stat_sql=" INSERT overwrite TABLE ${to_job_ubas_appflow_h} partition(dt='${DATE}',hour=${HOURS})
      select '${DATE}${HOURS}' as stat_time
			    ,usergroupno as usergroupno
				,apptype as apptype
              ,appid as appid
				,appname as appname
				,max(appusernum) as appusernum
				,sum(apptraffic_up) as apptraffic_up
				,sum(apptraffic_dn) as apptraffic_dn
				,sum(apppacketsnum) as apppacketsnum
				,max(appsessionsnum) as appsessionsnum
				,sum(appnewsessionnum) as appnewsessionnum
              ,probe_type
				,area_id
            from ${from_job_ubas_appflow} where dt='${DATE}' and hour=${HOURS}
			 group by usergroupno,apptype,appid,appname,probe_type,area_id
			"

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_appflow_h} add if not exists partition(dt='${DATE}',hour='${HOURS}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_appflow_h_${DATE}_${HOURS};
		  set hive.execution.engine=spark;
        ${stat_sql}"

beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS],exit $ret !" 
        exit $ret
fi

exit 0
