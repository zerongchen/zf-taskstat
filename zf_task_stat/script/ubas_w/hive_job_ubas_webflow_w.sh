#!/bin/bash

#Created by turk 2018-03-27
#Project  ZF STAT
#Version 4.6.1 modified by chenym
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
##-----------------------------------------------
SDATE=`date -d"7 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	SDATE=$1
fi
EDATE=`date -d +7day"$SDATE" +%Y%m%d`
echo "get SDATE: ${SDATE},EDATE:${EDATE}"
##-----------------------------------------------
user=`echo "$USER"`
echo "current user:$user"
##-----------------------------------------------
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain

#--------------------------------------------------
key='hive.database'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
hive_database=$value
echo "hive_database:${hive_database}"
#--------------------------------------------------
# web流量天粒度表
from_job_ubas_webflow_d=job_ubas_webflow_d
# web流量周粒度表
to_job_ubas_webflow_w=job_ubas_webflow_w

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-27)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_webflow_w}/${SDATE}

stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_webflow_w} partition(dt='${SDATE}')
      select '${SDATE}' as stat_time
			    ,usergroupno as usergroupno
				,sitename as sitename
              ,sitetype as sitetype
				,sum(sitehitfreq) as sitehitfreq
				,sum(sitetraffic_up) as sitetraffic_up
				,sum(sitetraffic_dn) as sitetraffic_dn
				,packetsubtype
              ,probe_type
				,area_id
            from ${from_job_ubas_webflow_d} where dt>='${SDATE}' and dt<'${EDATE}'
			 group by usergroupno,sitename,sitetype,packetsubtype,probe_type,area_id
			"

echo "stat_sql=${stat_sql}"

hive -e   "use ${hive_database};
         alter table ${to_job_ubas_webflow_w} add if not exists partition(dt='${SDATE}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_webflow_w_${SDATE};
         ${stat_sql}"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$SDATE],exit $ret !"
        exit $ret
fi

key='hadoop.url'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url:"$value
IMPALA_SERVER=`echo ${value}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`

echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_webflow_w} "
else
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_webflow_w} " -i ${IMPALA_SERVER}
fi
exit 0
