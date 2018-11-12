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
#通用流量分钟粒度表
from_job_ubas_traffic_d=job_ubas_traffic_d
#通用流量小时粒度表
to_job_ubas_traffic_w=job_ubas_traffic_w

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-27)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_traffic_w}/${SDATE}

# select lpad(conv(13,10,2),32,'0') ;
stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_traffic_w} partition(dt='${SDATE}')
       select '${SDATE}' as stat_time
			    ,srcgroup_id
			    ,dstgroup_id
			    ,apptype
			    ,appid
			    ,appname
			    ,sum(apptraffic_up) as apptraffic_up
			    ,sum(apptraffic_dn) as apptraffic_dn
			    ,probe_type
			    ,area_id
			    ,src_areasubid1
			    ,src_areasubid2
               ,src_areasubid3
               ,src_areasubid4
			    ,dst_areasubid1
			    ,dst_areasubid2
              ,dst_areasubid3
              ,dst_areasubid4
            from ${from_job_ubas_traffic_d} where dt>='${SDATE}' and dt<'${EDATE}'
			 group by srcgroup_id,dstgroup_id,apptype,appid,appname,probe_type,area_id
			 ,src_areasubid1,src_areasubid2,src_areasubid3,src_areasubid4,dst_areasubid1,dst_areasubid2,dst_areasubid3,dst_areasubid4
	   "

echo "stat_sql=${stat_sql}"

hive -e   "use ${hive_database};
         alter table ${to_job_ubas_traffic_w} add if not exists partition(dt='${SDATE}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_traffic_d_${SDATE};
         ${stat_sql}"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$SDATE],exit $ret !"
        exit $ret
fi

exit 0
