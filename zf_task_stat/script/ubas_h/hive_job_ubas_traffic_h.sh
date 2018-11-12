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
from_job_ubas_traffic=job_ubas_traffic
#通用流量小时粒度表
to_job_ubas_traffic_h=job_ubas_traffic_h

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-16)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_traffic_h}/${DATE}/${HOURS}

# select lpad(conv(13,10,2),32,'0') ;
stat_sql=" INSERT overwrite TABLE ${to_job_ubas_traffic_h} partition(dt='${DATE}',hour=${HOURS})
       select '${DATE}${HOURS}' as stat_time
			    ,srcgroup_id
			    ,dstgroup_id
			    ,apptype
			    ,appid
			    ,appname
			    ,apptraffic_up
			    ,apptraffic_dn
			    ,probe_type
			    ,area_id
			    ,substr(srcgroup_id_binary,1,3) as src_areasubid1
			    ,case when(substr(srcgroup_id_binary,0,3) == '001') then substr(srcgroup_id_binary,4,2)
			          when(substr(srcgroup_id_binary,0,3) == '010') then substr(srcgroup_id_binary,4,6)
			          when(substr(srcgroup_id_binary,0,3) == '011') then substr(srcgroup_id_binary,4,8)
			          else 000 end as src_areasubid2
               ,case when(substr(srcgroup_id_binary,0,3) == '001') then substr(srcgroup_id_binary,6,5)
			          when(substr(srcgroup_id_binary,0,3) == '010') then substr(srcgroup_id_binary,10,5)
			          when(substr(srcgroup_id_binary,0,3) == '011') then substr(srcgroup_id_binary,12,3)
			          else 000 end as src_areasubid3
              ,case when(substr(srcgroup_id_binary,0,3) == '001') then substr(srcgroup_id_binary,11,6)
			          when(substr(srcgroup_id_binary,0,3) == '010') then substr(srcgroup_id_binary,15,6)
			          when(substr(srcgroup_id_binary,0,3) == '011') then substr(srcgroup_id_binary,15,8)
			          else 000 end as src_areasubid4
			    ,substr(dstgroup_id_binary,1,3) as dst_areasubid1
			    ,case when(substr(dstgroup_id_binary,0,3) == '001') then substr(dstgroup_id_binary,4,2)
			          when(substr(dstgroup_id_binary,0,3) == '010') then substr(dstgroup_id_binary,4,6)
			          when(substr(dstgroup_id_binary,0,3) == '011') then substr(dstgroup_id_binary,4,8)
			          else 000 end as dst_areasubid2
               ,case when(substr(dstgroup_id_binary,0,3) == '001') then substr(dstgroup_id_binary,6,5)
			          when(substr(dstgroup_id_binary,0,3) == '010') then substr(dstgroup_id_binary,10,5)
			          when(substr(dstgroup_id_binary,0,3) == '011') then substr(dstgroup_id_binary,12,3)
			          else 000 end as dst_areasubid3
              ,case when(substr(dstgroup_id_binary,0,3) == '001') then substr(dstgroup_id_binary,11,6)
			          when(substr(dstgroup_id_binary,0,3) == '010') then substr(dstgroup_id_binary,15,6)
			          when(substr(dstgroup_id_binary,0,3) == '011') then substr(dstgroup_id_binary,15,8)
			          else 000 end as dst_areasubid4
      from(select  lpad(conv(srcgroup_id,10,2),32,0) as srcgroup_id_binary
				,lpad(conv(dstgroup_id,10,2),32,0) as dstgroup_id_binary
				,srcgroup_id
				,dstgroup_id
              ,apptype as apptype
              ,appid as appid
              ,appname as appname
				,max(apptraffic_up) as apptraffic_up
				,max(apptraffic_dn) as apptraffic_dn
              ,probe_type
				,area_id
            from ${from_job_ubas_traffic} where dt='${DATE}' and hour=${HOURS}
			 group by srcgroup_id,dstgroup_id,apptype,appid,appname,probe_type,area_id
	  )t1
			"

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_traffic_h} add if not exists partition(dt='${DATE}',hour='${HOURS}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_traffic_h_${DATE}_${HOURS};
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
