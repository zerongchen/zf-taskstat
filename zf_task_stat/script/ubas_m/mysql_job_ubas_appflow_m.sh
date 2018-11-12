#!/bin/bash

#Created by turk 2018-03-23
#Project  ZF STAT
#Version 4.6.1 modified by chenym
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
#-----------------------------------------------
SDATE=`date -d last-month +%Y%m`01
#SDATESTR=`date -d last-month +%Y%m`

if [ $# -eq 1 ]
then
	SDATE=$1
fi
SDATESTR=${SDATE:0:6}
EDATE=`date -d next-month"${SDATE}" +%Y%m%d`
echo "get SDATE: ${SDATE},EDATE:${EDATE}"
##-----------------------------------------------
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain

#--------------------------------------------------
key='hive.database'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
hive_database=$value
echo "hive_database:${hive_database}"
#--------------------------------------------------
#通用流量小时粒度表
from_job_ubas_appflow_m=job_ubas_appflow_m

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-27)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${from_job_ubas_appflow_m}/${SDATE}
DB_URL=`awk -F'=' '/DB_URL/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`
MYSQL_TABLE=zf_v2_ubas_appflow_m

#---------------------------------------------------------------------------------------------------
DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`
stat_sql=" use ${DB_DATABASE};delete from ${MYSQL_TABLE} where stat_time=${SDATESTR};"
echo "stat_sql=${stat_sql}"
mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "delete data failed [$DATE],exit $ret !"
        exit $ret
fi
#---------------------------------------------------------------------------------------------------

COLUMNS="STAT_TIME,USERGROUPNO,APPTYPE,APPID,APPNAME,APPUSERNUM,APPTRAFFIC_UP,APPTRAFFIC_DN,APPPACKETSNUM,APPSESSIONSNUM,APPNEWSESSIONNUM,PROBE_TYPE,AREA_ID"

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.37.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${MYSQL_TABLE} --columns ${COLUMNS} --export-dir ${OUTPUTPATH} --fields-terminated-by '|' --input-null-string '\\N' --input-null-non-string '\\N' -m 2

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "sqoop mysql failed [$SDATE],exit $ret !"
        exit $ret
fi

exit 0
