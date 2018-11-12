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
#DDOS 天粒度表
from_job_ubas_ddos_d=job_ubas_ddos_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.10.1 update(2018-06-20)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${from_job_ubas_ddos_d}/${DATE}
DB_URL=`awk -F'=' '/DB_URL/{print $2}' ${WORKPATH}/../conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/../conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/../conf/config`
MYSQL_TABLE=zf_v2_ubas_ddos_d


DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/../conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/../conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/../conf/config`

stat_sql=" use ${DB_DATABASE};
           delete from ${MYSQL_TABLE} where STAT_TIME=${DATE} ;
           "
mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "clean mysql failed [$DATE],exit $ret !"
        exit $ret
fi

#---------------------------------------------------------------------------------------------

COLUMNS="STAT_TIME,PUSERGROUGNO,APPATTACKTYPE,APPATTACKTRAFFIC,APPATTACKRATE,PROBE_TYPE,AREA_ID"
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/../lib/mysql-connector-java-5.1.37.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${MYSQL_TABLE} --columns ${COLUMNS} --export-dir ${OUTPUTPATH} --fields-terminated-by '|' --input-null-string '\\N' --input-null-non-string '\\N' -m 2

ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "sqoop mysql failed [$DATE][$HOURS],exit $ret !"
        exit $ret
fi

exit 0
