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
job_radius_bras_d=job_radius_bras_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.9.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.9.1 update(2018-05-21)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"


DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`

mysql_sql="use ${DB_DATABASE};delete from zf_v2_gen_bras"

mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${mysql_sql}"

#########################################################################################

BASE=/user/hive/warehouse/${hive_database}.db/${job_radius_bras_d}/19700101

DB_URL=`awk -F'=' '/DB_URL/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`
MYSQL_TABLE=zf_v2_gen_bras
COLUMNS="BRAS_IP,BRAS_NAME,FIRST_TIME,LAST_TIME"

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.37.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${MYSQL_TABLE} --columns ${COLUMNS} --export-dir ${BASE} --fields-terminated-by '|' --input-null-string '\\N' --input-null-non-string '\\N' -m 2

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "sqoop mysql failed [$DATE][$HOURS],exit $ret !"
        exit $ret
fi
echo `date +"%Y-%m-%d %H:%M:%S"`      "sqoop mysql Success [$DATE]!"

exit 0
