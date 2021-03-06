#!/bin/bash

#Created by turk 2018-06-20
#Project  ZF STAT
#Version 2.10.1 modified by chenym
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
#-----------------------------------------------
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
#非法路由月粒度表
from_job_ubas_illegalroutes_w=job_ubas_illegalroutes_w

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.10.1 update(2018-06-20)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
location_directory=tmp/zf_v2_ubas_illegalroutes_w_${SDATE}
OUTPUTPATH=/user/${user}/${location_directory}

hadoop fs -test -e ${OUTPUTPATH}
$ret=$?
if [ $ret -eq 0 ]
then
        hadoop fs -rm -r ${OUTPUTPATH}
        echo "delete directory ${OUTPUTPATH}"
fi

sql="insert overwrite directory '${location_directory}' row format delimited fields terminated by '|'
select '${SDATE}'
,cp
,sum(nodeintraffic)
,sum(nodeouttraffic)
,probe_type
,area_id
from ${from_job_ubas_illegalroutes_w} where dt='${SDATE}'
group by cp,probe_type,area_id;"

echo "location_directory:${location_directory}"
echo "OUTPUTPATH:${OUTPUTPATH}"
echo "sql:${sql}"

hive -e   "use ${hive_database};${sql}"
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

DB_URL=`awk -F'=' '/DB_URL/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`
MYSQL_TABLE=zf_v2_ubas_illegalroutes_w
#---------------------------------------------------------------------------------------------------
DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`
stat_sql=" use ${DB_DATABASE};delete from ${MYSQL_TABLE} where stat_time=${SDATE};"
echo "stat_sql=${stat_sql}"
mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "delete data failed [${SDATE}],exit $ret !"
        exit $ret
fi
#---------------------------------------------------------------------------------------------------
COLUMNS="STAT_TIME,CP,NODEINTRAFFIC,NODEOUTTRAFFIC,PROBE_TYPE,AREA_ID"

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.37.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${MYSQL_TABLE} --columns ${COLUMNS} --export-dir ${OUTPUTPATH} --fields-terminated-by '|' --input-null-string '\\N' --input-null-non-string '\\N' -m 2

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "sqoop mysql failed [$DATE],exit $ret !"
        exit $ret
fi

hadoop fs -test -e ${OUTPUTPATH}
if [ $ret -eq 0 ]
then
        hadoop fs -rm -r ${OUTPUTPATH}
        echo "delete directory ${OUTPUTPATH}"
fi

exit 0
