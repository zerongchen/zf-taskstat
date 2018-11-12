#!/bin/bash

#Created by turk 2018-04-03
#Project  ZF STAT
#Version 4.6.1 modified by chenzr
#Description:

SOURCE="${BASH_SOURCE[0]}"
BIN_DIR="$( dirname "$SOURCE" )"
while [ -h "$SOURCE" ]
  do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
  done
  BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

#work home directory
WORKPATH=`cd $BIN_DIR/../../;pwd`

#--------------------------------------------------

OUTPUT_PATH=$1
TIMESTAMP=$2
PACKETTYPE=$3
PACKETSUBTYPE=$4

echo "get OUTPUT_PATH: ${OUTPUT_PATH} ,get TIMESTAMP : ${TIMESTAMP} ,get PACKETTYPE : ${PACKETTYPE} , get PACKETSUBTYPE : ${PACKETSUBTYPE}"
#--------------------------------------------------


#mysql table
MYSQL_TABLE=zf_v2_monitor_savedata_h
MYSQL_COLUMN="STAT_TIME,PACKETTYPE,PACKETSUBTYPE,SAVENUM,PROBE_TYPE,AREA_ID,SOFTWARE_PROVIDER,CREATE_TIME"

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-04-08)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

DB_URL=`awk -F'=' '/DB_URL/{print $2}' ${WORKPATH}/conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/conf/config`

DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/conf/config`
DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/conf/config`


clean_sql=" use ${DB_DATABASE};
            delete from ${MYSQL_TABLE} where stat_time=${TIMESTAMP} and packettype=${PACKETTYPE} and packetsubtype in (${PACKETSUBTYPE});
          "
echo "clean_sql = ${clean_sql}"

mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${clean_sql}"

ret=$?
if [ $ret -ne 0 ]
then
      echo `date +"%Y-%m-%d %H:%M:%S"`   "clean mysql failed [$TIMESTAMP][$PACKETTYPE][$PACKETSUBTYPE],exit $ret !"
      exit $ret
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/mysql-connector-java-5.1.37.jar
sqoop export --connect ${DB_URL} --username ${DB_USER} --password ${DB_PASSWD} --table ${MYSQL_TABLE} --columns ${MYSQL_COLUMN} --export-dir ${OUTPUT_PATH} --fields-terminated-by '|' --input-null-string '\\N' --input-null-non-string '\\N' -m 2  --map-column-java CREATE_TIME=java.sql.Timestamp

ret=$?
if [ $ret -ne 0 ]
then
      echo `date +"%Y-%m-%d %H:%M:%S"`   "sqoop mysql failed [$DATE][$HOURS],exit $ret !"
      exit $ret
fi

exit 0
