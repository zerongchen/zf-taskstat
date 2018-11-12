#!/bin/bash

#Created by turk 2018-03-15
#Project  ZF STAT
#Version 4.6.1 modified by chenzr
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
#work home directory
WORKPATH=`cd $bin/..;pwd`
if [ $# -ne 1  -a  $# -ne 2 -a ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:[DATE][HOURS]"
        exit 1
fi
#--------------------------------------------------
DATEHOUR=$(date +%Y%m%d%H -d  '-1 hours')
DATE=${DATEHOUR:0:8}
HOURS=${DATEHOUR:8}


if [ $# -eq 2 ]
then
	DATE=$1                 #specified date
	HOURS=$2                #specified hour
fi
echo "get Date: ${DATE}, get Hour:${HOURS} "
#--------------------------------------------------

lastTimestamp=`date -d "$DATE $HOURS" +%s`
currTimestamp=`expr $lastTimestamp + 3600`


#Radius接收文件详情表
receivedfile_detail_table=zf_v2_monitor_receivedfile_detail
#Radius上报文件详情表
uploadfile_detail_table=zf_v2_monitor_uploaddfile_detail


#wirte error msg to kafka
CLASS_PATH=" -classpath "$(echo $WORKPATH/../lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.kafkamessage.WriteWarnMsgToKafkaMain

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-27)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

DB_DATABASE=`awk -F'=' '/DB_DATABASE/{print $2}' ${WORKPATH}/../conf/config`
DB_HOSTNAME=`awk -F'=' '/DB_HOSTNAME/{print $2}' ${WORKPATH}/../conf/config`
DB_PORT=`awk -F'=' '/DB_PORT/{print $2}' ${WORKPATH}/../conf/config`
DB_USER=`awk -F'=' '/DB_USER/{print $2}' ${WORKPATH}/../conf/config`
DB_PASSWD=`awk -F'=' '/DB_PASSWD/{print $2}' ${WORKPATH}/../conf/config`
#上报时延
UPLOAD_TIMEOUT=`java ${CLASS_PATH} com.aotain.statmange.config.ReadParamMain 'monitor.file.upload.timeout' | tail -1`
echo "   monitor.file.received.timeout:" $UPLOAD_TIMEOUT
currentHour=`date -d @${lastTimestamp}  "+%Y%m%d%H"`
collectSql=" use ${DB_DATABASE};
           SELECT * FROM (
                          SELECT
                            t.file_type,
                            ${currentHour} as time,
                            SUM(
                              CASE
                                WHEN u.file_name IS NULL
                                THEN 1
                                ELSE 0
                              END
                            ) AS warn_file_num
                          FROM
                            ${receivedfile_detail_table} t
                            LEFT JOIN ${uploadfile_detail_table} u
                              ON t.file_name = u.file_name
                              AND t.file_type = u.file_type
                            WHERE t.filereceived_time >= ${lastTimestamp}
                            and t.filereceived_time < ${currTimestamp}
                          GROUP BY t.file_type
             ) A WHERE warn_file_num > 0
           "

echo "receivedFile collectSql=${collectSql}"

result=`mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${collectSql}"`

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "send received file warn msg error,exit $ret !"
        exit $ret
fi

source ~/.bashrc

param=`echo $result|sed 's/ /|/g'|cut -d '|' -f 4-`

echo ${param}

echo "********** java version ********************"
java -version
echo "********** CLASS PATH ********************"
echo ${CLASS_PATH}
echo "********** MAIN CLASS ********************"
echo ${MAIN_CLASS}

java ${CLASS_PATH} ${MAIN_CLASS} 2 ${param}
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "send upload file warn msg error,exit $ret !"
        exit $ret
fi
exit 0
