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


#Radius生成,接收文件详情表
createfile_detail_table=zf_v2_monitor_createfile_detail
receivedfile_detail_table=zf_v2_monitor_receivedfile_detail
#Radius上报文件详情表
uploadfile_detail_table=zf_v2_monitor_uploaddfile_detail
#上报异常文件监控详细表
uploaddfile_warn=zf_v2_monitor_uploaddfile_warn_detail
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



#1=文件接收延时：对比文件创建时间和文件接收时间，如果间隔超过阀值则为异常
RECEIVED_TIMEOUT=`java ${CLASS_PATH} com.aotain.statmange.config.ReadParamMain 'monitor.file.received.timeout' | tail -1`
echo "   monitor.file.received.timeout:" $RECEIVED_TIMEOUT
#2=文件上报延时：对比文件的上报时间和文件接收时间，如果间隔超过阀值则为异常
UPLOAD_TIMEOUT=`java ${CLASS_PATH} com.aotain.statmange.config.ReadParamMain 'monitor.file.upload.timeout' | tail -1`
echo "   monitor.file.received.timeout:" $UPLOAD_TIMEOUT
#3=文件大小不一致：对比上报文件大小和接收文件大小，不一致则为异常

stat_sql=" use ${DB_DATABASE};
           delete from ${uploaddfile_warn} where fileupload_time >= ${lastTimestamp} and fileupload_time < ${currTimestamp};
           INSERT INTO ${uploaddfile_warn}(
               file_name,
               server_ip,
               received_ip,
               file_type,
               fileupload_time,
               probe_type,
               area_id,
               software_provider,
               warn_type,
               create_time
           )(
           SELECT * from
           (SELECT
              t.file_name,
              t.server_ip,
              t.received_ip,
              t.file_type,
              t.fileupload_time,
              t.probe_type,
              t.area_id,
              t.software_provider,
              (CASE
                    WHEN t.fileupload_time - r.filereceived_time >= ${UPLOAD_TIMEOUT} THEN 2
                    WHEN r.filereceived_time - r.filecreate_time >= ${RECEIVED_TIMEOUT} THEN 1
                    ELSE  3
               END) AS warn_type,
              SYSDATE() AS create_time
            FROM
              ${uploadfile_detail_table} t
              LEFT JOIN ${receivedfile_detail_table} r
                ON t.file_name = r.file_name
                AND t.file_type = r.file_type
                WHERE t.fileupload_time >= ${lastTimestamp}
                      and t.fileupload_time < ${currTimestamp}
                      and (t.fileupload_time - r.filereceived_time >= ${UPLOAD_TIMEOUT}
                        OR r.filereceived_time-r.filecreate_time>= ${RECEIVED_TIMEOUT}
                        OR t.file_size!= r.file_size)

            union all

              SELECT
                  t.file_name,
                  t.server_ip,
                  t.received_ip,
                  t.file_type,
                  t.fileupload_time,
                  t.probe_type,
                  t.area_id,
                  t.software_provider,
                  (CASE
                        WHEN t.fileupload_time - c.file_time >= ${UPLOAD_TIMEOUT} THEN 2
                        ELSE 3
                   END) AS warn_type,
                  SYSDATE() AS create_time
                FROM
                   ${uploadfile_detail_table} t
                  LEFT JOIN ${createfile_detail_table} c
                    ON t.file_name = c.file_name
                    AND t.file_type = c.file_type
                WHERE t.fileupload_time >= ${lastTimestamp}
                      and t.fileupload_time < ${currTimestamp}
                      and (t.fileupload_time - c.file_time >= ${UPLOAD_TIMEOUT}
                      OR t.file_size!=c.file_size)
             )A
           )"

echo "stat_sql=${stat_sql}"

mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${stat_sql}"

ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE][$HOURS],exit $ret !"
        exit $ret
fi

currentHour=`date -d @${lastTimestamp}  "+%Y%m%d%H"`
collectSql=" use ${DB_DATABASE};
            select
                file_type,
                ${currentHour} as time,
                count(file_type) As warn_file_num
            from ${uploaddfile_warn}
            where
                fileupload_time >= ${lastTimestamp}
                and fileupload_time < ${currTimestamp}
            group by file_type
            "
echo "uploadFile collectSql=${collectSql}"
result=`mysql -h${DB_HOSTNAME}  -P${DB_PORT}  -u${DB_USER} -p${DB_PASSWD} -e "${collectSql}"`
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "send upload file warn msg error,exit $ret !"
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

java ${CLASS_PATH} ${MAIN_CLASS} 3 ${param}
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "send upload file error msg error,exit $ret !"
        exit $ret
fi
exit 0
