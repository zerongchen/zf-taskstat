#!/bin/bash
#Created by turk 2018-04-13
#Project  ZF STAT
#Version 4.7.1 modified by chenym
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

key='hive.database'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hive.database:"$value
hive_database=$value
echo "hive_database:${hive_database}"
r_endtime=`date -d "$DATE $HOURS" +%s`
r_starttime=`expr ${r_endtime} - 3600`

exitMsg(){
ret=$1
msg=$2
if [ $ret -ne 0 ]
then
	echo `date +"%Y-%m-%d %H:%M:%S"`   "${msg}"
	exit $ret
fi
}

key='system.deploy.province.shortname'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "system.deploy.province.shortname:"$value
deployProvinceShortName=$value
echo "deployProvinceShortName:${deployProvinceShortName}"

key='system.deploy.province.provider'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "system.deploy.province.provider:"$value
depolyProvinceProvince=$value
echo "depolyProvinceProvince:${depolyProvinceProvince}"

version=0x01
module=0x01c4
file_type=000
device_deploy_loc=${deployProvinceShortName}
software_provider=${depolyProvinceProvince}

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.10.1 update(2018-06-22)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"


exec_hive_export(){
input_path=$1
output_file=$2
tmp_file=${output_file}.tmp
sql="insert overwrite directory '${input_path}' row format delimited fields terminated by '|'
select
'${r_starttime}' as r_starttime,
'${r_endtime}' as r_endtime,
srcgroup_id,
dstgroup_id,
count(1) as app_num,
apptype,
appid,
length(appname) as appnamelength,
appname,
round(sum(apptraffic_up)/1048576,2) as apptraffic_up,
round(sum(apptraffic_dn)/1048576,2) as apptraffic_dn
 from job_ubas_traffic_h where dt=${DATE} and hour=${HOURS} group by srcgroup_id,dstgroup_id,apptype,appid,appname"


echo "------------------------CONFIG---------------------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"` "INPUT_PATH          : ${input_path}"
echo `date +"%Y-%m-%d %H:%M:%S"` "TMP_FILE            : ${tmp_file}"
echo `date +"%Y-%m-%d %H:%M:%S"` "OUTPUT_FILE         : ${output_file}"
echo `date +"%Y-%m-%d %H:%M:%S"` "sql                 : ${sql}"
echo "---------------------------------------------------------------------------------"

hadoop fs -rm -r ${input_path}

echo "*********************************************************************"
echo ${sql}
echo "*********************************************************************"

hive -e "use ${hive_database};set mapred.job.name=${output_file};$sql"
ret=$?
exitMsg $ret "hive -e error"

hadoop fs -getmerge ${input_path} ${tmp_file}
ret=$?
exitMsg $ret "getmerge file error|input_path=${input_path},tmp_file=${tmp_file}"

mv ${tmp_file} ${output_file}
ret=$?
exitMsg $ret "mv file error"

hadoop fs -rm -r ${input_path}
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success ${output_file}!"
}

revokeJarSendKafka(){
filetype=$1
filename=$2
filetime=$3
filesize=$4
filerecord=$5
echo "filetype:${filetype}"
echo "filename:${filename}"
echo "filetime:${filetime}"
echo "filesize:${filesize}"
echo "filerecord:${filerecord}"
source ~/.bashrc

CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.kafkamessage.KafkaMessageMain

echo "---------- system arguments ----------------"
# java -version
echo "********** java version ********************"
java -version
echo "********** ip info *************************"
ifconfig | grep inet | grep -v inet6 | grep -v 127
echo "********** ZF_HOME *************************"
echo "ZF_HOME:${ZF_HOME}"
echo "--------------------------------------------"

java ${CLASS_PATH} ${MAIN_CLASS} ${filetype} ${filename} ${filetime} ${filesize} ${filerecord}
ret=$?
exitMsg $ret "send kafka message error"
}

# 0x01+0x01c4+000+GZ+CTSI+001+20180413021333
key='ubas.td.export.path'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
#java ${CLASS_PATH} ${MAIN_CLASS} ${key}
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "ubas.td.export.path:"$value

export_path=${value}/traffic
if [ x"${export_path}" = x ]; then
echo "config.UBAS_TRAFFIC_EXPORT_PATH is not set!"
export_path=${WORKPATH}/data
fi

if [ ! -d "${export_path}" ];then
mkdir ${export_path}
else
echo "文件夹 ${export_path} 已经存在"
fi

export_path_tmp=${export_path}_tmp
report_time=`date +%Y%m%d%H%M%S`
txtfile_name=${version}+${module}+${file_type}+${device_deploy_loc}+${software_provider}+001+${report_time}.txt
#tarfile_name=${version}+${module}+${file_type}+${device_deploy_loc}+${software_provider}+001+${report_time}.tar.gz
input_path=/tmp/job_ubas_traffic_h/export_traffic_file_${DATE}_${HOURS}
output_txtfile_tmp=${export_path_tmp}/${txtfile_name}
#output_tarfile_tmp=${export_path_tmp}/${tarfile_name}

#output_file=${export_path}/${tarfile_name}
output_file=${export_path}/${txtfile_name}
exec_hive_export ${input_path} ${output_txtfile_tmp}

filerecord=`cat ${output_txtfile_tmp} | wc -l`

#echo "tar -zcPvf ${output_tarfile_tmp} ${output_txtfile_tmp}"
#tar -zcPvf ${output_tarfile_tmp} ${output_txtfile_tmp}

filetype=01c4
filename=${txtfile_name}
filetime=${report_time}
filesize=`du -b ${output_txtfile_tmp}|awk -F ' ' '{print $1}'`

source ~/.bashrc

CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.kafkamessage.KafkaMessageMain

echo `date +"%Y-%m-%d %H:%M:%S"`  "---------- system arguments ----------------"
# java -version
echo `date +"%Y-%m-%d %H:%M:%S"`  "********** java version ********************"
java -version
echo `date +"%Y-%m-%d %H:%M:%S"`  "********** ip info *************************"
ifconfig
echo `date +"%Y-%m-%d %H:%M:%S"`  "********** ZF_HOME *************************"
echo "ZF_HOME:${ZF_HOME}"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------------"

echo "java CLASS_PATH MAIN_CLASS:${MAIN_CLASS} filetype:${filetype} filename:${filename} filetime:${filetime} filesize:${filesize} filerecord:${filerecord}"
echo "--------------------------------------------"
java ${CLASS_PATH} ${MAIN_CLASS} ${filetype} ${filename} ${filetime} ${filesize} ${filerecord}

ret=$?
exitMsg $ret "send kafka message error"
output_file=${export_path}/${txtfile_name}
mv ${output_txtfile_tmp} ${output_file}
ret=$?
exitMsg $ret "mv file ${output_txtfile_tmp} to ${output_file} error"

exit 0
