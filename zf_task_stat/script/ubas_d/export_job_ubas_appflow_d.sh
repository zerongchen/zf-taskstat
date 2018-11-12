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

r_endtime=`date -d "$DATE $HOURS" +%s`
r_starttime=`expr ${r_endtime} - 3600`

echo "r_endtime  :${r_endtime}"
echo "r_starttime:${r_starttime}"

exitMsg(){
ret=$1
msg=$2
if [ $ret -ne 0 ]
then
	echo `date +"%Y-%m-%d %H:%M:%S"`   "${msg}"
	exit $ret
fi
}
########################### 将生成文件信息写到kafka队列 ####################
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
ifconfig
echo "********** ZF_HOME *************************"
echo "ZF_HOME:${ZF_HOME}"
echo "--------------------------------------------"
echo "java CLASS_PATH MAIN_CLASS:${MAIN_CLASS} filetype:${filetype} filename:${filename} filetime:${filetime} filesize:${filesize} filerecord:${filerecord}"
echo "--------------------------------------------"

java ${CLASS_PATH} ${MAIN_CLASS} ${filetype} ${filename} ${filetime} ${filesize} ${filerecord}
ret=$?
exitMsg $ret "send kafka message error"
}


########################### 获取 DPI网路模式 system.deploy.mode ####################
key='system.deploy.mode'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "system.deploy.mode:"$value
system_deploy_mode=$value

########################### 获取 系统部署省份简称 system.deploy.province.shortname ####################
key='system.deploy.province.shortname'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "system.deploy.province.shortname:"$value
system_deploy_province_shortname=$value

########################### 获取 TD文件生成路径 ubas.td.export.path ####################
key='ubas.td.export.path'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "ubas.td.export.path:"$value
export_path=${value}/appflow

if [ ! -d "${export_path}" ];then
mkdir ${export_path}
else
echo "directory ${export_path} exists!"
fi

########################### 获取 probe_type=1时 idc机房列表 ####################
distinct_house_output=/tmp/distinct_house_output_${DATE}
select_idc_house_sql="insert overwrite local directory '${distinct_house_output}'
    select distinct area_id from ${hive_database}.job_ubas_appflow_d where dt='${DATE}' and probe_type=1 and length(area_id)>0 and area_id <> 'unkownAreaId'"

echo "select_idc_house_sql:${select_idc_house_sql}"
hive -e "use ${hive_database};set mapred.job.name=select_idc_house_sql;$select_idc_house_sql"

ret=$?
exitMsg $ret "hive -e error {distinct_house_output}"

echo `date +"%Y-%m-%d %H:%M:%S"`  "select_idc_house list"
cat ${distinct_house_output}/*
echo `date +"%Y-%m-%d %H:%M:%S"`  ""

########################### 循环 idc机房列表 ####################
cat ${distinct_house_output}/* | while read line
do
    echo "get area_id:"$line
    report_time=`date +%Y%m%d%H%M%S`
    file_name=0x01+0x0102+000+I-${system_deploy_province_shortname}-${line}+001.txt
    echo "fileName:"$file_name

input_path=/tmp/job_ubas_appflow_d/appflow_1_${line}
export_path_tmp=${export_path}_tmp/${DATE}
if [ ! -d "${export_path_tmp}" ];then
mkdir -p ${export_path_tmp}
else
echo "directory ${export_path_tmp} exists!"
rm -rf ${export_path_tmp}/*
fi
output_txtfile_tmp=${export_path_tmp}/${file_name}

select_one_idc_sql="insert overwrite directory '${input_path}' row format delimited fields terminated by '|'
    select
'${r_starttime}' as r_starttime,
'${r_endtime}' as r_endtime,
    apptype,
    appid,
    appname,
    sum(appusernum) as appusernum,
    round(sum(apptraffic_up)/1048576,2) as apptraffic_up,
    round(sum(apptraffic_dn)/1048576,2) as apptraffic_dn,
    sum(apppacketsnum) as apppacketsnum,
    max(appsessionsnum) as appsessionsnum,
    sum(appnewsessionnum) as appnewsessionnum
    from job_ubas_appflow_d where dt=${DATE} and probe_type=1 and area_id='${line}' group by apptype,appid,appname"

echo "------------------------CONFIG---------------------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"` "idc_area_id                  : ${line}"
echo `date +"%Y-%m-%d %H:%M:%S"` "input_path                   : ${input_path}"
echo `date +"%Y-%m-%d %H:%M:%S"` "output_txtfile_tmp           : ${output_txtfile_tmp}"
echo `date +"%Y-%m-%d %H:%M:%S"` "sql                          : ${select_one_idc_sql}"
echo "---------------------------------------------------------------------------------"

hive -e "use ${hive_database};set mapred.job.name=select_one_idc_${line};${select_one_idc_sql}"
ret=$?
if [ $ret -ne 0 ]
then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "get data by idcHouse error"
	exit ${ret}
fi

hadoop fs -getmerge ${input_path} ${output_txtfile_tmp}_tmp
ret=$?
exitMsg $ret "getmerge file error"

mv ${output_txtfile_tmp}_tmp ${output_txtfile_tmp}
ret=$?
exitMsg $ret "mv file error"

hadoop fs -rm -r ${input_path}

done
########################### 循环 idc机房列表 结束####################

if [ ${system_deploy_mode} -eq 0 ]
then
echo "************** 汇聚网络模式 probe_type=0 所有dpi命名 P-${system_deploy_province_shortname} **************************"
########################### 汇聚网络模式 probe_type=0 所有dpi命名 P-${system_deploy_province_shortname} ####################

	echo `date +"%Y-%m-%d %H:%M:%S"`   "probe_type=0,system_deploy_mode="${system_deploy_mode}
	echo "system_deploy_province_shortname="${system_deploy_province_shortname}
    report_time=`date +%Y%m%d%H%M%S`
    file_name=0x01+0x0102+000+P-${system_deploy_province_shortname}+001.txt
    echo "fileName:"$file_name

input_path=/tmp/job_ubas_appflow_d/appflow_0_${system_deploy_province_shortname}
export_path_tmp=${export_path}_tmp/${DATE}
if [ ! -d "${export_path_tmp}" ];then
mkdir -p ${export_path_tmp}
else
echo "directory ${export_path_tmp} exists!"
rm -rf ${export_path_tmp}/*
fi
output_txtfile_tmp=${export_path_tmp}/${file_name}

select_one_province_sql="insert overwrite directory '${input_path}' row format delimited fields terminated by '|'
    select
'${r_starttime}' as r_starttime,
'${r_endtime}' as r_endtime,
    apptype,
    appid,
    appname,
    sum(appusernum) as appusernum,
    round(sum(apptraffic_up)/1048576,2) as apptraffic_up,
    round(sum(apptraffic_dn)/1048576,2) as apptraffic_dn,
    sum(apppacketsnum) as apppacketsnum,
    max(appsessionsnum) as appsessionsnum,
    sum(appnewsessionnum) as appnewsessionnum
    from job_ubas_appflow_d where dt=${DATE} and probe_type=0 group by apptype,appid,appname"

echo "------------------------CONFIG---------------------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"` "idc_area_id                  : ${line}"
echo `date +"%Y-%m-%d %H:%M:%S"` "input_path                   : ${input_path}"
echo `date +"%Y-%m-%d %H:%M:%S"` "output_txtfile_tmp           : ${output_txtfile_tmp}"
echo `date +"%Y-%m-%d %H:%M:%S"` "select_one_province_sql      : ${select_one_province_sql}"
echo "---------------------------------------------------------------------------------"

hive -e "use ${hive_database};set mapred.job.name=select_one_province_${system_deploy_province_shortname};${select_one_province_sql}"
ret=$?
if [ $ret -ne 0 ]
then
	echo `date +"%Y-%m-%d %H:%M:%S"`   " hive -e {select_one_province_sql} error"
	exit $ret
fi

hadoop fs -getmerge ${input_path} ${output_txtfile_tmp}_tmp
ret=$?
exitMsg $ret "getmerge file error"

mv ${output_txtfile_tmp}_tmp ${output_txtfile_tmp}
ret=$?
exitMsg $ret "mv file error"

hadoop fs -rm -r ${input_path}
fi

if [ ${system_deploy_mode} -eq 1 ]
then
echo "************** 独立网络模式 probe_type=0 所有dpi命名 M-{地市简称} **************************"
########################### 独立网络模式 probe_type=0 所有dpi命名 M-${地市简称} ####################

	echo `date +"%Y-%m-%d %H:%M:%S"`   "system_deploy_mode              :${system_deploy_mode}"
	echo `date +"%Y-%m-%d %H:%M:%S"`   "probe_type                      :0"
	echo `date +"%Y-%m-%d %H:%M:%S"`   "system_deploy_province_shortname:${system_deploy_province_shortname}"

########################### 获取 probe_type=1时 地市简称列表 ####################
distinct_city_output=/tmp/distinct_city_output_${DATE}
select_idc_city_sql="insert overwrite local directory '${distinct_city_output}'
    select distinct area_id from ${hive_database}.job_ubas_appflow_d where dt='${DATE}' and probe_type=0 and length(area_id)>0 and area_id <> 'unkownAreaId'"

echo "select_idc_city_sql:${select_idc_city_sql}"

hive -e "use ${hive_database};set mapred.job.name=select_idc_city_sql;$select_idc_city_sql"

ret=$?
exitMsg $ret "hive -e error {select_idc_city_sql}"

echo `date +"%Y-%m-%d %H:%M:%S"`  "select_idc_city list"
cat ${distinct_city_output}/*
echo `date +"%Y-%m-%d %H:%M:%S"`  ""

cat ${distinct_city_output}/* | while read line
do
    key=${line}
    CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
    MAIN_CLASS=com.aotain.statmange.config.ReadZfDictChinaareaParam
    value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
    echo "${line}:"$value
    jiancheng=${value}
    echo "get area_id  :"$line
    echo "get jiancheng:"$jiancheng

if [ x${jiancheng} != "x" ]
then
    file_name=0x01+0x0102+000+M-${jiancheng}+001.txt
    echo "fileName:"$file_name

    input_path=/tmp/job_ubas_appflow_d/appflow_0_${jiancheng}
    export_path_tmp=${export_path}_tmp/${DATE}
if [ ! -d "${export_path_tmp}" ];then
mkdir -p ${export_path_tmp}
else
echo "directory ${export_path_tmp} exists!"
rm -rf ${export_path_tmp}/*
fi
    output_txtfile_tmp=${export_path_tmp}/${file_name}

select_one_city_sql="insert overwrite directory '${input_path}' row format delimited fields terminated by '|'
    select
'${r_starttime}' as r_starttime,
'${r_endtime}' as r_endtime,
    apptype,
    appid,
    appname,
    sum(appusernum) as appusernum,
    round(sum(apptraffic_up)/1048576,2) as apptraffic_up,
    round(sum(apptraffic_dn)/1048576,2) as apptraffic_dn,
    sum(apppacketsnum) as apppacketsnum,
    max(appsessionsnum) as appsessionsnum,
    sum(appnewsessionnum) as appnewsessionnum
    from job_ubas_appflow_d where dt=${DATE} and probe_type=0 and area_id='${line}' group by apptype,appid,appname"

echo "------------------------CONFIG---------------------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"` "idc_area_id                  : ${line}"
echo `date +"%Y-%m-%d %H:%M:%S"` "jiancheng                    : ${jiancheng}"
echo `date +"%Y-%m-%d %H:%M:%S"` "input_path                   : ${input_path}"
echo `date +"%Y-%m-%d %H:%M:%S"` "output_txtfile_tmp           : ${output_txtfile_tmp}"
echo `date +"%Y-%m-%d %H:%M:%S"` "sql                          : ${select_one_city_sql}"
echo "---------------------------------------------------------------------------------"

hive -e "use ${hive_database};set mapred.job.name=select_one_idc_${jiancheng};${select_one_city_sql}"
ret=$?
exitMsg $ret "hive -e error"

hadoop fs -getmerge ${input_path} ${output_txtfile_tmp}_tmp
ret=$?
exitMsg $ret "getmerge file error"

mv ${output_txtfile_tmp}_tmp ${output_txtfile_tmp}
ret=$?
exitMsg $ret "mv file error"

hadoop fs -rm -r ${input_path}

fi
done
fi

exit 0
