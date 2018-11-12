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
ifconfig | grep inet | grep -v inet6 | grep -v 127
echo "********** ZF_HOME *************************"
echo "ZF_HOME:${ZF_HOME}"
echo "--------------------------------------------"
echo "java CLASS_PATH MAIN_CLASS:${MAIN_CLASS} filetype:${filetype} filename:${filename} filetime:${filetime} filesize:${filesize} filerecord:${filerecord}"
echo "--------------------------------------------"

java ${CLASS_PATH} ${MAIN_CLASS} ${filetype} ${filename} ${filetime} ${filesize} ${filerecord}
ret=$?
exitMsg $ret "send kafka message error"
}


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

export_path_tmp=${export_path}_tmp/${DATE}

########################### 循环 idc机房列表 ####################

filelist=`ls ${export_path_tmp}`
for line in $filelist
do
    echo $line
    echo "get file info:"$line
    filePrefix=`echo $line|awk -F '.' '{print $1}'`
    report_time=`date +%Y%m%d%H%M%S`
    output_txtfile_tmp=${export_path_tmp}/${filePrefix}.txt
#    output_tarfile_tmp=${export_path_tmp}/${filePrefix}+${report_time}.tar.gz

filerecord=`cat ${output_txtfile_tmp} | wc -l`
#echo "tar -zcPvf ${output_tarfile_tmp} ${output_txtfile_tmp}"
#tar -zcPvf ${output_tarfile_tmp} ${output_txtfile_tmp}

filetype=0102
filename=${filePrefix}+${report_time}.txt
filetime=${report_time}
filesize=`du -b ${output_txtfile_tmp}|awk -F ' ' '{print $1}'`

revokeJarSendKafka ${filetype} ${filename} ${filetime} ${filesize} ${filerecord}
output_file=${export_path}/${filePrefix}+${report_time}.txt
mv ${output_txtfile_tmp} ${output_file}
ret=$?
exitMsg $ret "mv file ${output_txtfile_tmp} to ${output_file} error"

if [ ! -f ${output_txtfile_tmp} ];then
echo "directory [${output_txtfile_tmp}] not exists"
else
echo "delete temp file:"${output_txtfile_tmp}
rm -rf ${output_txtfile_tmp}

    if [ ! -f ${output_txtfile_tmp} ];then
        echo "temp file[${output_txtfile_tmp}]delete success"
    else
        echo "temp file[${output_txtfile_tmp}]delete failure"
    fi

fi

done

exit 0
