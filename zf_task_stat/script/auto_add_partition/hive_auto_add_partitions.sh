#!/bin/bash
#Created by turk 2018-03-15
#Project  ZF STAT
#Version 4.6.1 modified by chenym
#Description:

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`
WORKPATH=`cd $bin/../..;pwd`
echo "WORKPATH:${WORKPATH}"
##----------------------------------
DATE=`date -d "+1 days" +%Y%m%d`

if [ $# -eq 1 ]
then
	DATE=$1                 #specified date
fi
echo "get Date: ${DATE}"
##----------------------------------

key='hive.database'
CLASS_PATH=" -classpath "$(echo $WORKPATH/lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hive.database:"$value

hive_database=$value
echo "hive_database:${hive_database}"

#hive table
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 4.6.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 4.6.1 update(2018-03-16)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

create_partition(){
table_name=$1

echo "table_name=${table_name}"

table_path=""
create_partition_sql=""
for((i=0;i<=23;i++))
do
  if [ ${i} -lt 10 ];
  then
        hours=0${i}
  else
        hours=${i}
  fi
     table_path=/user/hive/warehouse/${hive_database}.db/${table_name}/${DATE}/${hours}

     echo "hadoop fs  -mkdir -p ${table_path}"
     hadoop fs  -mkdir -p ${table_path}
     create_partition_sql=${create_partition_sql}" alter table ${table_name} add if not exists partition(dt='${DATE}',hour=${i}) location '${table_path}';"
done
echo "${create_partition_sql}"
hive -e "use ${hive_database};${create_partition_sql}"
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

}

timestamp=`date +%s`
echo $timestamp

hdfs_path=/tmp/create_partition_$timestamp
echo $hdfs_path
echo ''>$hdfs_path

hive -e "use ${hive_database};show tables">$hdfs_path
ret=$?
if [ $ret -ne 0 ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`   "show tables failed [$DATE],exit $ret !"
        exit $ret
fi

cat $hdfs_path | while read line
do

if [[ ${line} == *job_ubas* && ${line} != *_d && ${line} != *_h && ${line} != *_w && ${line} != *_m ]]
	then
		create_partition ${line}
	else
		echo "no create partition:${line}"
fi
if [[ ${line} == job_radius_log  ]]
	then
		create_partition ${line}
	else
		echo "no create partition:${line}"
fi
done

#hive -e "use ${hive_database};alter table job_radius_log add if not exists partition(dt='${DATE}') location '/user/hive/warehouse/${hive_database}.db/job_radius_log/${DATE}';"
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" 
exit 0

