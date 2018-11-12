#!/bin/bash

#Created by turk 2018-06-15
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

impalanodekey='hadoop.url';
impalanode=`java ${CLASS_PATH} ${MAIN_CLASS} ${impalanodekey} | tail -1`
IMPALA_SERVER=`echo ${impalanode}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`


echo "hive_database:${hive_database}"
#--------------------------------------------------
key='hadoop.url.hive'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url.hive:"$value

hadoop_url_hive=$value
echo "hadoop_url_hive:${hadoop_url_hive}"
#--------------------------------------------------
#1拖N详情表
from_job_ubas_share_kw=job_ubas_share_kw
#1拖N天粒度表
to_job_ubas_share_kw_d=job_ubas_share_kw_d

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.10.1 update(2018-06-15)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_share_kw_d}/${DATE}

stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_share_kw_d} partition(dt='${DATE}')
        select ${DATE} as stat_time ,
               useraccount,
               userip,
               (case when size(natiparray)>0 then size(natiparray)
                     when size(natiparray)=0 and size(devnamearray)>0 then size(devnamearray)
                     when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)>0 then size(osnamearray)
                     when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)=0 and cookiescnt>0 then cookiescnt
                     when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)=0 and cookiescnt=0 and size(qqnoarray)>0 then size(qqnoarray)
                     else 0 end) as hostcnt,
               qqnoarray,
               size(qqnoarray),
               natiparray,
               size(natiparray),
               cookiearray,
               cookiescnt,
               devnamearray,
               size(devnamearray),
               osnamearray,
               size(osnamearray),
               probe_type,
               area_id
        from (
            select
                useraccount,
                userip,
                probe_type,
                area_id,
                max(qqnoarray) as qqnoarray,
                max(natiparray) as natiparray,
                max(cookiearray) as cookiearray,
                max(devnamearray) as devnamearray,
                max(osnamearray) as osnamearray,
                max(cookievalcnt) as cookiescnt
            from (
               select
               useraccount,
               userip,
               probe_type,
               area_id,
               collect_set(qq) as qqnoarray,
               collect_set(natip) natiparray,
               collect_set(orriginalcookie) cookiearray,
               collect_set(devname) devnamearray,
               collect_set(osname) osnamearray,
               size(collect_set(cookieval)) as cookievalcnt
               from (
                   select
                   useraccount,
                   userip,
                   qq,
                   natip,
                   cookie,
                   orriginalcookie,
                   split(cookie,':')[0] as cookieval,
                   split(cookie,':')[1] as cookiehost,
                   devname,
                   osname,
                   probe_type,
                   area_id
                   from
                       (select
                       useraccount,
                       userip,
                       probe_type,
                       area_id,
                       split(concat_ws(',', collect_set(qqids)),',') as qqids,
                       split(concat_ws(',', collect_set(natips)),',') as natips,
                       split(concat_ws(',', collect_set(cookies)),',') as cookies,
                       split(concat_ws(',', collect_set(cookies)),',') as orriginalcookies,
                       split(concat_ws(',', collect_set(devnames)),',') as devnames,
                       split(concat_ws(',', collect_set(osnames)),',') as osnames
                       from ${from_job_ubas_share_kw} t where dt='${DATE}' GROUP BY useraccount,userip,probe_type,area_id
                       ) A
                   lateral view explode(qqids) QQT as qq
                   lateral view explode(natips) NATIPT as natip
                   lateral view explode(cookies) COOKIET as cookie
                   lateral view explode(orriginalcookies) ORRIGINALCOOKIE as orriginalcookie
                   lateral view explode(devnames) DEVNAMET as devname
                   lateral view explode(osnames) OSNAMET as osname)T
                   where qq!='' and natip!='' and cookie!='' and devname!='' and osname!='' and orriginalcookie!=''
                   group by useraccount,userip,probe_type,area_id,cookiehost
               ) B group by useraccount,userip,probe_type,area_id
               ) C
	   "

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_share_kw_d} add if not exists partition(dt='${DATE}') location '${OUTPUTPATH}' ;
          set hive.execution.engine=spark;
		  set mapred.job.name=to_job_ubas_share_kw_d_${DATE};
         ${stat_sql}"
beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$DATE],exit $ret !"
        exit $ret
fi

echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_share_kw_d} "
else
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_share_kw_d} " -i ${IMPALA_SERVER}
fi

exit 0
