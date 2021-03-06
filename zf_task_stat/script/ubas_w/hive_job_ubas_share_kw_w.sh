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
if [ $# -ne 1  ]
then
        echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:[DATE]"
        exit 1
fi
##-----------------------------------------------
SDATE=`date -d"7 days ago" +%Y%m%d`

if [ $# -eq 1 ]
then
	SDATE=$1
fi
EDATE=`date -d +7day"$SDATE" +%Y%m%d`
echo "get SDATE: ${SDATE},EDATE:${EDATE}"
##-----------------------------------------------
key='hive.database'
CLASS_PATH=" -classpath "$(echo $WORKPATH/../lib/*.jar|sed 's/ /:/g')
MAIN_CLASS=com.aotain.statmange.config.ReadParamMain
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hive.database:"$value

impalanodekey='hadoop.url';
impalanode=`java ${CLASS_PATH} ${MAIN_CLASS} ${impalanodekey} | tail -1`
IMPALA_SERVER=`echo ${impalanode}|grep -E -o "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"`

hive_database=$value
echo "hive_database:${hive_database}"
#--------------------------------------------------
key='hadoop.url.hive'
value=`java ${CLASS_PATH} ${MAIN_CLASS} ${key} | tail -1`
echo "hadoop.url.hive:"$value

hadoop_url_hive=$value
echo "hadoop_url_hive:${hadoop_url_hive}"
#--------------------------------------------------
#1拖N天粒度表
from_job_ubas_share_kw_d=job_ubas_share_kw_d
#1拖N周粒度表
to_job_ubas_share_kw_w=job_ubas_share_kw_w

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "---- zf-statmange 2.10.1 --------------"
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.10.1 update(2018-06-15)"
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"

OUTPUTPATH=/user/hive/warehouse/${hive_database}.db/${to_job_ubas_share_kw_w}/${SDATE}

stat_sql=" INSERT OVERWRITE TABLE ${to_job_ubas_share_kw_w} partition(dt='${SDATE}')

      select ${SDATE} as stat_time ,
                     useraccount,
                     userip,
                     (case when size(natiparray)>0 then size(natiparray)
                           when size(natiparray)=0 and size(devnamearray)>0 then size(devnamearray)
                           when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)>0 then size(osnamearray)
                           when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)=0 and cookievalcnt>0 then cookievalcnt
                           when size(natiparray)=0 and size(devnamearray)=0 and size(osnamearray)=0 and cookievalcnt=0 and size(qqnoarray)>0 then size(qqnoarray)
                           else 0 end) as hostcnt,
                     qqnoarray,
                     size(qqnoarray),
                     natiparray,
                     size(natiparray),
                     cookiearray,
                     cookievalcnt,
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
                       max(cookievalcnt) as cookievalcnt
      			    from(
      					select
      					useraccount,
      					userip,
      					collect_set(qq) as qqnoarray,
      					collect_set(natip) as natiparray,
      					collect_set(orriginalcookie) as cookiearray,
      					collect_set(devname) as devnamearray,
      					collect_set(osname) as osnamearray,
      					size(collect_set(cookieval)) as cookievalcnt,
      					probe_type,
      					area_id
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
      						from (
      							SELECT
      							useraccount,
      							userip,
      							qqnoarray,
      							natiparray,
      							cookiearray,
      							cookiearray as orriginalcookiearray,
      							devnamearray,
      							osnamearray,
      							probe_type,
      							area_id
      							FROM ${from_job_ubas_share_kw_d} t where dt>='${SDATE}' and dt<'${EDATE}'
      							) A
      						lateral view explode(qqnoarray) QQT as qq
      						lateral view explode(natiparray) NATIPT as natip
      						lateral view explode(cookiearray) COOKIET as cookie
      						lateral view explode(orriginalcookiearray) ORRIGINALCOOKIE as orriginalcookie
      						lateral view explode(devnamearray) DEVNAMET as devname
      						lateral view explode(osnamearray) OSNAMET as osname
      					)T group by useraccount,userip,probe_type,area_id,cookiehost
                       )B group by useraccount,userip,probe_type,area_id
                     )C
			"

echo "stat_sql=${stat_sql}"

beeline_sql="use ${hive_database};
         alter table ${to_job_ubas_share_kw_w} add if not exists partition(dt='${SDATE}') location '${OUTPUTPATH}' ;
		  set mapred.job.name=to_job_ubas_share_kw_w_${SDATE};
		   set hive.execution.engine=spark;
         ${stat_sql}"
beeline -u "${hadoop_url_hive}" -e "${beeline_sql};"
ret=$?
if [ $ret -ne 0 ]
then 
        echo `date +"%Y-%m-%d %H:%M:%S"`   "Exec failed [$SDATE],exit $ret !"
        exit $ret
fi

echo "IMPALA_SERVER:${IMPALA_SERVER}"
if [ "${IMPALA_SERVER}" = "" ];then
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_share_kw_w} "
else
	impala-shell -q "refresh  ${hive_database}.${to_job_ubas_share_kw_w} " -i ${IMPALA_SERVER}
fi

exit 0
