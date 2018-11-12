use zf;
create external table job_ubas_appflow_h
(
   stat_time INT,
   usergroupno bigint,
   apptype int,
   appid int,
   appname String,
   appusernum bigint,
   apptraffic_up bigint,
   apptraffic_dn bigint,
   apppacketsnum bigint,
   appsessionsnum bigint,
   appnewsessionnum  bigint,
   probe_type INT,
   area_id STRING
)
PARTITIONED BY (dt STRING, hour INT)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/user/hive/warehouse/zf.db/job_ubas_appflow_h';
