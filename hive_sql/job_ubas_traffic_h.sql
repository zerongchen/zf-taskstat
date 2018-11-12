use zf;
CREATE EXTERNAL TABLE `job_ubas_traffic_h`(
  `stat_time` int,
  `srcgroup_id` int,
  `dstgroup_id` int,
  `apptype` int,
  `appid` int,
  `appname` string,
  `apptraffic_up` bigint,
  `apptraffic_dn` bigint,
  `probe_type` int,
  `area_id` string,
  `src_areasubid1` string,
  `src_areasubid2` string,
  `src_areasubid3` string,
  `src_areasubid4` string,
  `dst_areasubid1` string,
  `dst_areasubid2` string,
  `dst_areasubid3` string,
  `dst_areasubid4` string)
PARTITIONED BY (
  `dt` bigint,
  `hour` int)
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
  '/user/hive/warehouse/zf.db/job_ubas_traffic_h';